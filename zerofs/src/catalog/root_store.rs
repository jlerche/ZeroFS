use super::{DurableRoot, MAX_ROOT_IDENTIFIER_BYTES, catalog_timestamp, validate_root};
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use slatedb::admin::{Admin, AdminBuilder, CloneSourceSpec};
use slatedb::config::CheckpointOptions;
use slatedb::object_store::path::Path;
use slatedb::{DbReader, DbReaderMode, PathResolver, VersionedManifest};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use uuid::Uuid;

const ROOT_CHECKPOINT_PREFIX: &str = "__zerofs_branch_root_";
const SHARED_SOURCE_CHECKPOINT_PREFIX: &str = "__zerofs_shared_source_";
const HEAD_CHECKPOINT_PREFIX: &str = "__zerofs_branch_head_";
const ROOT_OWNER_OBJECT: &str = "__zerofs_branch_root_owner.json";
const ROOT_RESULT_OBJECT: &str = "__zerofs_branch_root_result.json";
const HEAD_RESULT_PREFIX: &str = "__zerofs_branch_head_result_";
const ROOT_DESCRIPTOR_SCHEMA_VERSION: u32 = 1;
const ROOT_VERIFY_HEAD_CONCURRENCY: usize = 32;
const CONCURRENT_CLONE_RECONCILE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const CONCURRENT_CLONE_RECONCILE_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(20);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RootOwner {
    schema_version: u32,
    operation_id: Uuid,
    destination_id: Uuid,
    destination_path: String,
    source_path: String,
    source_checkpoint_id: Uuid,
    source_manifest_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnerClaim {
    Created,
    Existing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RootResult {
    owner: RootOwner,
    root: DurableRoot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WriterHeadResult {
    schema_version: u32,
    branch_id: Uuid,
    writer_lease_id: Uuid,
    destination_path: String,
    previous_root: DurableRoot,
    previous_writer_epoch: u64,
    root: DurableRoot,
    writer_epoch: u64,
}

#[derive(Debug, Clone)]
struct AuthenticatedSharedSource {
    checkpoint: ImmutableCheckpoint,
    physical_checkpoint: slatedb::Checkpoint,
    manifest: VersionedManifest,
}

type SharedSourceIdentity = (String, Uuid, u64);
type SharedSourceCache = BTreeMap<SharedSourceIdentity, AuthenticatedSharedSource>;
type SharedPinIdentity = (String, Uuid);
type SharedPinProofResult =
    Result<Arc<BTreeSet<slatedb::manifest::SsTableId>>, SharedPinProofFailure>;

#[derive(Debug)]
struct SharedPinProofFlight {
    result: watch::Receiver<Option<SharedPinProofResult>>,
}

#[derive(Debug, Clone)]
enum SharedPinProofFailure {
    MissingPin {
        source_path: String,
        checkpoint_id: Uuid,
    },
    MissingManifest(String),
    Backend(String),
}

impl SharedPinProofFailure {
    fn into_root_store_error(self) -> RootStoreError {
        match self {
            Self::MissingPin {
                source_path,
                checkpoint_id,
            } => RootStoreError::MissingExternalPin {
                source_path,
                checkpoint_id: Some(checkpoint_id),
            },
            Self::MissingManifest(manifest_id) => RootStoreError::MissingManifest(manifest_id),
            Self::Backend(error) => {
                RootStoreError::Clone(format!("shared source pin verification failed: {error}"))
            }
        }
    }
}

#[derive(Debug, Clone)]
struct PreparedCloneSource {
    checkpoint: ImmutableCheckpoint,
    physical_checkpoint: Option<slatedb::Checkpoint>,
    preloaded_manifest: Option<VersionedManifest>,
}

/// Exact immutable source resolved once before branch creation begins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImmutableCheckpoint {
    pub database_path: Path,
    pub checkpoint_id: Uuid,
    pub manifest_id: u64,
}

impl ImmutableCheckpoint {
    pub fn durable_root(&self) -> DurableRoot {
        DurableRoot {
            identity: self.database_path.to_string(),
            manifest_id: encode_root_checkpoint(self.checkpoint_id, self.manifest_id),
        }
    }

    /// Decode an exact SlateDB checkpoint identity from an authoritative root.
    pub fn from_durable_root(root: &DurableRoot) -> Result<Self, RootStoreError> {
        validate_root(root).map_err(|error| RootStoreError::Invalid(error.to_string()))?;
        let (checkpoint_id, manifest_id) = decode_root_checkpoint(&root.manifest_id)?;
        let database_path = Path::from(root.identity.clone());
        validate_database_path("source", &database_path)?;
        Ok(Self {
            database_path,
            checkpoint_id,
            manifest_id,
        })
    }
}

/// Creates and authenticates SlateDB roots used by ready branches.
///
/// A SlateDB clone is shallow. Its manifest names external SSTs and installs an
/// unnamed final checkpoint in each source database. Those final checkpoints,
/// not catalog ancestry or the user's named checkpoint, are storage pins owned
/// by the returned root. Physical source namespaces must remain available until
/// every such external pin has been detached by SlateDB.
#[derive(Clone)]
pub struct SlateDbRootStore {
    object_store: Arc<dyn ObjectStore>,
    wal_object_store: Option<Arc<dyn ObjectStore>>,
    branch_database_root: Path,
    segment_pool_root: Path,
    shared_sources: Arc<Mutex<SharedSourceCache>>,
    shared_pin_proofs: Arc<Mutex<BTreeMap<SharedPinIdentity, Arc<SharedPinProofFlight>>>>,
}

impl std::fmt::Debug for SlateDbRootStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SlateDbRootStore")
            .finish_non_exhaustive()
    }
}

impl SlateDbRootStore {
    pub fn new(object_store: Arc<dyn ObjectStore>, branch_database_root: Path) -> Self {
        Self {
            object_store,
            wal_object_store: None,
            branch_database_root,
            segment_pool_root: Path::default(),
            shared_sources: Arc::new(Mutex::new(BTreeMap::new())),
            shared_pin_proofs: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_wal_object_store(mut self, wal_object_store: Arc<dyn ObjectStore>) -> Self {
        self.wal_object_store = Some(wal_object_store);
        self
    }

    pub fn with_segment_pool_root(mut self, segment_pool_root: Path) -> Self {
        self.segment_pool_root = segment_pool_root;
        self
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }

    pub(crate) fn segment_pool_root(&self) -> &Path {
        &self.segment_pool_root
    }

    /// Open the immutable checkpoint encoded by an already-authenticated root.
    pub(crate) async fn checkpoint_reader(
        &self,
        root: &DurableRoot,
    ) -> Result<DbReader, RootStoreError> {
        let checkpoint = ImmutableCheckpoint::from_durable_root(root)?;
        let mut builder =
            DbReader::builder(checkpoint.database_path, Arc::clone(&self.object_store))
                .with_reader_mode(DbReaderMode::Checkpoint(checkpoint.checkpoint_id))
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor));
        if let Some(wal_object_store) = &self.wal_object_store {
            builder = builder.with_wal_object_store(Arc::clone(wal_object_store));
        }
        Ok(builder.build().await?)
    }

    /// Authenticate an ordinary named checkpoint before exposing a read lease.
    pub async fn verify_checkpoint(
        &self,
        checkpoint: &ImmutableCheckpoint,
    ) -> Result<(), RootStoreError> {
        validate_database_path("checkpoint", &checkpoint.database_path)?;
        self.authenticate_source(checkpoint).await.map(|_| ())
    }

    /// Authenticate every physical field required to publish a customer-named,
    /// permanent checkpoint as an authoritative catalog root.
    pub async fn verify_public_checkpoint(
        &self,
        checkpoint: &ImmutableCheckpoint,
        expected_name: &str,
        expected_created_at: DateTime<Utc>,
    ) -> Result<(), RootStoreError> {
        validate_database_path("checkpoint", &checkpoint.database_path)?;
        let (physical, _) = self.authenticate_source(checkpoint).await?;
        if physical.name.as_deref() != Some(expected_name) {
            return Err(RootStoreError::SourceCheckpointNameMismatch {
                checkpoint_id: checkpoint.checkpoint_id,
                expected: expected_name.to_string(),
                actual: physical.name,
            });
        }
        let named = self
            .admin(checkpoint.database_path.clone())
            .list_checkpoints(Some(expected_name))
            .await?;
        let resolved = match named.as_slice() {
            [resolved] => resolved,
            [] => {
                return Err(RootStoreError::MissingSourceCheckpointName(
                    expected_name.to_string(),
                ));
            }
            _ => {
                return Err(RootStoreError::DuplicateSourceCheckpointName(
                    expected_name.to_string(),
                ));
            }
        };
        if resolved.id != checkpoint.checkpoint_id {
            return Err(RootStoreError::SourceCheckpointNameIdentityMismatch {
                name: expected_name.to_string(),
                expected: checkpoint.checkpoint_id,
                actual: resolved.id,
            });
        }
        if resolved.manifest_id != checkpoint.manifest_id {
            return Err(RootStoreError::SourceManifestMismatch {
                checkpoint_id: checkpoint.checkpoint_id,
                expected: checkpoint.manifest_id,
                actual: resolved.manifest_id,
            });
        }
        if physical.expire_time.is_some() {
            return Err(RootStoreError::ExpiringSourceCheckpoint(
                checkpoint.checkpoint_id,
            ));
        }
        let expected = catalog_timestamp(expected_created_at);
        let actual = catalog_timestamp(physical.create_time);
        if actual != expected {
            return Err(RootStoreError::SourceCheckpointCreateTimeMismatch {
                checkpoint_id: checkpoint.checkpoint_id,
                expected,
                actual,
            });
        }
        Ok(())
    }

    /// Idempotently create a destination clone from one exact checkpoint.
    ///
    /// The caller must keep the source checkpoint held until the resulting root
    /// is durably recorded as an incomplete GC root or published as `Ready`.
    pub async fn create_from_checkpoint(
        &self,
        operation_id: Uuid,
        destination_id: Uuid,
        source: &ImmutableCheckpoint,
    ) -> Result<DurableRoot, RootStoreError> {
        self.create_from_checkpoint_inner(operation_id, destination_id, source, false)
            .await
    }

    /// Create a catalog-owned clone whose direct source checkpoint remains
    /// held for the published branch lifetime.
    pub(crate) async fn create_from_checkpoint_shared(
        &self,
        operation_id: Uuid,
        destination_id: Uuid,
        source: &ImmutableCheckpoint,
    ) -> Result<DurableRoot, RootStoreError> {
        self.create_from_checkpoint_inner(operation_id, destination_id, source, true)
            .await
    }

    async fn create_from_checkpoint_inner(
        &self,
        operation_id: Uuid,
        destination_id: Uuid,
        source: &ImmutableCheckpoint,
        shared_source_pin: bool,
    ) -> Result<DurableRoot, RootStoreError> {
        if operation_id.is_nil() {
            return Err(RootStoreError::Invalid(
                "branch operation UUID cannot be nil".to_string(),
            ));
        }
        if source.checkpoint_id.is_nil() {
            return Err(RootStoreError::Invalid(
                "source checkpoint UUID cannot be nil".to_string(),
            ));
        }
        if destination_id.is_nil() {
            return Err(RootStoreError::Invalid(
                "destination branch UUID cannot be nil".to_string(),
            ));
        }
        validate_database_path("source", &source.database_path)?;
        validate_database_path("branch database root", &self.branch_database_root)?;
        let destination_path = self
            .branch_database_root
            .clone()
            .join(destination_id.to_string());
        validate_database_path("destination", &destination_path)?;
        if database_paths_overlap(&source.database_path, &destination_path) {
            return Err(RootStoreError::Invalid(
                "source and destination database namespaces must not overlap".to_string(),
            ));
        }

        let destination_admin = self.admin(destination_path.clone());
        let owner = RootOwner {
            schema_version: ROOT_DESCRIPTOR_SCHEMA_VERSION,
            operation_id,
            destination_id,
            destination_path: destination_path.to_string(),
            source_path: source.database_path.to_string(),
            source_checkpoint_id: source.checkpoint_id,
            source_manifest_id: source.manifest_id,
        };
        let (owner_claim, prepared_source, recovered_destination) = if shared_source_pin {
            if let Some(result) = self.read_result(&destination_path).await? {
                if result.owner != owner {
                    return Err(RootStoreError::OwnershipConflict(
                        destination_path.to_string(),
                    ));
                }
                self.verify(&result.root).await?;
                self.cleanup_operation_checkpoints(&destination_path, operation_id, &result.root)
                    .await;
                return Ok(result.root);
            }
            // Keep the operation's requested immutable source authoritative
            // even while reconciling an already-initialized destination. An
            // external DB entry with the same path but a different checkpoint
            // must never become the source merely because it won a prior write.
            let prepared = Some(self.authenticated_shared_source(source).await?);
            (OwnerClaim::Created, prepared, None)
        } else {
            let inspected_owner = self.inspect_owner(&owner, &destination_admin).await?;
            let prepared = if inspected_owner.is_none() {
                let (checkpoint, manifest) = self.authenticate_source(source).await?;
                Some(AuthenticatedSharedSource {
                    checkpoint: source.clone(),
                    physical_checkpoint: checkpoint,
                    manifest,
                })
            } else {
                None
            };
            let claim = match inspected_owner {
                Some(claim) => claim,
                None => self.create_owner(&owner).await?,
            };
            let destination = if claim == OwnerClaim::Existing {
                destination_admin
                    .read_manifest(None)
                    .await?
                    .filter(|manifest| manifest.initialized())
            } else {
                None
            };
            (claim, prepared, destination)
        };
        if owner_claim == OwnerClaim::Existing
            && let Some(result) = self.read_result(&destination_path).await?
        {
            if result.owner != owner {
                return Err(RootStoreError::OwnershipConflict(
                    destination_path.to_string(),
                ));
            }
            self.verify(&result.root).await?;
            self.cleanup_operation_checkpoints(&destination_path, operation_id, &result.root)
                .await;
            return Ok(result.root);
        }
        let clone_source = if let Some(prepared) = prepared_source {
            PreparedCloneSource {
                checkpoint: prepared.checkpoint,
                physical_checkpoint: shared_source_pin.then_some(prepared.physical_checkpoint),
                preloaded_manifest: shared_source_pin.then_some(prepared.manifest),
            }
        } else if shared_source_pin {
            let prepared = self.authenticated_shared_source(source).await?;
            PreparedCloneSource {
                checkpoint: prepared.checkpoint,
                physical_checkpoint: Some(prepared.physical_checkpoint),
                preloaded_manifest: Some(prepared.manifest),
            }
        } else if recovered_destination.is_some() {
            // An initialized private clone is self-authenticating against the
            // exact requested source identity below. Do not require the
            // customer checkpoint to remain present during crash recovery,
            // and never substitute a checkpoint discovered in the destination.
            PreparedCloneSource {
                checkpoint: source.clone(),
                physical_checkpoint: None,
                preloaded_manifest: None,
            }
        } else {
            self.verify_source(source).await?;
            PreparedCloneSource {
                checkpoint: source.clone(),
                physical_checkpoint: None,
                preloaded_manifest: None,
            }
        };

        let mut source_spec = CloneSourceSpec::with_checkpoint(
            clone_source.checkpoint.database_path.clone(),
            clone_source.checkpoint.checkpoint_id,
        );
        if let (Some(checkpoint), Some(manifest)) = (
            clone_source.physical_checkpoint,
            clone_source.preloaded_manifest,
        ) {
            source_spec = source_spec.with_preloaded_manifest(checkpoint, manifest);
        }
        let mut builder = destination_admin.create_clone_builder_from_source(source_spec);
        if shared_source_pin {
            builder = builder.with_shared_source_pins();
        }
        if let Some(wal_object_store) = &self.wal_object_store {
            builder = builder.with_wal_object_store(Arc::clone(wal_object_store));
        }
        let checkpoint_name = format!("{ROOT_CHECKPOINT_PREFIX}{operation_id}");
        let root_checkpoint_id = branch_root_checkpoint_identity(operation_id, destination_id);
        let clone_attempt = builder
            .build_with_stable_checkpoint(root_checkpoint_id, &checkpoint_name)
            .await;
        let clone_error = clone_attempt.as_ref().err().map(ToString::to_string);

        let reconcile_deadline = tokio::time::Instant::now() + CONCURRENT_CLONE_RECONCILE_TIMEOUT;
        let (manifest, created_checkpoint) = match clone_attempt {
            Ok(result) => (result.manifest, Some(result.checkpoint)),
            Err(_) => {
                let manifest = loop {
                    if let Some(result) = self.read_result(&destination_path).await? {
                        if result.owner != owner {
                            return Err(RootStoreError::OwnershipConflict(
                                destination_path.to_string(),
                            ));
                        }
                        self.verify(&result.root).await?;
                        self.cleanup_operation_checkpoints(
                            &destination_path,
                            operation_id,
                            &result.root,
                        )
                        .await;
                        return Ok(result.root);
                    }
                    match destination_admin.read_manifest(None).await? {
                        Some(manifest) if manifest.initialized() => break manifest,
                        state if tokio::time::Instant::now() < reconcile_deadline => {
                            tokio::time::sleep(CONCURRENT_CLONE_RECONCILE_INTERVAL).await;
                            drop(state);
                        }
                        Some(_) => {
                            return Err(RootStoreError::Uninitialized(
                                destination_path.to_string(),
                            ));
                        }
                        None => {
                            return Err(RootStoreError::Clone(
                                clone_error.clone().expect("failed clone has an error"),
                            ));
                        }
                    }
                };
                let expected_source_attached = manifest.external_dbs().iter().any(|external| {
                    external.path == clone_source.checkpoint.database_path.to_string()
                        && external.source_checkpoint_id == clone_source.checkpoint.checkpoint_id
                        && if shared_source_pin {
                            external.final_checkpoint_id
                                == Some(clone_source.checkpoint.checkpoint_id)
                        } else {
                            external.final_checkpoint_id.is_some()
                        }
                });
                if !expected_source_attached {
                    return Err(RootStoreError::Clone(
                        clone_error.clone().expect("failed clone has an error"),
                    ));
                }
                let recovered = destination_admin
                    .create_current_checkpoint_with_id(root_checkpoint_id, &checkpoint_name)
                    .await?;
                let pinned_manifest = destination_admin
                    .read_manifest(Some(recovered.manifest_id))
                    .await?
                    .ok_or_else(|| {
                        RootStoreError::MissingManifest(format!(
                            "{}@{}",
                            destination_path, recovered.manifest_id
                        ))
                    })?;
                let mut exact = destination_admin
                    .list_checkpoints(Some(&checkpoint_name))
                    .await?
                    .into_iter()
                    .filter(|checkpoint| checkpoint.id == root_checkpoint_id)
                    .collect::<Vec<_>>();
                if exact.len() != 1 {
                    return Err(RootStoreError::NonCanonicalRoot(encode_root_checkpoint(
                        recovered.id,
                        recovered.manifest_id,
                    )));
                }
                (pinned_manifest, Some(exact.remove(0)))
            }
        };
        let attached = manifest.external_dbs().iter().any(|external| {
            external.path == clone_source.checkpoint.database_path.to_string()
                && external.source_checkpoint_id == clone_source.checkpoint.checkpoint_id
                && if shared_source_pin {
                    external.final_checkpoint_id == Some(clone_source.checkpoint.checkpoint_id)
                } else {
                    external.final_checkpoint_id.is_some()
                }
        });
        if !attached {
            if let Some(error) = clone_error {
                return Err(RootStoreError::Clone(error));
            }
            return Err(RootStoreError::WrongSource {
                destination: destination_path.to_string(),
                source_path: source.database_path.to_string(),
                checkpoint_id: source.checkpoint_id,
            });
        }
        ensure_no_wal_dependency(&manifest)?;

        let created_checkpoint = created_checkpoint
            .expect("successful or reconciled clone always returns its exact root checkpoint");
        let root_manifest_id = created_checkpoint.manifest_id;
        let root = DurableRoot {
            identity: destination_path.to_string(),
            manifest_id: encode_root_checkpoint(root_checkpoint_id, root_manifest_id),
        };
        if created_checkpoint.id != root_checkpoint_id
            || created_checkpoint.manifest_id != root_manifest_id
            || created_checkpoint.name.as_deref() != Some(checkpoint_name.as_str())
            || created_checkpoint.expire_time.is_some()
            || manifest.id() != root_manifest_id
            || !manifest.initialized()
        {
            return Err(RootStoreError::NonCanonicalRoot(root.manifest_id));
        }
        self.verify_manifest_storage(destination_path.clone(), &manifest)
            .await?;
        let canonical = self.publish_result(RootResult { owner, root }).await?;
        if owner_claim == OwnerClaim::Existing || clone_error.is_some() {
            self.cleanup_operation_checkpoints(&destination_path, operation_id, &canonical)
                .await;
        }
        Ok(canonical)
    }

    /// Publish the latest fully flushed manifest as an immutable branch head.
    ///
    /// The writable database must already be closed. The exact writer lease is
    /// the idempotency identity: retries elect one permanent checkpoint and
    /// never retarget a different branch or previous authoritative root.
    pub async fn publish_writer_head(
        &self,
        branch_id: Uuid,
        writer_lease_id: Uuid,
        previous_root: &DurableRoot,
    ) -> Result<DurableRoot, RootStoreError> {
        if branch_id.is_nil() || writer_lease_id.is_nil() || branch_id == writer_lease_id {
            return Err(RootStoreError::Invalid(
                "branch and writer lease UUIDs must be distinct and non-nil".to_string(),
            ));
        }
        self.verify(previous_root).await?;
        let destination = Path::from(previous_root.identity.clone());
        let initial = self
            .read_result(&destination)
            .await?
            .ok_or_else(|| RootStoreError::MissingResult(destination.to_string()))?;
        let owner = self
            .read_optional::<RootOwner>(&owner_object_path(&destination))
            .await?
            .unwrap_or(initial.owner);
        if owner.destination_id != branch_id || owner.destination_path != destination.to_string() {
            return Err(RootStoreError::OwnershipConflict(destination.to_string()));
        }
        let result_path = head_result_object_path(&destination, writer_lease_id);
        if let Some(existing) = self.read_optional::<WriterHeadResult>(&result_path).await? {
            self.validate_writer_head_result(
                &existing,
                branch_id,
                writer_lease_id,
                previous_root,
                &destination,
            )?;
            self.verify_writer_epoch_advance(
                &existing.previous_root,
                existing.previous_writer_epoch,
                &existing.root,
                existing.writer_epoch,
            )
            .await?;
            self.verify(&existing.root).await?;
            return Ok(existing.root);
        }

        let checkpoint_name = format!("{HEAD_CHECKPOINT_PREFIX}{writer_lease_id}");
        let admin = self.admin(destination.clone());
        let previous_writer_epoch = self.root_writer_epoch(previous_root).await?;
        let mut checkpoints = admin.list_checkpoints(Some(&checkpoint_name)).await?;
        checkpoints.sort_by_key(|checkpoint| (checkpoint.manifest_id, checkpoint.id));
        let mut advanced_checkpoint = None;
        for checkpoint in checkpoints {
            let writer_epoch = admin
                .read_manifest(Some(checkpoint.manifest_id))
                .await?
                .ok_or_else(|| RootStoreError::MissingManifest(checkpoint.manifest_id.to_string()))?
                .writer_epoch();
            if writer_epoch > previous_writer_epoch {
                advanced_checkpoint = Some(checkpoint);
                break;
            }
        }
        let (checkpoint_id, checkpoint_manifest_id) = if let Some(checkpoint) = advanced_checkpoint
        {
            (checkpoint.id, checkpoint.manifest_id)
        } else {
            match admin
                .create_detached_checkpoint(&CheckpointOptions {
                    name: Some(checkpoint_name.clone()),
                    ..CheckpointOptions::default()
                })
                .await
            {
                Ok(checkpoint) => (checkpoint.id, checkpoint.manifest_id),
                Err(error) => {
                    if let Some(existing) =
                        self.read_optional::<WriterHeadResult>(&result_path).await?
                    {
                        self.validate_writer_head_result(
                            &existing,
                            branch_id,
                            writer_lease_id,
                            previous_root,
                            &destination,
                        )?;
                        self.verify_writer_epoch_advance(
                            &existing.previous_root,
                            existing.previous_writer_epoch,
                            &existing.root,
                            existing.writer_epoch,
                        )
                        .await?;
                        self.verify(&existing.root).await?;
                        return Ok(existing.root);
                    }
                    let mut recovered = admin.list_checkpoints(Some(&checkpoint_name)).await?;
                    recovered.sort_by_key(|checkpoint| (checkpoint.manifest_id, checkpoint.id));
                    let mut advanced = None;
                    for checkpoint in recovered {
                        let writer_epoch = admin
                            .read_manifest(Some(checkpoint.manifest_id))
                            .await?
                            .ok_or_else(|| {
                                RootStoreError::MissingManifest(checkpoint.manifest_id.to_string())
                            })?
                            .writer_epoch();
                        if writer_epoch > previous_writer_epoch {
                            advanced = Some(checkpoint);
                            break;
                        }
                    }
                    let checkpoint = advanced.ok_or(error)?;
                    (checkpoint.id, checkpoint.manifest_id)
                }
            }
        };
        let candidate_root = DurableRoot {
            identity: destination.to_string(),
            manifest_id: encode_root_checkpoint(checkpoint_id, checkpoint_manifest_id),
        };
        let (_, writer_epoch) = self
            .writer_epoch_advance(previous_root, &candidate_root)
            .await?;
        let candidate = WriterHeadResult {
            schema_version: ROOT_DESCRIPTOR_SCHEMA_VERSION,
            branch_id,
            writer_lease_id,
            destination_path: destination.to_string(),
            previous_root: previous_root.clone(),
            previous_writer_epoch,
            root: candidate_root,
            writer_epoch,
        };
        self.verify_storage_root(&candidate.root, &checkpoint_name)
            .await?;
        let canonical = self
            .publish_writer_head_result(&result_path, candidate)
            .await?;
        self.cleanup_named_checkpoints(&destination, &checkpoint_name, &canonical)
            .await;
        Ok(canonical)
    }

    /// Authenticate the canonical operation-owned root, its reachable objects,
    /// and every external final-checkpoint pin it needs.
    pub async fn verify(&self, root: &DurableRoot) -> Result<(), RootStoreError> {
        validate_root(root).map_err(|error| RootStoreError::Invalid(error.to_string()))?;
        let destination = Path::from(root.identity.clone());
        let initial = self
            .read_result(&destination)
            .await?
            .ok_or_else(|| RootStoreError::MissingResult(destination.to_string()))?;
        let owner = self
            .read_optional::<RootOwner>(&owner_object_path(&destination))
            .await?
            .unwrap_or_else(|| initial.owner.clone());
        if owner != initial.owner
            || owner.schema_version != ROOT_DESCRIPTOR_SCHEMA_VERSION
            || owner.destination_path != destination.to_string()
            || destination
                != self
                    .branch_database_root
                    .clone()
                    .join(owner.destination_id.to_string())
            || owner.destination_id.is_nil()
            || owner.operation_id.is_nil()
        {
            return Err(RootStoreError::OwnershipConflict(destination.to_string()));
        }
        let expected_name = if initial.root == *root {
            format!("{ROOT_CHECKPOINT_PREFIX}{}", owner.operation_id)
        } else {
            let (checkpoint_id, _) = decode_root_checkpoint(&root.manifest_id)?;
            let checkpoint = self
                .admin(destination.clone())
                .list_checkpoints(None)
                .await?
                .into_iter()
                .find(|checkpoint| checkpoint.id == checkpoint_id)
                .ok_or_else(|| RootStoreError::MissingRootCheckpoint(checkpoint_id.to_string()))?;
            let name = checkpoint
                .name
                .ok_or_else(|| RootStoreError::NonCanonicalRoot(root.manifest_id.clone()))?;
            let lease_id = name
                .strip_prefix(HEAD_CHECKPOINT_PREFIX)
                .and_then(|value| Uuid::parse_str(value).ok())
                .ok_or_else(|| RootStoreError::NonCanonicalRoot(root.manifest_id.clone()))?;
            let head = self
                .read_optional::<WriterHeadResult>(&head_result_object_path(&destination, lease_id))
                .await?
                .ok_or_else(|| RootStoreError::NonCanonicalRoot(root.manifest_id.clone()))?;
            if head.schema_version != ROOT_DESCRIPTOR_SCHEMA_VERSION
                || head.branch_id != owner.destination_id
                || head.writer_lease_id != lease_id
                || head.destination_path != destination.to_string()
                || head.root != *root
                || head.previous_root.identity != root.identity
                || head.previous_root == head.root
            {
                return Err(RootStoreError::NonCanonicalRoot(root.manifest_id.clone()));
            }
            self.verify_canonical_root_identity(&head.previous_root, &owner, &initial)
                .await?;
            self.verify_writer_epoch_advance(
                &head.previous_root,
                head.previous_writer_epoch,
                &head.root,
                head.writer_epoch,
            )
            .await?;
            name
        };
        self.verify_storage_root(root, &expected_name).await
    }

    async fn verify_canonical_root_identity(
        &self,
        root: &DurableRoot,
        owner: &RootOwner,
        initial: &RootResult,
    ) -> Result<(), RootStoreError> {
        if root.identity != owner.destination_path || initial.owner != *owner {
            return Err(RootStoreError::OwnershipConflict(
                owner.destination_path.clone(),
            ));
        }
        let expected_name = if initial.root == *root {
            format!("{ROOT_CHECKPOINT_PREFIX}{}", owner.operation_id)
        } else {
            let (checkpoint_id, _) = decode_root_checkpoint(&root.manifest_id)?;
            let checkpoint = self
                .admin(Path::from(root.identity.clone()))
                .list_checkpoints(None)
                .await?
                .into_iter()
                .find(|checkpoint| checkpoint.id == checkpoint_id)
                .ok_or_else(|| RootStoreError::MissingRootCheckpoint(checkpoint_id.to_string()))?;
            let name = checkpoint
                .name
                .ok_or_else(|| RootStoreError::NonCanonicalRoot(root.manifest_id.clone()))?;
            let lease_id = name
                .strip_prefix(HEAD_CHECKPOINT_PREFIX)
                .and_then(|value| Uuid::parse_str(value).ok())
                .ok_or_else(|| RootStoreError::NonCanonicalRoot(root.manifest_id.clone()))?;
            let head = self
                .read_optional::<WriterHeadResult>(&head_result_object_path(
                    &Path::from(root.identity.clone()),
                    lease_id,
                ))
                .await?
                .ok_or_else(|| RootStoreError::NonCanonicalRoot(root.manifest_id.clone()))?;
            if head.schema_version != ROOT_DESCRIPTOR_SCHEMA_VERSION
                || head.branch_id != owner.destination_id
                || head.writer_lease_id != lease_id
                || head.destination_path != owner.destination_path
                || head.root != *root
                || head.previous_root.identity != root.identity
                || head.previous_root == head.root
            {
                return Err(RootStoreError::NonCanonicalRoot(root.manifest_id.clone()));
            }
            name
        };
        self.verify_storage_root(root, &expected_name).await
    }

    fn validate_writer_head_result(
        &self,
        result: &WriterHeadResult,
        branch_id: Uuid,
        writer_lease_id: Uuid,
        previous_root: &DurableRoot,
        destination: &Path,
    ) -> Result<(), RootStoreError> {
        if result.schema_version != ROOT_DESCRIPTOR_SCHEMA_VERSION
            || result.branch_id != branch_id
            || result.writer_lease_id != writer_lease_id
            || result.destination_path != destination.to_string()
            || result.previous_root != *previous_root
            || result.root.identity != destination.to_string()
            || result.root == result.previous_root
        {
            return Err(RootStoreError::OwnershipConflict(destination.to_string()));
        }
        Ok(())
    }

    async fn writer_epoch_advance(
        &self,
        previous_root: &DurableRoot,
        root: &DurableRoot,
    ) -> Result<(u64, u64), RootStoreError> {
        let previous_writer_epoch = self.root_writer_epoch(previous_root).await?;
        let writer_epoch = self.root_writer_epoch(root).await?;
        if writer_epoch <= previous_writer_epoch {
            return Err(RootStoreError::StaleWriterIncarnation {
                previous: previous_writer_epoch,
                current: writer_epoch,
            });
        }
        Ok((previous_writer_epoch, writer_epoch))
    }

    async fn verify_writer_epoch_advance(
        &self,
        previous_root: &DurableRoot,
        expected_previous_writer_epoch: u64,
        root: &DurableRoot,
        expected_writer_epoch: u64,
    ) -> Result<(), RootStoreError> {
        let (previous_writer_epoch, writer_epoch) =
            self.writer_epoch_advance(previous_root, root).await?;
        if previous_writer_epoch != expected_previous_writer_epoch
            || writer_epoch != expected_writer_epoch
        {
            return Err(RootStoreError::WriterIncarnationMismatch {
                expected_previous: expected_previous_writer_epoch,
                actual_previous: previous_writer_epoch,
                expected_current: expected_writer_epoch,
                actual_current: writer_epoch,
            });
        }
        Ok(())
    }

    async fn root_writer_epoch(&self, root: &DurableRoot) -> Result<u64, RootStoreError> {
        validate_root(root).map_err(|error| RootStoreError::Invalid(error.to_string()))?;
        let (checkpoint_id, manifest_id) = decode_root_checkpoint(&root.manifest_id)?;
        let admin = self.admin(Path::from(root.identity.clone()));
        let checkpoint = admin
            .list_checkpoints(None)
            .await?
            .into_iter()
            .find(|checkpoint| checkpoint.id == checkpoint_id)
            .ok_or_else(|| RootStoreError::MissingRootCheckpoint(checkpoint_id.to_string()))?;
        if checkpoint.manifest_id != manifest_id {
            return Err(RootStoreError::RootManifestMismatch {
                checkpoint_id,
                expected: manifest_id,
                actual: checkpoint.manifest_id,
            });
        }
        admin
            .read_manifest(Some(manifest_id))
            .await?
            .ok_or_else(|| RootStoreError::MissingManifest(root.manifest_id.clone()))
            .map(|manifest| manifest.writer_epoch())
    }

    async fn verify_storage_root(
        &self,
        root: &DurableRoot,
        expected_checkpoint_name: &str,
    ) -> Result<(), RootStoreError> {
        validate_root(root).map_err(|error| RootStoreError::Invalid(error.to_string()))?;
        let (checkpoint_id, manifest_id) = decode_root_checkpoint(&root.manifest_id)?;
        let path = Path::from(root.identity.clone());
        let root_checkpoint = self
            .admin(path.clone())
            .list_checkpoints(None)
            .await?
            .into_iter()
            .find(|checkpoint| checkpoint.id == checkpoint_id)
            .ok_or(RootStoreError::MissingRootCheckpoint(
                checkpoint_id.to_string(),
            ))?;
        if root_checkpoint.name.as_deref() != Some(expected_checkpoint_name)
            || root_checkpoint.expire_time.is_some()
        {
            return Err(RootStoreError::Invalid(
                "durable root checkpoint is not a permanent internal checkpoint".to_string(),
            ));
        }
        if root_checkpoint.manifest_id != manifest_id {
            return Err(RootStoreError::RootManifestMismatch {
                checkpoint_id,
                expected: manifest_id,
                actual: root_checkpoint.manifest_id,
            });
        }
        let manifest = self
            .admin(path.clone())
            .read_manifest(Some(manifest_id))
            .await?
            .ok_or_else(|| RootStoreError::MissingManifest(root.manifest_id.clone()))?;
        if !manifest.initialized() {
            return Err(RootStoreError::Uninitialized(path.to_string()));
        }
        ensure_no_wal_dependency(&manifest)?;

        self.verify_manifest_storage(path, &manifest).await
    }

    async fn verify_manifest_storage(
        &self,
        path: Path,
        manifest: &slatedb::VersionedManifest,
    ) -> Result<(), RootStoreError> {
        for external in manifest.external_dbs() {
            let final_checkpoint_id =
                external
                    .final_checkpoint_id
                    .ok_or_else(|| RootStoreError::MissingExternalPin {
                        source_path: external.path.clone(),
                        checkpoint_id: None,
                    })?;
            let source_path = Path::from(external.path.clone());
            let pinned_ids = if final_checkpoint_id == external.source_checkpoint_id {
                self.verify_shared_pin(source_path, final_checkpoint_id)
                    .await?
            } else {
                Arc::new(
                    self.verify_private_pin(source_path, final_checkpoint_id)
                        .await?,
                )
            };
            if !external.sst_ids.iter().all(|id| pinned_ids.contains(id)) {
                return Err(RootStoreError::ExternalPinCoverage {
                    source_path: external.path.clone(),
                    checkpoint_id: final_checkpoint_id,
                });
            }
        }
        let resolver = PathResolver::new(path, manifest);
        let mut root_sst_ids = BTreeSet::new();
        root_sst_ids.extend(manifest.l0().iter().map(|view| view.sst.id));
        root_sst_ids.extend(
            manifest
                .compacted()
                .iter()
                .flat_map(|run| run.sst_views.iter().map(|view| view.sst.id)),
        );
        for segment in manifest.segments() {
            root_sst_ids.extend(segment.l0().iter().map(|view| view.sst.id));
            root_sst_ids.extend(
                segment
                    .compacted()
                    .iter()
                    .flat_map(|run| run.sst_views.iter().map(|view| view.sst.id)),
            );
        }
        root_sst_ids.extend(
            manifest
                .external_dbs()
                .iter()
                .flat_map(|external| external.sst_ids.iter().copied()),
        );
        let object_store = Arc::clone(&self.object_store);
        let mut checks = stream::iter(
            root_sst_ids
                .into_iter()
                .map(|sst_id| resolver.sst_path(&sst_id)),
        )
        .map(|sst_path| {
            let object_store = Arc::clone(&object_store);
            async move { (sst_path.clone(), object_store.head(&sst_path).await) }
        })
        .buffer_unordered(ROOT_VERIFY_HEAD_CONCURRENCY);
        while let Some((sst_path, result)) = checks.next().await {
            if let Err(error) = result {
                return match error {
                    object_store::Error::NotFound { .. } => {
                        Err(RootStoreError::MissingSst(sst_path.to_string()))
                    }
                    other => Err(other.into()),
                };
            }
        }
        Ok(())
    }

    /// Authenticate one borrowed permanent source pin for the set of branch
    /// publications that overlap this proof. The completed result is removed
    /// immediately: a later wave must touch storage again, while siblings
    /// already in flight avoid repeating the same manifest LIST and GET.
    async fn verify_shared_pin(
        &self,
        source_path: Path,
        checkpoint_id: Uuid,
    ) -> Result<Arc<BTreeSet<slatedb::manifest::SsTableId>>, RootStoreError> {
        let identity = (source_path.to_string(), checkpoint_id);
        let (flight, leader_sender) = {
            let mut flights = self.shared_pin_proofs.lock().await;
            if let Some(flight) = flights.get(&identity) {
                (Arc::clone(flight), None)
            } else {
                let (sender, receiver) = watch::channel(None);
                let flight = Arc::new(SharedPinProofFlight { result: receiver });
                flights.insert(identity.clone(), Arc::clone(&flight));
                (flight, Some(sender))
            }
        };

        if let Some(sender) = leader_sender {
            let roots = self.clone();
            let flight_for_task = Arc::clone(&flight);
            let source_path_for_task = source_path.clone();
            tokio::spawn(async move {
                let result = roots
                    .load_shared_pin_proof(source_path_for_task, checkpoint_id)
                    .await;
                {
                    let mut flights = roots.shared_pin_proofs.lock().await;
                    if flights
                        .get(&identity)
                        .is_some_and(|current| Arc::ptr_eq(current, &flight_for_task))
                    {
                        flights.remove(&identity);
                    }
                }
                // Existing waiters retain this flight. Removing it before the
                // broadcast ensures callers arriving after physical proof
                // completion always elect a new storage check.
                sender.send_replace(Some(result));
            });
        }

        let mut receiver = flight.result.clone();
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result.map_err(SharedPinProofFailure::into_root_store_error);
            }
            receiver.changed().await.map_err(|_| {
                RootStoreError::Clone("shared source pin verifier stopped unexpectedly".to_string())
            })?;
        }
    }

    async fn load_shared_pin_proof(
        &self,
        source_path: Path,
        checkpoint_id: Uuid,
    ) -> SharedPinProofResult {
        let pin = self
            .admin(source_path.clone())
            .list_checkpoints(None)
            .await
            .map_err(|error| SharedPinProofFailure::Backend(error.to_string()))?
            .into_iter()
            .find(|checkpoint| checkpoint.id == checkpoint_id)
            .filter(|checkpoint| {
                checkpoint.expire_time.is_none()
                    && valid_shared_source_checkpoint(&source_path, checkpoint)
            })
            .ok_or_else(|| SharedPinProofFailure::MissingPin {
                source_path: source_path.to_string(),
                checkpoint_id,
            })?;
        let manifest = self
            .admin(source_path)
            .read_manifest(Some(pin.manifest_id))
            .await
            .map_err(|error| SharedPinProofFailure::Backend(error.to_string()))?
            .ok_or_else(|| SharedPinProofFailure::MissingManifest(pin.manifest_id.to_string()))?;
        Ok(Arc::new(manifest_owned_sst_ids(&manifest)))
    }

    async fn verify_private_pin(
        &self,
        source_path: Path,
        checkpoint_id: Uuid,
    ) -> Result<BTreeSet<slatedb::manifest::SsTableId>, RootStoreError> {
        let pin = self
            .admin(source_path.clone())
            .list_checkpoints(None)
            .await?
            .into_iter()
            .find(|checkpoint| checkpoint.id == checkpoint_id)
            .filter(|checkpoint| checkpoint.expire_time.is_none() && checkpoint.name.is_none())
            .ok_or_else(|| RootStoreError::MissingExternalPin {
                source_path: source_path.to_string(),
                checkpoint_id: Some(checkpoint_id),
            })?;
        let manifest = self
            .admin(source_path)
            .read_manifest(Some(pin.manifest_id))
            .await?
            .ok_or_else(|| RootStoreError::MissingManifest(pin.manifest_id.to_string()))?;
        Ok(manifest_owned_sst_ids(&manifest))
    }

    async fn ensure_shared_source_pin(
        &self,
        source: &ImmutableCheckpoint,
    ) -> Result<(ImmutableCheckpoint, slatedb::Checkpoint), RootStoreError> {
        let (checkpoint_id, checkpoint_name) = shared_source_checkpoint_identity(
            &source.database_path,
            source.checkpoint_id,
            source.manifest_id,
        );
        let source_admin = self.admin(source.database_path.clone());
        let checkpoint = self
            .admin(source.database_path.clone())
            .create_detached_checkpoint_with_id(
                checkpoint_id,
                &CheckpointOptions {
                    lifetime: None,
                    source: Some(source.checkpoint_id),
                    name: Some(checkpoint_name.clone()),
                },
            )
            .await?;
        if checkpoint.id != checkpoint_id || checkpoint.manifest_id != source.manifest_id {
            return Err(RootStoreError::SourceManifestMismatch {
                checkpoint_id,
                expected: source.manifest_id,
                actual: checkpoint.manifest_id,
            });
        }
        let physical = source_admin
            .list_checkpoints(None)
            .await?
            .into_iter()
            .find(|candidate| candidate.id == checkpoint_id)
            .ok_or(RootStoreError::MissingSourceCheckpoint(checkpoint_id))?;
        if physical.manifest_id != source.manifest_id
            || physical.expire_time.is_some()
            || physical.name.as_deref() != Some(checkpoint_name.as_str())
        {
            return Err(RootStoreError::Invalid(format!(
                "shared source checkpoint {checkpoint_id} does not match its permanent descriptor"
            )));
        }
        Ok((
            ImmutableCheckpoint {
                database_path: source.database_path.clone(),
                checkpoint_id,
                manifest_id: source.manifest_id,
            },
            physical,
        ))
    }

    async fn authenticated_shared_source(
        &self,
        source: &ImmutableCheckpoint,
    ) -> Result<AuthenticatedSharedSource, RootStoreError> {
        let identity = (
            source.database_path.to_string(),
            source.checkpoint_id,
            source.manifest_id,
        );
        let mut shared = self.shared_sources.lock().await;
        if let Some(prepared) = shared.get(&identity) {
            return Ok(prepared.clone());
        }
        let (checkpoint, manifest) = self.authenticate_source(source).await?;
        if checkpoint.expire_time.is_some() {
            return Err(RootStoreError::ExpiringSourceCheckpoint(
                source.checkpoint_id,
            ));
        }
        if !manifest.initialized() {
            return Err(RootStoreError::Uninitialized(
                source.database_path.to_string(),
            ));
        }
        self.verify_manifest_storage(source.database_path.clone(), &manifest)
            .await?;
        let (checkpoint, physical_checkpoint) = self.ensure_shared_source_pin(source).await?;
        let prepared = AuthenticatedSharedSource {
            checkpoint,
            physical_checkpoint,
            manifest,
        };
        shared.insert(identity, prepared.clone());
        Ok(prepared)
    }

    async fn inspect_owner(
        &self,
        owner: &RootOwner,
        destination_admin: &Admin,
    ) -> Result<Option<OwnerClaim>, RootStoreError> {
        let destination = Path::from(owner.destination_path.clone());
        let path = owner_object_path(&destination);
        if let Some(existing) = self.read_optional::<RootOwner>(&path).await? {
            return if existing == *owner {
                Ok(Some(OwnerClaim::Existing))
            } else {
                Err(RootStoreError::OwnershipConflict(
                    owner.destination_path.clone(),
                ))
            };
        }
        if destination_admin.read_manifest(None).await?.is_some() {
            if let Some(existing) = self.read_optional::<RootOwner>(&path).await? {
                return if existing == *owner {
                    Ok(Some(OwnerClaim::Existing))
                } else {
                    Err(RootStoreError::OwnershipConflict(
                        owner.destination_path.clone(),
                    ))
                };
            }
            return Err(RootStoreError::UnownedDestination(
                owner.destination_path.clone(),
            ));
        }
        Ok(None)
    }

    async fn create_owner(&self, owner: &RootOwner) -> Result<OwnerClaim, RootStoreError> {
        let destination = Path::from(owner.destination_path.clone());
        let path = owner_object_path(&destination);
        match self
            .object_store
            .put_opts(
                &path,
                serde_json::to_vec(owner)?.into(),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => Ok(OwnerClaim::Created),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self
                    .read_optional::<RootOwner>(&path)
                    .await?
                    .ok_or_else(|| RootStoreError::MissingOwner(owner.destination_path.clone()))?;
                if existing == *owner {
                    Ok(OwnerClaim::Existing)
                } else {
                    Err(RootStoreError::OwnershipConflict(
                        owner.destination_path.clone(),
                    ))
                }
            }
            Err(error) => {
                if self.read_optional::<RootOwner>(&path).await? == Some(owner.clone()) {
                    Ok(OwnerClaim::Existing)
                } else {
                    Err(error.into())
                }
            }
        }
    }

    #[cfg(test)]
    async fn claim_owner(
        &self,
        owner: &RootOwner,
        destination_admin: &Admin,
    ) -> Result<OwnerClaim, RootStoreError> {
        match self.inspect_owner(owner, destination_admin).await? {
            Some(claim) => Ok(claim),
            None => self.create_owner(owner).await,
        }
    }

    #[cfg(test)]
    async fn ensure_owner(
        &self,
        owner: &RootOwner,
        destination_admin: &Admin,
    ) -> Result<(), RootStoreError> {
        self.claim_owner(owner, destination_admin).await.map(|_| ())
    }

    async fn verify_source(&self, source: &ImmutableCheckpoint) -> Result<(), RootStoreError> {
        self.authenticate_source(source).await.map(|_| ())
    }

    async fn authenticate_source(
        &self,
        source: &ImmutableCheckpoint,
    ) -> Result<(slatedb::Checkpoint, slatedb::VersionedManifest), RootStoreError> {
        let source_admin = self.admin(source.database_path.clone());
        let source_checkpoint = source_admin
            .list_checkpoints(None)
            .await?
            .into_iter()
            .find(|checkpoint| checkpoint.id == source.checkpoint_id)
            .ok_or(RootStoreError::MissingSourceCheckpoint(
                source.checkpoint_id,
            ))?;
        if source_checkpoint.manifest_id != source.manifest_id {
            return Err(RootStoreError::SourceManifestMismatch {
                checkpoint_id: source.checkpoint_id,
                expected: source.manifest_id,
                actual: source_checkpoint.manifest_id,
            });
        }
        let source_manifest = source_admin
            .read_manifest(Some(source.manifest_id))
            .await?
            .ok_or_else(|| RootStoreError::MissingManifest(source.manifest_id.to_string()))?;
        ensure_no_wal_dependency(&source_manifest)?;
        Ok((source_checkpoint, source_manifest))
    }

    async fn read_result(&self, destination: &Path) -> Result<Option<RootResult>, RootStoreError> {
        self.read_optional(&result_object_path(destination)).await
    }

    async fn publish_result(&self, result: RootResult) -> Result<DurableRoot, RootStoreError> {
        let destination = Path::from(result.owner.destination_path.clone());
        let path = result_object_path(&destination);
        match self
            .object_store
            .put_opts(
                &path,
                serde_json::to_vec(&result)?.into(),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => Ok(result.root),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let canonical = self
                    .read_result(&destination)
                    .await?
                    .ok_or_else(|| RootStoreError::MissingResult(destination.to_string()))?;
                self.reconcile_published_result(&destination, &result, canonical)
                    .await
            }
            Err(error) => {
                if let Some(canonical) = self.read_result(&destination).await? {
                    self.reconcile_published_result(&destination, &result, canonical)
                        .await
                } else {
                    Err(error.into())
                }
            }
        }
    }

    async fn reconcile_published_result(
        &self,
        destination: &Path,
        candidate: &RootResult,
        canonical: RootResult,
    ) -> Result<DurableRoot, RootStoreError> {
        if canonical.owner != candidate.owner {
            return Err(RootStoreError::OwnershipConflict(destination.to_string()));
        }
        self.verify(&canonical.root).await?;
        if canonical.root != candidate.root
            && let Ok((losing_checkpoint, _)) = decode_root_checkpoint(&candidate.root.manifest_id)
        {
            let _ = self
                .admin(destination.clone())
                .delete_checkpoint(losing_checkpoint)
                .await;
        }
        Ok(canonical.root)
    }

    async fn publish_writer_head_result(
        &self,
        path: &Path,
        candidate: WriterHeadResult,
    ) -> Result<DurableRoot, RootStoreError> {
        match self
            .object_store
            .put_opts(
                path,
                serde_json::to_vec(&candidate)?.into(),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => Ok(candidate.root),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let canonical = self
                    .read_optional::<WriterHeadResult>(path)
                    .await?
                    .ok_or_else(|| RootStoreError::MissingResult(path.to_string()))?;
                if canonical.schema_version != candidate.schema_version
                    || canonical.branch_id != candidate.branch_id
                    || canonical.writer_lease_id != candidate.writer_lease_id
                    || canonical.destination_path != candidate.destination_path
                    || canonical.previous_root != candidate.previous_root
                {
                    return Err(RootStoreError::OwnershipConflict(
                        candidate.destination_path,
                    ));
                }
                self.verify(&canonical.root).await?;
                Ok(canonical.root)
            }
            Err(error) => {
                if let Some(canonical) = self.read_optional::<WriterHeadResult>(path).await?
                    && canonical.schema_version == candidate.schema_version
                    && canonical.branch_id == candidate.branch_id
                    && canonical.writer_lease_id == candidate.writer_lease_id
                    && canonical.destination_path == candidate.destination_path
                    && canonical.previous_root == candidate.previous_root
                {
                    self.verify(&canonical.root).await?;
                    return Ok(canonical.root);
                }
                Err(error.into())
            }
        }
    }

    async fn cleanup_operation_checkpoints(
        &self,
        destination: &Path,
        operation_id: Uuid,
        canonical: &DurableRoot,
    ) {
        let Ok((canonical_checkpoint, _)) = decode_root_checkpoint(&canonical.manifest_id) else {
            return;
        };
        let name = format!("{ROOT_CHECKPOINT_PREFIX}{operation_id}");
        let Ok(checkpoints) = self
            .admin(destination.clone())
            .list_checkpoints(Some(&name))
            .await
        else {
            return;
        };
        for checkpoint in checkpoints {
            if checkpoint.id != canonical_checkpoint {
                let _ = self
                    .admin(destination.clone())
                    .delete_checkpoint(checkpoint.id)
                    .await;
            }
        }
    }

    async fn cleanup_named_checkpoints(
        &self,
        destination: &Path,
        name: &str,
        canonical: &DurableRoot,
    ) {
        let Ok((canonical_checkpoint, _)) = decode_root_checkpoint(&canonical.manifest_id) else {
            return;
        };
        let Ok(checkpoints) = self
            .admin(destination.clone())
            .list_checkpoints(Some(name))
            .await
        else {
            return;
        };
        for checkpoint in checkpoints {
            if checkpoint.id != canonical_checkpoint {
                let _ = self
                    .admin(destination.clone())
                    .delete_checkpoint(checkpoint.id)
                    .await;
            }
        }
    }

    async fn read_optional<T: DeserializeOwned>(
        &self,
        path: &Path,
    ) -> Result<Option<T>, RootStoreError> {
        match self.object_store.get(path).await {
            Ok(result) => Ok(Some(serde_json::from_slice(&result.bytes().await?)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn admin(&self, path: Path) -> Admin {
        let mut builder = AdminBuilder::new(path, Arc::clone(&self.object_store));
        if let Some(wal_object_store) = &self.wal_object_store {
            builder = builder.with_wal_object_store(Arc::clone(wal_object_store));
        }
        builder.build()
    }
}

fn manifest_owned_sst_ids(
    manifest: &slatedb::VersionedManifest,
) -> BTreeSet<slatedb::manifest::SsTableId> {
    let mut ids = BTreeSet::new();
    ids.extend(manifest.l0().iter().map(|view| view.sst.id));
    ids.extend(
        manifest
            .compacted()
            .iter()
            .flat_map(|run| run.sst_views.iter().map(|view| view.sst.id)),
    );
    for segment in manifest.segments() {
        ids.extend(segment.l0().iter().map(|view| view.sst.id));
        ids.extend(
            segment
                .compacted()
                .iter()
                .flat_map(|run| run.sst_views.iter().map(|view| view.sst.id)),
        );
    }
    for inherited in manifest.external_dbs() {
        for external_id in &inherited.sst_ids {
            ids.remove(external_id);
        }
    }
    ids
}

fn validate_database_path(label: &str, path: &Path) -> Result<(), RootStoreError> {
    let value = path.to_string();
    if value.is_empty() || value.len() > MAX_ROOT_IDENTIFIER_BYTES {
        return Err(RootStoreError::Invalid(format!(
            "{label} database path must contain 1..={MAX_ROOT_IDENTIFIER_BYTES} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(RootStoreError::Invalid(format!(
            "{label} database path cannot contain control characters"
        )));
    }
    Ok(())
}

fn database_paths_overlap(left: &Path, right: &Path) -> bool {
    let left = left.to_string();
    let right = right.to_string();
    left == right
        || right
            .strip_prefix(&left)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || left
            .strip_prefix(&right)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(crate) fn ensure_database_namespaces_disjoint(
    left_label: &str,
    left: &Path,
    right_label: &str,
    right: &Path,
) -> Result<(), RootStoreError> {
    validate_database_path(left_label, left)?;
    validate_database_path(right_label, right)?;
    if database_paths_overlap(left, right) {
        return Err(RootStoreError::Invalid(format!(
            "{left_label} and {right_label} database namespaces must not overlap"
        )));
    }
    Ok(())
}

fn ensure_no_wal_dependency(manifest: &slatedb::VersionedManifest) -> Result<(), RootStoreError> {
    if manifest.next_wal_sst_id() > manifest.replay_after_wal_id().saturating_add(1) {
        return Err(RootStoreError::WalDependency {
            manifest_id: manifest.id(),
            replay_after: manifest.replay_after_wal_id(),
            next_wal: manifest.next_wal_sst_id(),
        });
    }
    Ok(())
}

fn shared_source_checkpoint_identity(
    path: &Path,
    source_checkpoint_id: Uuid,
    manifest_id: u64,
) -> (Uuid, String) {
    let mut hash = Sha256::new();
    hash.update(b"zerofs-shared-source-checkpoint-v1\0");
    hash.update(path.to_string().as_bytes());
    hash.update(source_checkpoint_id.as_bytes());
    hash.update(manifest_id.to_be_bytes());
    let digest = hash.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    (
        Uuid::from_bytes(id),
        format!("{SHARED_SOURCE_CHECKPOINT_PREFIX}{source_checkpoint_id}_{manifest_id}"),
    )
}

fn branch_root_checkpoint_identity(operation_id: Uuid, destination_id: Uuid) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"zerofs-branch-root-checkpoint-v1\0");
    hash.update(operation_id.as_bytes());
    hash.update(destination_id.as_bytes());
    let digest = hash.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    // Use RFC 4122 variant/version bits so the derived physical identifier is
    // visibly distinct from either catalog identity supplied by the caller.
    id[6] = (id[6] & 0x0f) | 0x50;
    id[8] = (id[8] & 0x3f) | 0x80;
    Uuid::from_bytes(id)
}

fn valid_shared_source_checkpoint(path: &Path, checkpoint: &slatedb::Checkpoint) -> bool {
    let Some(name) = checkpoint
        .name
        .as_deref()
        .and_then(|name| name.strip_prefix(SHARED_SOURCE_CHECKPOINT_PREFIX))
    else {
        return false;
    };
    let Some((source_id, manifest_id)) = name.rsplit_once('_') else {
        return false;
    };
    let Ok(source_id) = Uuid::parse_str(source_id) else {
        return false;
    };
    let Ok(manifest_id) = manifest_id.parse::<u64>() else {
        return false;
    };
    let (expected_id, expected_name) =
        shared_source_checkpoint_identity(path, source_id, manifest_id);
    checkpoint.id == expected_id
        && checkpoint.manifest_id == manifest_id
        && checkpoint.expire_time.is_none()
        && checkpoint.name.as_deref() == Some(expected_name.as_str())
}

fn owner_object_path(destination: &Path) -> Path {
    destination.clone().join(ROOT_OWNER_OBJECT)
}

fn result_object_path(destination: &Path) -> Path {
    destination.clone().join(ROOT_RESULT_OBJECT)
}

fn head_result_object_path(destination: &Path, writer_lease_id: Uuid) -> Path {
    destination
        .clone()
        .join(format!("{HEAD_RESULT_PREFIX}{writer_lease_id}.json"))
}

fn encode_root_checkpoint(checkpoint_id: Uuid, manifest_id: u64) -> String {
    format!("{checkpoint_id}@{manifest_id}")
}

fn decode_root_checkpoint(value: &str) -> Result<(Uuid, u64), RootStoreError> {
    let (checkpoint, manifest) = value.split_once('@').ok_or_else(|| {
        RootStoreError::Invalid(
            "SlateDB root identity must contain checkpoint and manifest".to_string(),
        )
    })?;
    let checkpoint_id = Uuid::parse_str(checkpoint).map_err(|error| {
        RootStoreError::Invalid(format!("invalid SlateDB root checkpoint UUID: {error}"))
    })?;
    let manifest_id = manifest.parse::<u64>().map_err(|_| {
        RootStoreError::Invalid("SlateDB root manifest identity must be a u64".to_string())
    })?;
    Ok((checkpoint_id, manifest_id))
}

#[derive(Debug, thiserror::Error)]
pub enum RootStoreError {
    #[error("invalid durable root request: {0}")]
    Invalid(String),
    #[error("destination database already exists without a root owner: {0}")]
    UnownedDestination(String),
    #[error("destination root owner conflicts with this request: {0}")]
    OwnershipConflict(String),
    #[error("destination root owner disappeared after conditional creation: {0}")]
    MissingOwner(String),
    #[error("destination root result disappeared after conditional creation: {0}")]
    MissingResult(String),
    #[error("source checkpoint not found: {0}")]
    MissingSourceCheckpoint(Uuid),
    #[error(
        "source checkpoint {checkpoint_id} manifest mismatch: expected {expected}, found {actual}"
    )]
    SourceManifestMismatch {
        checkpoint_id: Uuid,
        expected: u64,
        actual: u64,
    },
    #[error(
        "source checkpoint {checkpoint_id} name mismatch: expected {expected:?}, found {actual:?}"
    )]
    SourceCheckpointNameMismatch {
        checkpoint_id: Uuid,
        expected: String,
        actual: Option<String>,
    },
    #[error("public source checkpoint name {0:?} does not resolve to a physical checkpoint")]
    MissingSourceCheckpointName(String),
    #[error("multiple physical checkpoints use public source name {0:?}")]
    DuplicateSourceCheckpointName(String),
    #[error(
        "public source checkpoint name {name:?} resolves to UUID {actual}, expected {expected}"
    )]
    SourceCheckpointNameIdentityMismatch {
        name: String,
        expected: Uuid,
        actual: Uuid,
    },
    #[error("source checkpoint {0} is expiring and cannot become a catalog root")]
    ExpiringSourceCheckpoint(Uuid),
    #[error(
        "source checkpoint {checkpoint_id} creation time mismatch: expected {expected}, found {actual}"
    )]
    SourceCheckpointCreateTimeMismatch {
        checkpoint_id: Uuid,
        expected: DateTime<Utc>,
        actual: DateTime<Utc>,
    },
    #[error(
        "destination {destination} is attached to a different source than {source_path} checkpoint {checkpoint_id}"
    )]
    WrongSource {
        destination: String,
        source_path: String,
        checkpoint_id: Uuid,
    },
    #[error("durable manifest not found: {0}")]
    MissingManifest(String),
    #[error("durable root checkpoint not found: {0}")]
    MissingRootCheckpoint(String),
    #[error("durable root is not the canonical operation result: {0}")]
    NonCanonicalRoot(String),
    #[error(
        "writer head did not advance SlateDB writer incarnation: previous epoch {previous}, current epoch {current}"
    )]
    StaleWriterIncarnation { previous: u64, current: u64 },
    #[error(
        "writer head incarnation proof changed: previous expected {expected_previous}, found {actual_previous}; current expected {expected_current}, found {actual_current}"
    )]
    WriterIncarnationMismatch {
        expected_previous: u64,
        actual_previous: u64,
        expected_current: u64,
        actual_current: u64,
    },
    #[error(
        "root checkpoint {checkpoint_id} manifest mismatch: expected {expected}, found {actual}"
    )]
    RootManifestMismatch {
        checkpoint_id: Uuid,
        expected: u64,
        actual: u64,
    },
    #[error(
        "manifest {manifest_id} requires WAL ids after {replay_after} and before {next_wal}; branch roots must be fully flushed"
    )]
    WalDependency {
        manifest_id: u64,
        replay_after: u64,
        next_wal: u64,
    },
    #[error("SlateDB root is not initialized: {0}")]
    Uninitialized(String),
    #[error("external source {source_path} is missing final checkpoint pin {checkpoint_id:?}")]
    MissingExternalPin {
        source_path: String,
        checkpoint_id: Option<Uuid>,
    },
    #[error(
        "external source {source_path} checkpoint {checkpoint_id} does not cover every referenced SST"
    )]
    ExternalPinCoverage {
        source_path: String,
        checkpoint_id: Uuid,
    },
    #[error("durable root references a missing SST object: {0}")]
    MissingSst(String),
    #[error("SlateDB clone failed: {0}")]
    Clone(String),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    SlateDb(#[from] slatedb::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionConfig;
    use crate::fault_store::FaultStore;
    use crate::frame_codec::FrameCodec;
    use crate::fs::key_codec::KeyCodec;
    use crate::segment::{FrameLoc, SEGMENT_INFO};
    use crate::segment_store::SegmentStore;
    use bytes::Bytes;
    use futures::StreamExt;
    use slatedb::Db;
    use slatedb::config::{CheckpointOptions, CheckpointScope};
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::prefix::PrefixStore;

    #[test]
    fn rejects_overlapping_catalog_and_branch_namespaces_without_io() {
        for (catalog, branches) in [
            ("zerofs/catalog", "zerofs/catalog"),
            ("zerofs/catalog", "zerofs/catalog/branches"),
            ("zerofs/catalog/metadata", "zerofs/catalog"),
        ] {
            assert!(matches!(
                ensure_database_namespaces_disjoint(
                    "catalog",
                    &Path::from(catalog),
                    "branch root",
                    &Path::from(branches),
                ),
                Err(RootStoreError::Invalid(_))
            ));
        }
        ensure_database_namespaces_disjoint(
            "catalog",
            &Path::from("zerofs/catalog"),
            "branch root",
            &Path::from("zerofs/branches"),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn shallow_clone_reads_inherited_data_from_the_shared_segment_pool() {
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let pool_path = Path::from("volume/segment-pool");
        let pool: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(Arc::clone(&raw), pool_path.clone()));
        let authority = SegmentStore::open_or_create_pool_authority(
            Arc::clone(&pool),
            &[9u8; 32],
            "source",
            true,
        )
        .await
        .unwrap();
        let epoch = SegmentStore::reserve_epoch(Arc::clone(&pool), &authority, "source")
            .await
            .unwrap();
        let segments = SegmentStore::new(
            Arc::clone(&pool),
            FrameCodec::new(&[9u8; 32], SEGMENT_INFO, CompressionConfig::Lz4),
            epoch,
            None,
        );
        let locations = segments
            .seal(&[(1, 0, Bytes::from_static(b"shared-data"))])
            .await
            .unwrap();
        let source_path = Path::from("volume/source");
        let source = Db::builder(source_path.clone(), Arc::clone(&raw))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        let key = KeyCodec::new().extent_key(1, 0);
        source.put(&key, locations[0].2.encode()).await.unwrap();
        source.flush().await.unwrap();
        let checkpoint = source
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        let exact_source = ImmutableCheckpoint {
            database_path: source_path,
            checkpoint_id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        };
        let roots = SlateDbRootStore::new(Arc::clone(&raw), Path::from("volume/branches"))
            .with_segment_pool_root(pool_path);
        let root = roots
            .create_from_checkpoint(Uuid::new_v4(), Uuid::new_v4(), &exact_source)
            .await
            .unwrap();
        let reader = roots.checkpoint_reader(&root).await.unwrap();
        let encoded = reader.get(&key).await.unwrap().unwrap();
        let inherited = FrameLoc::decode(&encoded).unwrap();
        assert_eq!(
            segments.read_extent(inherited, 1, 0).await.unwrap(),
            Bytes::from_static(b"shared-data")
        );
        reader.close().await.unwrap();
        source.close().await.unwrap();
    }

    #[tokio::test]
    async fn catalog_clones_create_one_hidden_pin_and_survive_customer_pin_deletion() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("shared-pin-source");
        let source = Db::open(source_path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        source.put(b"key", b"value").await.unwrap();
        source.flush().await.unwrap();
        let checkpoint = source
            .create_checkpoint(
                CheckpointScope::All,
                &CheckpointOptions {
                    name: Some("permanent-source".to_string()),
                    ..CheckpointOptions::default()
                },
            )
            .await
            .unwrap();
        source.close().await.unwrap();
        let source_admin = AdminBuilder::new(source_path.clone(), Arc::clone(&store)).build();
        let source_version = source_admin
            .read_manifest(None)
            .await
            .unwrap()
            .unwrap()
            .id();
        let exact = ImmutableCheckpoint {
            database_path: source_path,
            checkpoint_id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        };
        let roots = SlateDbRootStore::new(Arc::clone(&store), Path::from("shared-pin-branches"));

        let (left, right) = tokio::join!(
            roots.create_from_checkpoint_shared(Uuid::new_v4(), Uuid::new_v4(), &exact),
            roots.create_from_checkpoint_shared(Uuid::new_v4(), Uuid::new_v4(), &exact)
        );
        let left = left.unwrap();
        let right = right.unwrap();
        let shared_version = source_admin
            .read_manifest(None)
            .await
            .unwrap()
            .unwrap()
            .id();

        assert_eq!(
            source_admin
                .read_manifest(None)
                .await
                .unwrap()
                .unwrap()
                .id(),
            shared_version,
            "all clones of one immutable source must reuse one hidden physical pin"
        );
        assert_eq!(shared_version, source_version + 1);
        for root in [&left, &right] {
            assert!(matches!(
                store
                    .get(&owner_object_path(&Path::from(root.identity.clone())))
                    .await,
                Err(object_store::Error::NotFound { .. })
            ));
        }
        source_admin.delete_checkpoint(checkpoint.id).await.unwrap();
        roots.verify(&left).await.unwrap();
        roots.verify(&right).await.unwrap();
    }

    #[tokio::test]
    async fn overlapping_shared_pin_proofs_coalesce_but_later_waves_revalidate() {
        let (fault_store, faults) = FaultStore::new(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = fault_store;
        let source_path = Path::from("shared-pin-proof-source");
        let source = Db::open(source_path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        source.put(b"key", b"value").await.unwrap();
        source.flush().await.unwrap();
        let checkpoint = source
            .create_checkpoint(
                CheckpointScope::All,
                &CheckpointOptions {
                    name: Some("permanent-source".to_string()),
                    ..CheckpointOptions::default()
                },
            )
            .await
            .unwrap();
        source.close().await.unwrap();
        let exact = ImmutableCheckpoint {
            database_path: source_path.clone(),
            checkpoint_id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        };
        let roots = SlateDbRootStore::new(Arc::clone(&store), Path::from("shared-pin-proof-roots"));
        let prepared = roots.authenticated_shared_source(&exact).await.unwrap();

        faults.reset_counts();
        faults.delay_gets(1, std::time::Duration::from_millis(50));
        let (left, right) = tokio::join!(
            roots.verify_shared_pin(source_path.clone(), prepared.checkpoint.checkpoint_id),
            roots.verify_shared_pin(source_path.clone(), prepared.checkpoint.checkpoint_id),
        );
        assert_eq!(left.unwrap(), right.unwrap());
        assert_eq!(
            faults.list_count(),
            1,
            "overlapping callers must share one physical pin proof"
        );

        faults.reset_counts();
        faults.delay_gets(1, std::time::Duration::from_millis(50));
        let cancelled = {
            let roots = roots.clone();
            let source_path = source_path.clone();
            let checkpoint_id = prepared.checkpoint.checkpoint_id;
            tokio::spawn(async move { roots.verify_shared_pin(source_path, checkpoint_id).await })
        };
        while faults.list_count() == 0 {
            tokio::task::yield_now().await;
        }
        let survivor = {
            let roots = roots.clone();
            let source_path = source_path.clone();
            let checkpoint_id = prepared.checkpoint.checkpoint_id;
            tokio::spawn(async move { roots.verify_shared_pin(source_path, checkpoint_id).await })
        };
        tokio::task::yield_now().await;
        cancelled.abort();
        tokio::time::timeout(std::time::Duration::from_secs(1), survivor)
            .await
            .expect("cancelling the first waiter must not cancel the detached proof")
            .unwrap()
            .unwrap();
        assert_eq!(
            faults.list_count(),
            1,
            "a cancelled first waiter must not start a duplicate proof"
        );

        faults.reset_counts();
        roots
            .verify_shared_pin(source_path.clone(), prepared.checkpoint.checkpoint_id)
            .await
            .unwrap();
        assert_eq!(
            faults.list_count(),
            1,
            "later waves must revalidate storage"
        );

        roots
            .admin(source_path.clone())
            .delete_checkpoint(prepared.checkpoint.checkpoint_id)
            .await
            .unwrap();
        faults.reset_counts();
        faults.delay_gets(1, std::time::Duration::from_millis(50));
        let (left, right) = tokio::join!(
            roots.verify_shared_pin(source_path.clone(), prepared.checkpoint.checkpoint_id),
            roots.verify_shared_pin(source_path.clone(), prepared.checkpoint.checkpoint_id),
        );
        assert!(matches!(
            left,
            Err(RootStoreError::MissingExternalPin { .. })
        ));
        assert!(matches!(
            right,
            Err(RootStoreError::MissingExternalPin { .. })
        ));
        assert_eq!(
            faults.list_count(),
            1,
            "overlapping callers must share a failed physical proof"
        );

        roots.ensure_shared_source_pin(&exact).await.unwrap();
        roots
            .verify_shared_pin(source_path, prepared.checkpoint.checkpoint_id)
            .await
            .expect("a failed proof must be evicted so a later wave can retry");
    }

    #[tokio::test]
    async fn shared_pin_recovery_rejects_a_conflicting_stable_descriptor() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("shared-pin-conflict-source");
        let source = Db::open(source_path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        source.put(b"key", b"value").await.unwrap();
        source.flush().await.unwrap();
        let checkpoint = source
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source.close().await.unwrap();
        let exact = ImmutableCheckpoint {
            database_path: source_path.clone(),
            checkpoint_id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        };
        let (stable_id, _) =
            shared_source_checkpoint_identity(&source_path, checkpoint.id, checkpoint.manifest_id);
        AdminBuilder::new(source_path, Arc::clone(&store))
            .build()
            .create_detached_checkpoint_with_id(
                stable_id,
                &CheckpointOptions {
                    lifetime: None,
                    source: Some(checkpoint.id),
                    name: Some("wrong-hidden-name".to_string()),
                },
            )
            .await
            .unwrap();
        let roots = SlateDbRootStore::new(store, Path::from("shared-pin-conflict-branches"));
        assert!(
            roots
                .create_from_checkpoint_shared(Uuid::new_v4(), Uuid::new_v4(), &exact)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn shared_recovery_rejects_initialized_destination_from_another_checkpoint() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("shared-wrong-source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        source_db.put(b"key", b"first").await.unwrap();
        let first = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.put(b"key", b"second").await.unwrap();
        let second = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        assert_ne!(first.manifest_id, second.manifest_id);

        let roots = SlateDbRootStore::new(
            Arc::clone(&store),
            Path::from("shared-wrong-source-branches"),
        );
        let requested = ImmutableCheckpoint {
            database_path: source_path.clone(),
            checkpoint_id: first.id,
            manifest_id: first.manifest_id,
        };
        let other = ImmutableCheckpoint {
            database_path: source_path,
            checkpoint_id: second.id,
            manifest_id: second.manifest_id,
        };
        let prepared_other = roots.authenticated_shared_source(&other).await.unwrap();
        let destination_id = Uuid::new_v4();
        let destination = roots
            .branch_database_root
            .clone()
            .join(destination_id.to_string());
        roots
            .admin(destination.clone())
            .create_clone_builder_from_source(
                CloneSourceSpec::with_checkpoint(
                    prepared_other.checkpoint.database_path.clone(),
                    prepared_other.checkpoint.checkpoint_id,
                )
                .with_preloaded_manifest(
                    prepared_other.physical_checkpoint,
                    prepared_other.manifest,
                ),
            )
            .with_shared_source_pins()
            .build()
            .await
            .unwrap();

        assert!(
            roots
                .create_from_checkpoint_shared(Uuid::new_v4(), destination_id, &requested)
                .await
                .is_err(),
            "an initialized clone from another checkpoint of the same DB must not be adopted"
        );
        assert!(matches!(
            store.get(&result_object_path(&destination)).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn private_recovery_rejects_initialized_destination_from_another_checkpoint() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("private-wrong-source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        source_db.put(b"key", b"first").await.unwrap();
        let first = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.put(b"key", b"second").await.unwrap();
        let second = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let requested = ImmutableCheckpoint {
            database_path: source_path.clone(),
            checkpoint_id: first.id,
            manifest_id: first.manifest_id,
        };
        let operation_id = Uuid::new_v4();
        let destination_id = Uuid::new_v4();
        let roots = SlateDbRootStore::new(
            Arc::clone(&store),
            Path::from("private-wrong-source-branches"),
        );
        let destination = roots
            .branch_database_root
            .clone()
            .join(destination_id.to_string());
        let destination_admin = roots.admin(destination.clone());
        roots
            .ensure_owner(
                &RootOwner {
                    schema_version: ROOT_DESCRIPTOR_SCHEMA_VERSION,
                    operation_id,
                    destination_id,
                    destination_path: destination.to_string(),
                    source_path: source_path.to_string(),
                    source_checkpoint_id: first.id,
                    source_manifest_id: first.manifest_id,
                },
                &destination_admin,
            )
            .await
            .unwrap();
        destination_admin
            .create_clone_builder_from_source(CloneSourceSpec::with_checkpoint(
                source_path,
                second.id,
            ))
            .build()
            .await
            .unwrap();

        assert!(
            roots
                .create_from_checkpoint(operation_id, destination_id, &requested)
                .await
                .is_err(),
            "private recovery must retain the owner's exact requested checkpoint"
        );
        assert!(matches!(
            store.get(&result_object_path(&destination)).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn shared_recovery_installs_the_exact_root_after_initialized_clone() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("shared-stable-root-source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        source_db.put(b"key", b"value").await.unwrap();
        let checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let source = ImmutableCheckpoint {
            database_path: source_path,
            checkpoint_id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        };
        let roots = SlateDbRootStore::new(
            Arc::clone(&store),
            Path::from("shared-stable-root-branches"),
        );
        let prepared = roots.authenticated_shared_source(&source).await.unwrap();
        let operation_id = Uuid::new_v4();
        let destination_id = Uuid::new_v4();
        let destination = roots
            .branch_database_root
            .clone()
            .join(destination_id.to_string());
        let destination_admin = roots.admin(destination.clone());
        destination_admin
            .create_clone_builder_from_source(
                CloneSourceSpec::with_checkpoint(
                    prepared.checkpoint.database_path.clone(),
                    prepared.checkpoint.checkpoint_id,
                )
                .with_preloaded_manifest(prepared.physical_checkpoint, prepared.manifest),
            )
            .with_shared_source_pins()
            .build()
            .await
            .unwrap();

        let root = roots
            .create_from_checkpoint_shared(operation_id, destination_id, &source)
            .await
            .unwrap();
        let (root_checkpoint_id, _) = decode_root_checkpoint(&root.manifest_id).unwrap();
        assert_eq!(
            root_checkpoint_id,
            branch_root_checkpoint_identity(operation_id, destination_id)
        );
        let checkpoint_name = format!("{ROOT_CHECKPOINT_PREFIX}{operation_id}");
        let matching = destination_admin
            .list_checkpoints(Some(&checkpoint_name))
            .await
            .unwrap();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].id, root_checkpoint_id);
        assert_eq!(
            roots
                .create_from_checkpoint_shared(operation_id, destination_id, &source)
                .await
                .unwrap(),
            root
        );
    }

    #[tokio::test]
    async fn verification_rejects_borrowed_customer_named_checkpoint() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("borrowed-customer-source");
        let source = Db::open(source_path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        source.put(b"key", b"value").await.unwrap();
        source.flush().await.unwrap();
        let checkpoint = source
            .create_checkpoint(
                CheckpointScope::All,
                &CheckpointOptions {
                    name: Some("customer-visible".to_string()),
                    ..CheckpointOptions::default()
                },
            )
            .await
            .unwrap();
        source.close().await.unwrap();
        let destination = Path::from("borrowed-customer-destination");
        let destination_admin = AdminBuilder::new(destination.clone(), Arc::clone(&store)).build();
        destination_admin
            .create_clone_builder_from_source(CloneSourceSpec::with_checkpoint(
                source_path,
                checkpoint.id,
            ))
            .with_shared_source_pins()
            .build()
            .await
            .unwrap();
        let operation_id = Uuid::new_v4();
        let root_checkpoint = destination_admin
            .create_detached_checkpoint(&CheckpointOptions {
                name: Some(format!("{ROOT_CHECKPOINT_PREFIX}{operation_id}")),
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();
        let roots = SlateDbRootStore::new(store, Path::from("unused"));
        let root = DurableRoot {
            identity: destination.to_string(),
            manifest_id: encode_root_checkpoint(root_checkpoint.id, root_checkpoint.manifest_id),
        };
        assert!(roots.verify(&root).await.is_err());
    }

    #[tokio::test]
    async fn clone_root_survives_named_source_deletion_and_retries_exactly() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("root-store/source");
        let destination_id = Uuid::new_v4();
        let destination_path = Path::from(format!("root-store/{destination_id}"));
        let source_db = Db::open(source_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        source_db.put(b"key", b"source-value").await.unwrap();
        let checkpoint = source_db
            .create_checkpoint(
                CheckpointScope::All,
                &CheckpointOptions {
                    name: Some("customer-checkpoint".to_string()),
                    ..CheckpointOptions::default()
                },
            )
            .await
            .unwrap();
        source_db.close().await.unwrap();

        let source = ImmutableCheckpoint {
            database_path: source_path.clone(),
            checkpoint_id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        };
        let operation_id = Uuid::new_v4();
        let roots = SlateDbRootStore::new(Arc::clone(&object_store), Path::from("root-store"));
        let root = roots
            .create_from_checkpoint(operation_id, destination_id, &source)
            .await
            .unwrap();

        let source_admin = roots.admin(source_path.clone());
        let final_pin = source_admin
            .list_checkpoints(None)
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id != checkpoint.id && candidate.name.is_none())
            .expect("clone must install an unnamed final checkpoint");
        source_admin.delete_checkpoint(checkpoint.id).await.unwrap();
        assert_eq!(
            roots
                .create_from_checkpoint(operation_id, destination_id, &source,)
                .await
                .unwrap(),
            root
        );
        let mut changed_source = source.clone();
        changed_source.manifest_id += 1;
        assert!(matches!(
            roots
                .create_from_checkpoint(operation_id, destination_id, &changed_source,)
                .await,
            Err(RootStoreError::OwnershipConflict(_))
        ));
        assert!(matches!(
            roots
                .create_from_checkpoint(Uuid::new_v4(), destination_id, &source,)
                .await,
            Err(RootStoreError::OwnershipConflict(_))
        ));
        roots.verify(&root).await.unwrap();

        let destination = Db::open(destination_path, Arc::clone(&object_store))
            .await
            .unwrap();
        assert_eq!(
            destination.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"source-value"))
        );
        destination.put(b"key", b"child-value").await.unwrap();
        destination.close().await.unwrap();

        let reopened_source = Db::open(source_path, Arc::clone(&object_store))
            .await
            .unwrap();
        assert_eq!(
            reopened_source.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"source-value"))
        );
        reopened_source.close().await.unwrap();

        source_admin.delete_checkpoint(final_pin.id).await.unwrap();
        assert!(matches!(
            roots.verify(&root).await,
            Err(RootStoreError::MissingExternalPin { .. })
        ));
    }

    #[tokio::test]
    async fn writer_head_publication_is_immutable_exact_and_advances_readable_state() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("root-store/head-source");
        let source_db = Db::builder(source_path.clone(), Arc::clone(&object_store))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        let key = KeyCodec::new().inode_key(1);
        source_db.put(&key, b"source").await.unwrap();
        source_db.flush().await.unwrap();
        let checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();

        let branch_id = Uuid::new_v4();
        let roots = SlateDbRootStore::new(Arc::clone(&object_store), Path::from("root-store"));
        let initial = roots
            .create_from_checkpoint(
                Uuid::new_v4(),
                branch_id,
                &ImmutableCheckpoint {
                    database_path: source_path,
                    checkpoint_id: checkpoint.id,
                    manifest_id: checkpoint.manifest_id,
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            roots
                .publish_writer_head(branch_id, Uuid::new_v4(), &initial)
                .await,
            Err(RootStoreError::StaleWriterIncarnation { previous, current })
                if current == previous
        ));
        let destination = Path::from(initial.identity.clone());
        let writer = Db::builder(destination, Arc::clone(&object_store))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        writer.put(&key, b"writer").await.unwrap();
        writer.flush().await.unwrap();
        writer.close().await.unwrap();

        let lease_id = Uuid::new_v4();
        let (left, right) = tokio::join!(
            roots.publish_writer_head(branch_id, lease_id, &initial),
            roots.publish_writer_head(branch_id, lease_id, &initial)
        );
        let head = left.unwrap();
        assert_eq!(right.unwrap(), head);
        assert_ne!(head, initial);
        assert_eq!(
            roots
                .checkpoint_reader(&initial)
                .await
                .unwrap()
                .get(&key)
                .await
                .unwrap(),
            Some(Bytes::from_static(b"source"))
        );
        assert_eq!(
            roots
                .checkpoint_reader(&head)
                .await
                .unwrap()
                .get(&key)
                .await
                .unwrap(),
            Some(Bytes::from_static(b"writer"))
        );
        roots.verify(&head).await.unwrap();
        assert!(matches!(
            roots
                .publish_writer_head(Uuid::new_v4(), lease_id, &initial)
                .await,
            Err(RootStoreError::OwnershipConflict(_))
        ));
        assert!(matches!(
            roots.publish_writer_head(branch_id, lease_id, &head).await,
            Err(RootStoreError::OwnershipConflict(_))
        ));
        let descriptor_path = head_result_object_path(&Path::from(head.identity.clone()), lease_id);
        let descriptor = roots
            .read_optional::<WriterHeadResult>(&descriptor_path)
            .await
            .unwrap()
            .unwrap();
        let mut forged_predecessor = descriptor.clone();
        let (_, previous_manifest_id) =
            decode_root_checkpoint(&forged_predecessor.previous_root.manifest_id).unwrap();
        forged_predecessor.previous_root.manifest_id =
            encode_root_checkpoint(Uuid::new_v4(), previous_manifest_id);
        object_store
            .put(
                &descriptor_path,
                serde_json::to_vec(&forged_predecessor).unwrap().into(),
            )
            .await
            .unwrap();
        assert!(matches!(
            roots.verify(&head).await,
            Err(RootStoreError::MissingRootCheckpoint(_))
        ));

        let mut forged_epoch = descriptor;
        forged_epoch.writer_epoch += 1;
        object_store
            .put(
                &descriptor_path,
                serde_json::to_vec(&forged_epoch).unwrap().into(),
            )
            .await
            .unwrap();
        assert!(matches!(
            roots.verify(&head).await,
            Err(RootStoreError::WriterIncarnationMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn new_clone_rejects_checkpoint_manifest_mismatch() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("root-store/mismatch-source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        let checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let roots = SlateDbRootStore::new(object_store, Path::from("root-store"));
        let destination_id = Uuid::new_v4();
        let error = roots
            .create_from_checkpoint(
                Uuid::new_v4(),
                destination_id,
                &ImmutableCheckpoint {
                    database_path: source_path,
                    checkpoint_id: checkpoint.id,
                    manifest_id: checkpoint.manifest_id + 1,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RootStoreError::SourceManifestMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn concurrent_exact_retries_converge_on_one_root() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("root-store/concurrent-source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        source_db.put(b"key", b"value").await.unwrap();
        let checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let source = ImmutableCheckpoint {
            database_path: source_path,
            checkpoint_id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        };
        let operation_id = Uuid::new_v4();
        let destination_id = Uuid::new_v4();
        let roots = SlateDbRootStore::new(object_store, Path::from("root-store"));

        let (left, right) = tokio::join!(
            roots.create_from_checkpoint(operation_id, destination_id, &source,),
            roots.create_from_checkpoint(operation_id, destination_id, &source,)
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(left, right);
        roots.verify(&left).await.unwrap();
    }

    #[tokio::test]
    async fn retry_elects_one_checkpoint_after_all_concurrent_creators_crash() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("root-store/crash-source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        source_db.put(b"key", b"value").await.unwrap();
        let source_checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();

        let operation_id = Uuid::new_v4();
        let destination_id = Uuid::new_v4();
        let destination = Path::from(format!("root-store/{destination_id}"));
        let source = ImmutableCheckpoint {
            database_path: source_path.clone(),
            checkpoint_id: source_checkpoint.id,
            manifest_id: source_checkpoint.manifest_id,
        };
        let owner = RootOwner {
            schema_version: ROOT_DESCRIPTOR_SCHEMA_VERSION,
            operation_id,
            destination_id,
            destination_path: destination.to_string(),
            source_path: source_path.to_string(),
            source_checkpoint_id: source_checkpoint.id,
            source_manifest_id: source_checkpoint.manifest_id,
        };
        let roots = SlateDbRootStore::new(object_store, Path::from("root-store"));
        let destination_admin = roots.admin(destination.clone());
        roots
            .ensure_owner(&owner, &destination_admin)
            .await
            .unwrap();
        destination_admin
            .create_clone_builder_from_source(CloneSourceSpec::with_checkpoint(
                source_path.clone(),
                source_checkpoint.id,
            ))
            .build()
            .await
            .unwrap();
        let checkpoint_name = format!("{ROOT_CHECKPOINT_PREFIX}{operation_id}");
        for _ in 0..2 {
            destination_admin
                .create_detached_checkpoint(&CheckpointOptions {
                    name: Some(checkpoint_name.clone()),
                    ..CheckpointOptions::default()
                })
                .await
                .unwrap();
        }
        roots
            .admin(source_path)
            .delete_checkpoint(source_checkpoint.id)
            .await
            .unwrap();

        let root = roots
            .create_from_checkpoint(operation_id, destination_id, &source)
            .await
            .expect("retry must deterministically recover the pre-result crash state");
        roots.verify(&root).await.unwrap();
        let remaining = destination_admin
            .list_checkpoints(Some(&checkpoint_name))
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(
            remaining[0].id,
            decode_root_checkpoint(&root.manifest_id).unwrap().0
        );
    }

    #[tokio::test]
    async fn public_verification_rejects_a_noncanonical_losing_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("root-store/loser-source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        source_db.put(b"key", b"value").await.unwrap();
        let source_checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let operation_id = Uuid::new_v4();
        let destination_id = Uuid::new_v4();
        let destination = Path::from(format!("root-store/{destination_id}"));
        let owner = RootOwner {
            schema_version: ROOT_DESCRIPTOR_SCHEMA_VERSION,
            operation_id,
            destination_id,
            destination_path: destination.to_string(),
            source_path: source_path.to_string(),
            source_checkpoint_id: source_checkpoint.id,
            source_manifest_id: source_checkpoint.manifest_id,
        };
        let roots = SlateDbRootStore::new(object_store, Path::from("root-store"));
        let destination_admin = roots.admin(destination.clone());
        roots
            .ensure_owner(&owner, &destination_admin)
            .await
            .unwrap();
        destination_admin
            .create_clone_builder_from_source(CloneSourceSpec::with_checkpoint(
                source_path,
                source_checkpoint.id,
            ))
            .build()
            .await
            .unwrap();
        let checkpoint_name = format!("{ROOT_CHECKPOINT_PREFIX}{operation_id}");
        let first = destination_admin
            .create_detached_checkpoint(&CheckpointOptions {
                name: Some(checkpoint_name.clone()),
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();
        let second = destination_admin
            .create_detached_checkpoint(&CheckpointOptions {
                name: Some(checkpoint_name.clone()),
                ..CheckpointOptions::default()
            })
            .await
            .unwrap();
        let canonical = DurableRoot {
            identity: destination.to_string(),
            manifest_id: encode_root_checkpoint(first.id, first.manifest_id),
        };
        let losing = DurableRoot {
            identity: destination.to_string(),
            manifest_id: encode_root_checkpoint(second.id, second.manifest_id),
        };
        roots
            .verify_storage_root(&canonical, &checkpoint_name)
            .await
            .unwrap();
        roots
            .publish_result(RootResult {
                owner,
                root: canonical,
            })
            .await
            .unwrap();

        assert!(matches!(
            roots.verify(&losing).await,
            Err(RootStoreError::NonCanonicalRoot(_))
        ));
    }

    #[tokio::test]
    async fn verification_rejects_a_missing_reachable_external_sst() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("root-store/missing-sst-source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        source_db.put(b"key", b"value").await.unwrap();
        let source_checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let source = ImmutableCheckpoint {
            database_path: source_path,
            checkpoint_id: source_checkpoint.id,
            manifest_id: source_checkpoint.manifest_id,
        };
        let destination_id = Uuid::new_v4();
        let roots = SlateDbRootStore::new(Arc::clone(&object_store), Path::from("root-store"));
        let root = roots
            .create_from_checkpoint(Uuid::new_v4(), destination_id, &source)
            .await
            .unwrap();
        let (_, manifest_id) = decode_root_checkpoint(&root.manifest_id).unwrap();
        let manifest = roots
            .admin(Path::from(root.identity.clone()))
            .read_manifest(Some(manifest_id))
            .await
            .unwrap()
            .unwrap();
        let external = manifest.external_dbs().first().unwrap();
        let sst_id = *external.sst_ids.first().unwrap();
        let missing_path =
            PathResolver::new(Path::from(root.identity.clone()), &manifest).sst_path(&sst_id);
        object_store.delete(&missing_path).await.unwrap();

        assert!(matches!(
            roots.verify(&root).await,
            Err(RootStoreError::MissingSst(_))
        ));
    }

    #[tokio::test]
    async fn verification_enumerates_segmented_manifest_ssts() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("root-store/segmented-source");
        let source_db = slatedb::DbBuilder::new(source_path.clone(), Arc::clone(&object_store))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        source_db.put(b"meta-key", b"value").await.unwrap();
        source_db.put(b"extent-key", b"value").await.unwrap();
        let source_checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let source = ImmutableCheckpoint {
            database_path: source_path,
            checkpoint_id: source_checkpoint.id,
            manifest_id: source_checkpoint.manifest_id,
        };
        let destination_id = Uuid::new_v4();
        let roots = SlateDbRootStore::new(Arc::clone(&object_store), Path::from("root-store"));
        let root = roots
            .create_from_checkpoint(Uuid::new_v4(), destination_id, &source)
            .await
            .unwrap();
        let (_, manifest_id) = decode_root_checkpoint(&root.manifest_id).unwrap();
        let manifest = roots
            .admin(Path::from(root.identity.clone()))
            .read_manifest(Some(manifest_id))
            .await
            .unwrap()
            .unwrap();
        assert!(!manifest.segments().is_empty());
        let segment_sst = manifest
            .segments()
            .iter()
            .flat_map(|segment| segment.l0())
            .next()
            .expect("flushed segmented source must retain a segment SST")
            .sst
            .id;
        let missing_path =
            PathResolver::new(Path::from(root.identity.clone()), &manifest).sst_path(&segment_sst);
        object_store.delete(&missing_path).await.unwrap();

        assert!(matches!(
            roots.verify(&root).await,
            Err(RootStoreError::MissingSst(_))
        ));
    }

    #[tokio::test]
    async fn refuses_to_claim_an_existing_unowned_destination() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("root-store/unowned-source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        let checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let destination_id = Uuid::new_v4();
        let destination = Path::from(format!("root-store/{destination_id}"));
        Db::open(destination.clone(), Arc::clone(&object_store))
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
        let roots = SlateDbRootStore::new(object_store, Path::from("root-store"));
        assert!(matches!(
            roots
                .create_from_checkpoint(
                    Uuid::new_v4(),
                    destination_id,
                    &ImmutableCheckpoint {
                        database_path: source_path,
                        checkpoint_id: checkpoint.id,
                        manifest_id: checkpoint.manifest_id,
                    },
                )
                .await,
            Err(RootStoreError::UnownedDestination(_))
        ));
    }

    #[tokio::test]
    async fn rejects_a_root_with_separate_wal_dependencies_before_clone_io() {
        let main_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("root-store/wal-source");
        let source_db = slatedb::DbBuilder::new(source_path.clone(), Arc::clone(&main_store))
            .with_wal_object_store(Arc::clone(&wal_store))
            .build()
            .await
            .unwrap();
        source_db.put(b"key", b"wal-value").await.unwrap();
        let checkpoint = source_db
            .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let destination_id = Uuid::new_v4();
        let destination = Path::from(format!("root-store/{destination_id}"));
        let roots = SlateDbRootStore::new(Arc::clone(&main_store), Path::from("root-store"))
            .with_wal_object_store(Arc::clone(&wal_store));
        assert!(matches!(
            roots
                .create_from_checkpoint(
                    Uuid::new_v4(),
                    destination_id,
                    &ImmutableCheckpoint {
                        database_path: source_path,
                        checkpoint_id: checkpoint.id,
                        manifest_id: checkpoint.manifest_id,
                    },
                )
                .await,
            Err(RootStoreError::WalDependency { .. })
        ));
        assert!(matches!(
            main_store.get(&owner_object_path(&destination)).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn reconciles_applied_clone_and_result_writes_after_lost_responses() {
        let (fault_store, faults) = FaultStore::new(Arc::new(InMemory::new()));
        let object_store: Arc<dyn ObjectStore> = fault_store;
        let source_path = Path::from("root-store/fault-source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&object_store))
            .await
            .unwrap();
        source_db.put(b"key", b"value").await.unwrap();
        let checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();

        let operation_id = Uuid::new_v4();
        let destination_id = Uuid::new_v4();
        let destination = Path::from(format!("root-store/{destination_id}"));
        let source = ImmutableCheckpoint {
            database_path: source_path.clone(),
            checkpoint_id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        };
        let owner = RootOwner {
            schema_version: ROOT_DESCRIPTOR_SCHEMA_VERSION,
            operation_id,
            destination_id,
            destination_path: destination.to_string(),
            source_path: source_path.to_string(),
            source_checkpoint_id: checkpoint.id,
            source_manifest_id: checkpoint.manifest_id,
        };
        let roots = SlateDbRootStore::new(Arc::clone(&object_store), Path::from("root-store"));
        roots
            .ensure_owner(&owner, &roots.admin(destination.clone()))
            .await
            .unwrap();

        faults.fail_puts_after_apply(1);
        let root = roots
            .create_from_checkpoint(operation_id, destination_id, &source)
            .await
            .expect("SlateDB must reconcile an applied clone write with a lost response");
        roots.verify(&root).await.unwrap();

        object_store
            .delete(&result_object_path(&destination))
            .await
            .unwrap();
        faults.fail_puts_after_apply(1);
        assert_eq!(
            roots
                .publish_result(RootResult {
                    owner,
                    root: root.clone(),
                })
                .await
                .expect("an applied immutable result must survive a lost response"),
            root
        );
    }

    #[tokio::test]
    async fn rejects_invalid_destination_paths_before_storage_io() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let destination_id = Uuid::new_v4();
        let source = ImmutableCheckpoint {
            database_path: Path::from("root-store/path-source"),
            checkpoint_id: Uuid::new_v4(),
            manifest_id: 1,
        };

        assert!(matches!(
            SlateDbRootStore::new(Arc::clone(&object_store), Path::from(""))
                .create_from_checkpoint(Uuid::new_v4(), destination_id, &source)
                .await,
            Err(RootStoreError::Invalid(_))
        ));
        assert!(matches!(
            SlateDbRootStore::new(
                Arc::clone(&object_store),
                Path::from("root-store/path-source/branches"),
            )
            .create_from_checkpoint(Uuid::new_v4(), destination_id, &source)
            .await,
            Err(RootStoreError::Invalid(_))
        ));
        let nested_source = ImmutableCheckpoint {
            database_path: Path::from(format!("root-store/{destination_id}/source")),
            ..source.clone()
        };
        assert!(matches!(
            SlateDbRootStore::new(Arc::clone(&object_store), Path::from("root-store"))
                .create_from_checkpoint(Uuid::new_v4(), destination_id, &nested_source)
                .await,
            Err(RootStoreError::Invalid(_))
        ));
        assert!(matches!(
            SlateDbRootStore::new(
                Arc::clone(&object_store),
                Path::from("x".repeat(MAX_ROOT_IDENTIFIER_BYTES)),
            )
            .create_from_checkpoint(Uuid::new_v4(), destination_id, &source)
            .await,
            Err(RootStoreError::Invalid(_))
        ));
        assert_eq!(object_store.list(None).collect::<Vec<_>>().await.len(), 0);
    }
}
