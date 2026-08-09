use super::{
    BranchCreateOperation, BranchCreatePhase, BranchDeleteOperation, BranchDeletePhase,
    BranchRecord, BranchState, Catalog, CatalogError, CatalogMutation, CatalogSnapshot,
    ImmutableCheckpoint, LeaseAccessMode, LeaseAcquireRequest, LeaseGrant, LeaseRecord,
    RetiredCatalogKind, RootStoreError, SlateDbRootStore, TombstoneKind, TombstoneRecord,
    catalog_timestamp, validate_name,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchCreateRequest {
    pub operation_id: Uuid,
    pub destination_id: Uuid,
    pub destination_name: String,
    pub source: ImmutableCheckpoint,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchCreateFromCheckpointNameRequest {
    pub operation_id: Uuid,
    pub destination_id: Uuid,
    pub destination_name: String,
    pub source_branch_id: Uuid,
    pub source_checkpoint_name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchMountRequest {
    pub branch_name: String,
    /// The subject UUID identifies the exact branch incarnation; the remaining
    /// fields make retries idempotent without resolving a reused name again.
    pub lease: LeaseAcquireRequest,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BranchMountGrant {
    pub branch_name: String,
    /// The data-plane capability carries the exact UUID and authenticated root.
    pub lease: LeaseGrant,
}

/// Server-owned release controls for customer-visible lifecycle operations.
///
/// Defaults are deliberately off. Read-only list and inspection APIs remain
/// available because they cannot publish roots, leases, or tombstones.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchFeatureConfig {
    #[serde(default)]
    pub create: bool,
    #[serde(default)]
    pub mount: bool,
    #[serde(default)]
    pub checkpoint_delete: bool,
    #[serde(default)]
    pub branch_delete: bool,
}

impl BranchFeatureConfig {
    #[cfg(test)]
    pub(crate) const fn all_enabled() -> Self {
        Self {
            create: true,
            mount: true,
            checkpoint_delete: true,
            branch_delete: true,
        }
    }
}

pub const MAX_ADMINISTRATIVE_INSPECTION_RECORDS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdministrativeInspectionKind {
    Branch,
    Lease,
    Tombstone,
    IncompleteBranchCreate,
    IncompleteBranchDelete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdministrativeInspectionRequest {
    pub kind: AdministrativeInspectionKind,
    pub after: Option<Uuid>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdministrativeLeaseRecord {
    pub id: Uuid,
    pub revision: u64,
    pub subject_kind: super::LeaseSubjectKind,
    pub subject_id: Uuid,
    pub root: super::DurableRoot,
    pub access_mode: super::LeaseAccessMode,
    pub issued_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl From<&LeaseRecord> for AdministrativeLeaseRecord {
    fn from(record: &LeaseRecord) -> Self {
        Self {
            id: record.id,
            revision: record.revision,
            subject_kind: record.subject_kind,
            subject_id: record.subject_id,
            root: record.root.clone(),
            access_mode: record.access_mode,
            issued_at: record.issued_at,
            updated_at: record.updated_at,
            expires_at: record.expires_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "record", rename_all = "snake_case")]
pub enum AdministrativeInspectionRecord {
    Branch(BranchRecord),
    Lease(AdministrativeLeaseRecord),
    Tombstone(TombstoneRecord),
    IncompleteBranchCreate(BranchCreateOperation),
    IncompleteBranchDelete(BranchDeleteOperation),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdministrativeInspectionPage {
    /// Lock-consistent authoritative SlateDB generation for this page.
    pub generation: u64,
    pub records: Vec<AdministrativeInspectionRecord>,
    /// Exact UUID cursor for the next page, when more records remain.
    pub next_after: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalResourceStatus {
    Live,
    Tombstoned,
    Retired,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalResource {
    pub id: Uuid,
    pub status: HistoricalResourceStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchLineageInspection {
    pub parent: Option<HistoricalResource>,
    pub origin_checkpoint: Option<HistoricalResource>,
}

/// Authoritative branch inspection. Durable roots come from SlateDB and are
/// intentionally absent from the PostgreSQL/JSON customer projections.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BranchInspection {
    Live {
        record: BranchRecord,
        lineage: BranchLineageInspection,
    },
    Tombstoned {
        record: TombstoneRecord,
        lineage: BranchLineageInspection,
    },
    Retired {
        id: Uuid,
    },
}

#[derive(Clone)]
pub struct BranchLifecycle {
    catalog: Arc<dyn Catalog>,
    roots: SlateDbRootStore,
    features: BranchFeatureConfig,
}

impl std::fmt::Debug for BranchLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BranchLifecycle")
            .field("roots", &self.roots)
            .finish_non_exhaustive()
    }
}

impl BranchLifecycle {
    /// Close the authoritative catalog after serving has stopped. Callers must
    /// retain elected serving authority until this completes during an orderly
    /// shutdown; a deposed process closes without attempting new mutations.
    pub async fn close(&self) -> Result<(), BranchLifecycleError> {
        self.catalog.close().await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn new(catalog: Arc<dyn Catalog>, roots: SlateDbRootStore) -> Self {
        Self::new_with_features(catalog, roots, BranchFeatureConfig::all_enabled())
    }

    pub(crate) fn new_with_features(
        catalog: Arc<dyn Catalog>,
        roots: SlateDbRootStore,
        features: BranchFeatureConfig,
    ) -> Self {
        Self {
            catalog,
            roots,
            features,
        }
    }

    pub fn leases(&self) -> super::LeaseLifecycle {
        super::LeaseLifecycle::new_with_acquisition_control(
            Arc::clone(&self.catalog),
            self.roots.clone(),
            self.features.mount,
        )
    }

    /// Rebuild one customer projection from a lock-consistent authoritative
    /// SlateDB snapshot. Projection failure never changes lifecycle authority.
    pub async fn reconcile_projection(
        &self,
        volume_id: Uuid,
        projection: &dyn super::CatalogProjection,
    ) -> Result<(), BranchLifecycleError> {
        if volume_id.is_nil() {
            return Err(
                CatalogError::Invalid("projection volume UUID cannot be nil".to_string()).into(),
            );
        }
        let snapshot = self.catalog.snapshot().await?;
        projection.reconcile(volume_id, &snapshot).await?;
        Ok(())
    }

    pub fn deletions(&self) -> super::DeletionLifecycle {
        super::DeletionLifecycle::new_with_features(Arc::clone(&self.catalog), self.features)
    }

    /// Run one policy-bounded metadata cleanup pass. Eligible tombstones become
    /// permanent root-free UUID reservations; uncertainty and live dependencies
    /// retain the full historical record.
    pub async fn cleanup_tombstones(
        &self,
        policy: super::TombstoneCleanupPolicy,
    ) -> Result<super::TombstoneCleanupReport, BranchLifecycleError> {
        Ok(self.catalog.cleanup_tombstones(policy).await?)
    }

    /// Authorize a data-plane mount against one stable branch incarnation.
    ///
    /// Name resolution and lease acquisition serialize in the catalog. An
    /// exact retry remains bound to the original UUID/root even after deletion
    /// and name reuse. The root is verified once more immediately before the
    /// capability crosses this lifecycle boundary.
    pub async fn mount_branch_by_name(
        &self,
        request: BranchMountRequest,
    ) -> Result<BranchMountGrant, BranchLifecycleError> {
        if !self.features.mount {
            return Err(BranchLifecycleError::FeatureDisabled("branch mount"));
        }
        validate_name(&request.branch_name)?;
        let grant = self
            .leases()
            .acquire_branch_by_name(&request.branch_name, request.lease)
            .await?;
        self.roots.verify(&grant.lease.root).await?;
        Ok(BranchMountGrant {
            branch_name: request.branch_name,
            lease: grant,
        })
    }

    /// After the writable database has been fully flushed and closed, publish
    /// its immutable latest head and retire the exact writer capability in one
    /// authoritative catalog transition.
    pub async fn publish_writer_head(
        &self,
        grant: &LeaseGrant,
    ) -> Result<BranchRecord, BranchLifecycleError> {
        if !self.features.mount {
            return Err(BranchLifecycleError::FeatureDisabled("branch mount"));
        }
        if grant.lease.access_mode != LeaseAccessMode::Write {
            return Err(CatalogError::Invalid(
                "head publication requires a writer lease".to_string(),
            )
            .into());
        }
        let root = self
            .roots
            .publish_writer_head(grant.lease.subject_id, grant.lease.id, &grant.lease.root)
            .await?;
        self.roots.verify(&root).await?;
        let token_hash = super::lease::token_hash(grant.renewal_token);
        let tombstone = self.catalog.lease_tombstone(grant.lease.id).await?;
        let published_at = tombstone
            .as_ref()
            .filter(|tombstone| tombstone.token_hash == token_hash)
            .and_then(|tombstone| tombstone.writer_head.as_ref())
            .filter(|publication| {
                publication.branch_id == grant.lease.subject_id
                    && publication.consumed_lease_revision == grant.lease.revision
                    && publication.previous_root == grant.lease.root
                    && publication.root == root
            })
            .map_or_else(
                || catalog_timestamp(Utc::now()),
                |publication| publication.published_at,
            );
        self.catalog
            .apply(CatalogMutation::PublishWriterHead {
                lease_id: grant.lease.id,
                expected_lease_revision: grant.lease.revision,
                token_hash,
                previous_root: grant.lease.root.clone(),
                root: root.clone(),
                published_at,
            })
            .await?;
        self.catalog
            .branch(grant.lease.subject_id)
            .await?
            .filter(|branch| branch.root.as_ref() == Some(&root))
            .ok_or_else(|| CatalogError::OperationConflict(grant.lease.id.to_string()).into())
    }

    /// List every live branch from one lock-consistent authoritative snapshot.
    /// Results are deterministic by name and stable UUID.
    pub async fn list_branches(&self) -> Result<Vec<BranchInspection>, BranchLifecycleError> {
        let snapshot = self.catalog.snapshot().await?;
        let mut branches = snapshot.branches.values().collect::<Vec<_>>();
        branches.sort_by(|left, right| (&left.name, left.id).cmp(&(&right.name, right.id)));
        Ok(branches
            .into_iter()
            .map(|branch| inspect_live_branch(&snapshot, branch))
            .collect())
    }

    /// Inspect one exact branch incarnation by its never-reused UUID. Deleted
    /// incarnations remain distinguishable from a later branch reusing the name.
    pub async fn inspect_branch(&self, id: Uuid) -> Result<BranchInspection, BranchLifecycleError> {
        let snapshot = self.catalog.snapshot().await?;
        inspect_branch_id(&snapshot, id)
            .ok_or_else(|| CatalogError::NotFound(id.to_string()).into())
    }

    /// Resolve a currently live branch name, then inspect that exact UUID in
    /// the same authoritative snapshot. Historical names are never aliases.
    pub async fn inspect_branch_by_name(
        &self,
        name: &str,
    ) -> Result<BranchInspection, BranchLifecycleError> {
        validate_name(name)?;
        let snapshot = self.catalog.snapshot().await?;
        let branch = snapshot
            .branches
            .values()
            .find(|branch| branch.name == name)
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;
        Ok(inspect_live_branch(&snapshot, branch))
    }

    /// Return one bounded page of storage-sensitive administrative state from
    /// the authoritative SlateDB catalog. These roots and lease details are
    /// deliberately not part of the PostgreSQL/JSON customer projection.
    pub async fn inspect_administrative_catalog(
        &self,
        request: AdministrativeInspectionRequest,
    ) -> Result<AdministrativeInspectionPage, BranchLifecycleError> {
        if request.limit == 0 || request.limit > MAX_ADMINISTRATIVE_INSPECTION_RECORDS {
            return Err(CatalogError::Invalid(format!(
                "administrative inspection limit must be within 1..={MAX_ADMINISTRATIVE_INSPECTION_RECORDS}"
            ))
            .into());
        }
        let snapshot = self.catalog.snapshot().await?;
        Ok(administrative_inspection_page(&snapshot, request))
    }

    pub fn root_captures(&self) -> super::RootCaptureLifecycle {
        super::RootCaptureLifecycle::new(Arc::clone(&self.catalog), self.roots.clone())
    }

    /// Bind authenticated segment-pool reservations to this catalog's exact
    /// branch incarnations. The resulting records are local SlateDB authority,
    /// not part of the PostgreSQL/JSON customer projection.
    pub fn private_epochs(
        &self,
        segment_pool: Arc<dyn object_store::ObjectStore>,
        authority: crate::segment_store::SegmentPoolAuthority,
    ) -> super::PrivateEpochLifecycle {
        super::PrivateEpochLifecycle::new(Arc::clone(&self.catalog), segment_pool, authority)
    }

    /// Internal production construction boundary for sealing: bind the
    /// lifecycle to the exact mounted writer whose barriers issue receipts.
    #[allow(dead_code)] // Wired when branch mounting replaces the ownerless server path.
    pub(crate) fn private_epoch_publisher(
        &self,
        segment_pool: Arc<dyn object_store::ObjectStore>,
        authority: crate::segment_store::SegmentPoolAuthority,
        branch_id: Uuid,
        database_identity: String,
        extent_store: &crate::fs::store::ExtentStore,
    ) -> Result<super::PrivateEpochLifecycle, crate::fs::errors::FsError> {
        extent_store.bind_private_owner(branch_id, database_identity)?;
        Ok(self
            .private_epochs(segment_pool, authority)
            .with_publisher(extent_store))
    }

    /// Resolve a checkpoint name once to its stable catalog UUID and exact
    /// SlateDB checkpoint/manifest identity, then use the same creation
    /// primitive as an already-resolved request.
    pub async fn create_from_checkpoint_name(
        &self,
        request: BranchCreateFromCheckpointNameRequest,
    ) -> Result<BranchRecord, BranchLifecycleError> {
        self.require_create_enabled()?;
        if let Some(existing) = self
            .catalog
            .branch_create_operation(request.operation_id)
            .await?
        {
            if existing.destination_id != request.destination_id
                || existing.destination_name != request.destination_name
                || existing.parent_id != Some(request.source_branch_id)
                || existing.created_at != request.created_at
            {
                return Err(
                    CatalogError::OperationConflict(request.operation_id.to_string()).into(),
                );
            }
            let source = ImmutableCheckpoint::from_durable_root(&existing.source_root)?;
            if source.checkpoint_id != existing.source_checkpoint_id {
                return Err(BranchLifecycleError::SourceRootConflict(
                    existing.source_checkpoint_id,
                ));
            }
            return self
                .create_from_checkpoint(BranchCreateRequest {
                    operation_id: request.operation_id,
                    destination_id: request.destination_id,
                    destination_name: request.destination_name,
                    source,
                    created_at: request.created_at,
                })
                .await;
        }
        let source_record = self
            .catalog
            .checkpoint_by_name(request.source_branch_id, &request.source_checkpoint_name)
            .await?
            .ok_or_else(|| {
                CatalogError::NotFound(format!(
                    "checkpoint {}/{}",
                    request.source_branch_id, request.source_checkpoint_name
                ))
            })?;
        let source = ImmutableCheckpoint::from_durable_root(&source_record.root)?;
        if source.checkpoint_id != source_record.id {
            return Err(BranchLifecycleError::SourceRootConflict(source_record.id));
        }
        self.create_from_checkpoint(BranchCreateRequest {
            operation_id: request.operation_id,
            destination_id: request.destination_id,
            destination_name: request.destination_name,
            source,
            created_at: request.created_at,
        })
        .await
    }

    /// Create or resume one exact checkpoint-based branch operation.
    ///
    /// The catalog reserves the source before clone I/O. The resulting root is
    /// authenticated both before it becomes an incomplete GC root and directly
    /// before the atomic `Creating` to `Ready` publication.
    pub async fn create_from_checkpoint(
        &self,
        request: BranchCreateRequest,
    ) -> Result<BranchRecord, BranchLifecycleError> {
        self.require_create_enabled()?;
        let existing = self
            .catalog
            .branch_create_operation(request.operation_id)
            .await?;
        let operation = if let Some(existing) = existing {
            if existing.destination_id != request.destination_id
                || existing.destination_name != request.destination_name
                || existing.source_checkpoint_id != request.source.checkpoint_id
                || existing.source_root != request.source.durable_root()
                || existing.created_at != request.created_at
            {
                return Err(
                    CatalogError::OperationConflict(request.operation_id.to_string()).into(),
                );
            }
            if existing.phase == BranchCreatePhase::Published {
                return self.ready_branch(existing.destination_id).await;
            }
            existing
        } else {
            let source_record = self
                .catalog
                .checkpoint(request.source.checkpoint_id)
                .await?
                .ok_or_else(|| CatalogError::NotFound(request.source.checkpoint_id.to_string()))?;
            if source_record.root != request.source.durable_root() {
                return Err(BranchLifecycleError::SourceRootConflict(source_record.id));
            }
            let branch = BranchRecord {
                id: request.destination_id,
                revision: 1,
                name: request.destination_name.clone(),
                state: BranchState::Creating,
                root: None,
                parent_id: Some(source_record.branch_id),
                origin_checkpoint_id: Some(source_record.id),
                created_at: request.created_at,
                updated_at: request.created_at,
            };
            let reservation = BranchCreateOperation {
                id: request.operation_id,
                revision: 1,
                destination_id: request.destination_id,
                destination_name: request.destination_name.clone(),
                source_checkpoint_id: source_record.id,
                source_root: source_record.root,
                parent_id: Some(source_record.branch_id),
                phase: BranchCreatePhase::Reserved,
                destination_root: None,
                created_at: request.created_at,
                updated_at: request.created_at,
            };
            self.catalog
                .apply(CatalogMutation::ReserveBranchCreate {
                    branch,
                    operation: Box::new(reservation),
                })
                .await?;
            self.operation(request.operation_id).await?
        };
        if operation.phase == BranchCreatePhase::Published {
            return self.ready_branch(operation.destination_id).await;
        }
        let destination_root = match operation.phase {
            BranchCreatePhase::Reserved => {
                self.roots
                    .create_from_checkpoint(
                        request.operation_id,
                        request.destination_id,
                        &request.source,
                    )
                    .await?
            }
            BranchCreatePhase::RootCreated => {
                operation.destination_root.clone().ok_or_else(|| {
                    BranchLifecycleError::Invariant(
                        "root-created operation is missing its destination root".to_string(),
                    )
                })?
            }
            BranchCreatePhase::Published => unreachable!("handled above"),
        };
        self.roots.verify(&destination_root).await?;
        self.catalog
            .apply(CatalogMutation::RecordBranchCreateRoot {
                operation_id: request.operation_id,
                expected_revision: operation.revision,
                destination_root: destination_root.clone(),
                updated_at: transition_time(request.created_at),
            })
            .await?;

        let operation = self.operation(request.operation_id).await?;
        if operation.phase == BranchCreatePhase::Published {
            return self.ready_branch(operation.destination_id).await;
        }
        if operation.phase != BranchCreatePhase::RootCreated
            || operation.destination_root.as_ref() != Some(&destination_root)
        {
            return Err(BranchLifecycleError::Invariant(format!(
                "operation {} did not retain its authenticated destination root",
                operation.id
            )));
        }
        self.roots.verify(&destination_root).await?;
        self.catalog
            .apply(CatalogMutation::PublishBranchCreate {
                operation_id: request.operation_id,
                expected_revision: operation.revision,
                updated_at: transition_time(request.created_at),
            })
            .await?;
        self.ready_branch(request.destination_id).await
    }

    async fn operation(
        &self,
        operation_id: Uuid,
    ) -> Result<BranchCreateOperation, BranchLifecycleError> {
        self.catalog
            .branch_create_operation(operation_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(operation_id.to_string()).into())
    }

    fn require_create_enabled(&self) -> Result<(), BranchLifecycleError> {
        if self.features.create {
            Ok(())
        } else {
            Err(BranchLifecycleError::FeatureDisabled("branch creation"))
        }
    }

    async fn ready_branch(
        &self,
        destination_id: Uuid,
    ) -> Result<BranchRecord, BranchLifecycleError> {
        let branch = self
            .catalog
            .branch(destination_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(destination_id.to_string()))?;
        if branch.state != BranchState::Ready || branch.root.is_none() {
            return Err(BranchLifecycleError::Invariant(format!(
                "published operation destination {destination_id} is not ready"
            )));
        }
        Ok(branch)
    }
}

fn inspect_branch_id(snapshot: &CatalogSnapshot, id: Uuid) -> Option<BranchInspection> {
    if let Some(branch) = snapshot.branches.get(&id) {
        return Some(inspect_live_branch(snapshot, branch));
    }
    if let Some(tombstone) = snapshot
        .tombstones
        .get(&id)
        .filter(|record| record.kind == TombstoneKind::Branch)
    {
        return Some(BranchInspection::Tombstoned {
            lineage: inspect_lineage(
                snapshot,
                tombstone.parent_id,
                tombstone.origin_checkpoint_id,
            ),
            record: tombstone.clone(),
        });
    }
    snapshot
        .retired_catalog_ids
        .get(&id)
        .filter(|record| record.kind == RetiredCatalogKind::Branch)
        .map(|_| BranchInspection::Retired { id })
}

fn inspect_live_branch(snapshot: &CatalogSnapshot, branch: &BranchRecord) -> BranchInspection {
    BranchInspection::Live {
        lineage: inspect_lineage(snapshot, branch.parent_id, branch.origin_checkpoint_id),
        record: branch.clone(),
    }
}

fn inspect_lineage(
    snapshot: &CatalogSnapshot,
    parent_id: Option<Uuid>,
    origin_checkpoint_id: Option<Uuid>,
) -> BranchLineageInspection {
    BranchLineageInspection {
        parent: parent_id.map(|id| HistoricalResource {
            id,
            status: historical_status(
                snapshot,
                id,
                TombstoneKind::Branch,
                RetiredCatalogKind::Branch,
            ),
        }),
        origin_checkpoint: origin_checkpoint_id.map(|id| HistoricalResource {
            id,
            status: historical_status(
                snapshot,
                id,
                TombstoneKind::Checkpoint,
                RetiredCatalogKind::Checkpoint,
            ),
        }),
    }
}

fn historical_status(
    snapshot: &CatalogSnapshot,
    id: Uuid,
    tombstone_kind: TombstoneKind,
    retired_kind: RetiredCatalogKind,
) -> HistoricalResourceStatus {
    let live = match tombstone_kind {
        TombstoneKind::Branch => snapshot.branches.contains_key(&id),
        TombstoneKind::Checkpoint => snapshot.checkpoints.contains_key(&id),
    };
    if live {
        HistoricalResourceStatus::Live
    } else if snapshot
        .tombstones
        .get(&id)
        .is_some_and(|record| record.kind == tombstone_kind)
    {
        HistoricalResourceStatus::Tombstoned
    } else if snapshot
        .retired_catalog_ids
        .get(&id)
        .is_some_and(|record| record.kind == retired_kind)
    {
        HistoricalResourceStatus::Retired
    } else {
        HistoricalResourceStatus::Missing
    }
}

fn administrative_inspection_page(
    snapshot: &CatalogSnapshot,
    request: AdministrativeInspectionRequest,
) -> AdministrativeInspectionPage {
    let after = request.after;
    let mut records: Vec<AdministrativeInspectionRecord> = match request.kind {
        AdministrativeInspectionKind::Branch => snapshot
            .branches
            .values()
            .filter(|record| after.is_none_or(|after| record.id > after))
            .take(request.limit + 1)
            .cloned()
            .map(AdministrativeInspectionRecord::Branch)
            .collect(),
        AdministrativeInspectionKind::Lease => snapshot
            .leases
            .values()
            .filter(|record| after.is_none_or(|after| record.id > after))
            .take(request.limit + 1)
            .map(AdministrativeLeaseRecord::from)
            .map(AdministrativeInspectionRecord::Lease)
            .collect(),
        AdministrativeInspectionKind::Tombstone => snapshot
            .tombstones
            .values()
            .filter(|record| after.is_none_or(|after| record.id > after))
            .take(request.limit + 1)
            .cloned()
            .map(AdministrativeInspectionRecord::Tombstone)
            .collect(),
        AdministrativeInspectionKind::IncompleteBranchCreate => snapshot
            .branch_create_operations
            .values()
            .filter(|record| {
                record.phase != BranchCreatePhase::Published
                    && after.is_none_or(|after| record.id > after)
            })
            .take(request.limit + 1)
            .cloned()
            .map(AdministrativeInspectionRecord::IncompleteBranchCreate)
            .collect(),
        AdministrativeInspectionKind::IncompleteBranchDelete => snapshot
            .branch_delete_operations
            .values()
            .filter(|record| {
                record.phase != BranchDeletePhase::Published
                    && after.is_none_or(|after| record.id > after)
            })
            .take(request.limit + 1)
            .cloned()
            .map(AdministrativeInspectionRecord::IncompleteBranchDelete)
            .collect(),
    };
    let next_after = (records.len() > request.limit)
        .then(|| administrative_record_id(&records[request.limit - 1]));
    records.truncate(request.limit);
    AdministrativeInspectionPage {
        generation: snapshot.generation,
        records,
        next_after,
    }
}

fn administrative_record_id(record: &AdministrativeInspectionRecord) -> Uuid {
    match record {
        AdministrativeInspectionRecord::Branch(record) => record.id,
        AdministrativeInspectionRecord::Lease(record) => record.id,
        AdministrativeInspectionRecord::Tombstone(record) => record.id,
        AdministrativeInspectionRecord::IncompleteBranchCreate(record) => record.id,
        AdministrativeInspectionRecord::IncompleteBranchDelete(record) => record.id,
    }
}

fn transition_time(created_at: DateTime<Utc>) -> DateTime<Utc> {
    std::cmp::max(created_at, catalog_timestamp(Utc::now()))
}

#[derive(Debug, thiserror::Error)]
pub enum BranchLifecycleError {
    #[error("{0} is disabled by server feature control")]
    FeatureDisabled(&'static str),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    RootStore(#[from] RootStoreError),
    #[error(transparent)]
    Lease(#[from] super::LeaseLifecycleError),
    #[error("source checkpoint {0} root does not match its exact SlateDB checkpoint identity")]
    SourceRootConflict(Uuid),
    #[error("branch lifecycle invariant failed: {0}")]
    Invariant(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        BranchDeleteRequest, BranchDeleteResult, CatalogMutation, CheckpointDeleteRequest,
        CheckpointRecord, DurableRoot, JsonCatalogProjection, LeaseAccessMode, LeaseAcquireRequest,
        SlateDbCatalog, catalog_timestamp,
    };
    use crate::fs::key_codec::KeyCodec;
    use slatedb::Db;
    use slatedb::admin::AdminBuilder;
    use slatedb::config::{CheckpointOptions, CheckpointScope};
    use slatedb::object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;

    #[tokio::test]
    async fn named_concurrent_creates_publish_once_and_ignore_source_name_reuse_on_retry() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("lifecycle/catalog"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let now = catalog_timestamp(Utc::now());
        let parent = BranchRecord {
            id: Uuid::new_v4(),
            revision: 1,
            name: "parent".to_string(),
            state: BranchState::Ready,
            root: Some(DurableRoot {
                identity: "lifecycle/parent-root".to_string(),
                manifest_id: "parent-manifest".to_string(),
            }),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::CreateBranch(parent.clone()))
            .await
            .unwrap();

        let source_path = Path::from("lifecycle/source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        source_db.put(b"key", b"value").await.unwrap();
        let source_checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        let resumable_checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let source = ImmutableCheckpoint {
            database_path: source_path.clone(),
            checkpoint_id: source_checkpoint.id,
            manifest_id: source_checkpoint.manifest_id,
        };
        let source_record = CheckpointRecord {
            id: source_checkpoint.id,
            revision: 1,
            branch_id: parent.id,
            name: "source".to_string(),
            root: source.durable_root(),
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::CreateCheckpoint(source_record.clone()))
            .await
            .unwrap();
        let resumable_source = ImmutableCheckpoint {
            database_path: source_path.clone(),
            checkpoint_id: resumable_checkpoint.id,
            manifest_id: resumable_checkpoint.manifest_id,
        };
        let resumable_record = CheckpointRecord {
            id: resumable_checkpoint.id,
            revision: 1,
            branch_id: parent.id,
            name: "resumable".to_string(),
            root: resumable_source.durable_root(),
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::CreateCheckpoint(resumable_record.clone()))
            .await
            .unwrap();

        let root_store =
            SlateDbRootStore::new(Arc::clone(&store), Path::from("lifecycle/branches"));
        let lifecycle = BranchLifecycle::new(catalog.clone(), root_store.clone());
        let operation_id = Uuid::new_v4();
        let destination_id = Uuid::new_v4();
        let named_request = BranchCreateFromCheckpointNameRequest {
            operation_id,
            destination_id,
            destination_name: "child".to_string(),
            source_branch_id: parent.id,
            source_checkpoint_name: source_record.name.clone(),
            created_at: now,
        };
        let (left, right) = tokio::join!(
            lifecycle.create_from_checkpoint_name(named_request.clone()),
            lifecycle.create_from_checkpoint_name(named_request.clone())
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(left, right);
        assert_eq!(left.state, BranchState::Ready);
        root_store
            .verify(left.root.as_ref().unwrap())
            .await
            .unwrap();
        let snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(snapshot.generation, 6);
        assert_eq!(
            snapshot.branch_create_operations[&operation_id].phase,
            BranchCreatePhase::Published
        );

        let checkpoint_lease_request = LeaseAcquireRequest {
            lease_id: Uuid::new_v4(),
            renewal_token: Uuid::new_v4(),
            subject_id: source_record.id,
            access_mode: LeaseAccessMode::Read,
            duration: chrono::Duration::minutes(2),
        };
        let checkpoint_grant = lifecycle
            .leases()
            .acquire_checkpoint_by_name(
                parent.id,
                &source_record.name,
                checkpoint_lease_request.clone(),
            )
            .await
            .unwrap();
        let mut invalid_writer = checkpoint_lease_request.clone();
        invalid_writer.lease_id = Uuid::new_v4();
        invalid_writer.renewal_token = Uuid::new_v4();
        invalid_writer.access_mode = LeaseAccessMode::Write;
        assert!(matches!(
            lifecycle
                .leases()
                .acquire_checkpoint_by_name(parent.id, &source_record.name, invalid_writer)
                .await,
            Err(crate::catalog::LeaseLifecycleError::Catalog(
                CatalogError::Invalid(_)
            ))
        ));

        let delete_request = CheckpointDeleteRequest {
            checkpoint_id: source_record.id,
            expected_revision: source_record.revision,
            name: source_record.name.clone(),
        };
        lifecycle
            .deletions()
            .delete_checkpoint(delete_request.clone())
            .await
            .unwrap();
        assert_eq!(
            lifecycle
                .leases()
                .acquire_checkpoint_by_name(
                    parent.id,
                    &source_record.name,
                    checkpoint_lease_request,
                )
                .await
                .expect("an acquired checkpoint lease must survive logical deletion"),
            checkpoint_grant
        );
        lifecycle
            .leases()
            .release(
                checkpoint_grant.lease.id,
                checkpoint_grant.lease.revision,
                checkpoint_grant.renewal_token,
            )
            .await
            .unwrap();
        AdminBuilder::new(source_path, Arc::clone(&store))
            .build()
            .delete_checkpoint(source_checkpoint.id)
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::CreateCheckpoint(CheckpointRecord {
                id: Uuid::new_v4(),
                revision: 1,
                branch_id: parent.id,
                name: source_record.name,
                root: DurableRoot {
                    identity: "lifecycle/replacement-source".to_string(),
                    manifest_id: format!("{}@1", Uuid::new_v4()),
                },
                created_at: now,
                updated_at: now,
            }))
            .await
            .unwrap();
        lifecycle
            .deletions()
            .delete_checkpoint(delete_request)
            .await
            .expect("an exact checkpoint deletion retry must ignore name reuse");
        assert_eq!(
            lifecycle
                .create_from_checkpoint_name(named_request)
                .await
                .expect("a published named retry must ignore source deletion and name reuse"),
            left
        );
        let resume_request = BranchCreateRequest {
            operation_id: Uuid::new_v4(),
            destination_id: Uuid::new_v4(),
            destination_name: "resumed-child".to_string(),
            source: resumable_source,
            created_at: now,
        };
        catalog
            .apply(CatalogMutation::ReserveBranchCreate {
                branch: BranchRecord {
                    id: resume_request.destination_id,
                    revision: 1,
                    name: resume_request.destination_name.clone(),
                    state: BranchState::Creating,
                    root: None,
                    parent_id: Some(parent.id),
                    origin_checkpoint_id: Some(resumable_record.id),
                    created_at: now,
                    updated_at: now,
                },
                operation: Box::new(BranchCreateOperation {
                    id: resume_request.operation_id,
                    revision: 1,
                    destination_id: resume_request.destination_id,
                    destination_name: resume_request.destination_name.clone(),
                    source_checkpoint_id: resumable_record.id,
                    source_root: resumable_record.root.clone(),
                    parent_id: Some(parent.id),
                    phase: BranchCreatePhase::Reserved,
                    destination_root: None,
                    created_at: now,
                    updated_at: now,
                }),
            })
            .await
            .unwrap();
        let resumed_root = root_store
            .create_from_checkpoint(
                resume_request.operation_id,
                resume_request.destination_id,
                &resume_request.source,
            )
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::RecordBranchCreateRoot {
                operation_id: resume_request.operation_id,
                expected_revision: 1,
                destination_root: resumed_root,
                updated_at: catalog_timestamp(Utc::now()),
            })
            .await
            .unwrap();
        lifecycle
            .deletions()
            .delete_checkpoint(CheckpointDeleteRequest {
                checkpoint_id: resumable_record.id,
                expected_revision: resumable_record.revision,
                name: resumable_record.name,
            })
            .await
            .unwrap();
        let resumed = lifecycle
            .create_from_checkpoint(resume_request)
            .await
            .expect("root-created operation must resume after source deletion");
        assert_eq!(resumed.state, BranchState::Ready);
        root_store
            .verify(resumed.root.as_ref().unwrap())
            .await
            .unwrap();
        let leases = lifecycle.leases();
        let lease_request = LeaseAcquireRequest {
            lease_id: Uuid::new_v4(),
            renewal_token: Uuid::new_v4(),
            subject_id: left.id,
            access_mode: LeaseAccessMode::Write,
            duration: chrono::Duration::minutes(2),
        };
        let (first_grant, retry_grant) = tokio::join!(
            leases.acquire_branch_by_name(&left.name, lease_request.clone()),
            leases.acquire_branch_by_name(&left.name, lease_request.clone())
        );
        let first_grant = first_grant.unwrap();
        assert_eq!(first_grant, retry_grant.unwrap());
        let renewal = crate::catalog::LeaseRenewRequest {
            lease_id: first_grant.lease.id,
            expected_revision: first_grant.lease.revision,
            renewal_token: first_grant.renewal_token,
            duration: chrono::Duration::minutes(2),
        };
        let renewed = leases.renew(renewal.clone()).await.unwrap();
        assert_eq!(
            leases
                .renew(renewal)
                .await
                .expect("an ambiguous renewal retry must reconcile"),
            renewed
        );
        assert_eq!(
            leases
                .recover_writer(lease_request.clone())
                .await
                .expect("recovery must accept the exact renewed writer revision"),
            LeaseGrant {
                lease: renewed.clone(),
                renewal_token: first_grant.renewal_token,
            }
        );
        assert!(matches!(
            leases
                .release(renewed.id, renewed.revision, first_grant.renewal_token)
                .await,
            Err(crate::catalog::LeaseLifecycleError::Catalog(
                CatalogError::WriterLeaseActive(id)
            )) if id == left.id
        ));
        let writer_db = Db::builder(
            Path::from(left.root.as_ref().unwrap().identity.clone()),
            Arc::clone(&store),
        )
        .build()
        .await
        .unwrap();
        writer_db.put(b"writer", b"head").await.unwrap();
        writer_db.flush().await.unwrap();
        writer_db.close().await.unwrap();
        lifecycle
            .publish_writer_head(&LeaseGrant {
                lease: renewed.clone(),
                renewal_token: first_grant.renewal_token,
            })
            .await
            .unwrap();
        leases
            .release(renewed.id, renewed.revision, first_grant.renewal_token)
            .await
            .expect("release after exact head publication must reconcile");
        let retained_request = LeaseAcquireRequest {
            lease_id: Uuid::new_v4(),
            renewal_token: Uuid::new_v4(),
            subject_id: left.id,
            access_mode: LeaseAccessMode::Read,
            duration: chrono::Duration::minutes(2),
        };
        let retained = leases
            .acquire_branch_by_name(&left.name, retained_request.clone())
            .await
            .unwrap();
        let writer_request = LeaseAcquireRequest {
            lease_id: Uuid::new_v4(),
            renewal_token: Uuid::new_v4(),
            subject_id: left.id,
            access_mode: LeaseAccessMode::Write,
            duration: chrono::Duration::minutes(2),
        };
        let writer = leases
            .acquire_branch_by_name(&left.name, writer_request)
            .await
            .unwrap();
        let writer_renewal = crate::catalog::LeaseRenewRequest {
            lease_id: writer.lease.id,
            expected_revision: writer.lease.revision,
            renewal_token: writer.renewal_token,
            duration: chrono::Duration::minutes(2),
        };
        let current = catalog.branch(left.id).await.unwrap().unwrap();
        assert!(matches!(
            BranchLifecycle::new(catalog.clone(), root_store.clone())
            .deletions()
            .delete_branch(crate::catalog::BranchDeleteRequest {
                operation_id: Uuid::new_v4(),
                branch_id: left.id,
                expected_revision: current.revision,
                name: left.name.clone(),
            })
            .await,
            Err(crate::catalog::DeletionLifecycleError::Catalog(
                CatalogError::WriterLeaseActive(id)
            )) if id == left.id
        ));
        assert_eq!(
            catalog.branch_by_name(&left.name).await.unwrap(),
            Some(current)
        );
        let renewed_writer = leases.renew(writer_renewal.clone()).await.unwrap();
        let next_writer_db = Db::builder(
            Path::from(renewed_writer.root.identity.clone()),
            Arc::clone(&store),
        )
        .build()
        .await
        .unwrap();
        next_writer_db.put(b"writer", b"next-head").await.unwrap();
        next_writer_db.flush().await.unwrap();
        next_writer_db.close().await.unwrap();
        lifecycle
            .publish_writer_head(&LeaseGrant {
                lease: renewed_writer.clone(),
                renewal_token: writer.renewal_token,
            })
            .await
            .unwrap();
        let current = catalog.branch(left.id).await.unwrap().unwrap();
        assert!(matches!(
            BranchLifecycle::new(catalog.clone(), root_store.clone())
                .deletions()
                .delete_branch(crate::catalog::BranchDeleteRequest {
                    operation_id: Uuid::new_v4(),
                    branch_id: left.id,
                    expected_revision: current.revision,
                    name: left.name.clone(),
                })
                .await
                .unwrap(),
            crate::catalog::BranchDeleteResult::Deleted(_)
        ));
        catalog
            .apply(CatalogMutation::CreateBranch(BranchRecord {
                id: Uuid::new_v4(),
                revision: 1,
                name: left.name.clone(),
                state: BranchState::Ready,
                root: Some(DurableRoot {
                    identity: "lifecycle/reused-name".to_string(),
                    manifest_id: "replacement@1".to_string(),
                }),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at: now,
                updated_at: now,
            }))
            .await
            .unwrap();
        assert_eq!(
            leases
                .acquire_branch_by_name(&left.name, retained_request)
                .await
                .expect("the pre-deletion exact lease retry must retain the old root"),
            retained
        );
        assert!(matches!(
            leases.renew(writer_renewal).await,
            Err(crate::catalog::LeaseLifecycleError::Catalog(
                CatalogError::NotFound(_)
            ))
        ));
        leases
            .release(
                retained.lease.id,
                retained.lease.revision,
                retained.renewal_token,
            )
            .await
            .unwrap();
        leases
            .release(
                writer.lease.id,
                renewed_writer.revision,
                writer.renewal_token,
            )
            .await
            .unwrap();
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn create_recovers_from_every_persisted_linearization_boundary() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // 0: before reservation; 1: reserved; 2: clone/result durably written;
        // 3: destination root recorded; 4: ready publication committed.
        for persisted_boundary in 0..=4 {
            let case_root = format!("lifecycle-crash/{persisted_boundary}");
            let catalog_path = Path::from(format!("{case_root}/catalog"));
            let branch_root = Path::from(format!("{case_root}/branches"));
            let catalog = Arc::new(
                SlateDbCatalog::open(catalog_path.clone(), Arc::clone(&store))
                    .await
                    .unwrap(),
            );
            let now = catalog_timestamp(Utc::now());
            let source_path = Path::from(format!("{case_root}/source"));
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
            let parent = BranchRecord {
                id: Uuid::new_v4(),
                revision: 1,
                name: "parent".to_string(),
                state: BranchState::Ready,
                root: Some(source.durable_root()),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at: now,
                updated_at: now,
            };
            catalog
                .apply(CatalogMutation::CreateBranch(parent.clone()))
                .await
                .unwrap();
            catalog
                .apply(CatalogMutation::CreateCheckpoint(CheckpointRecord {
                    id: source.checkpoint_id,
                    revision: 1,
                    branch_id: parent.id,
                    name: "source".to_string(),
                    root: source.durable_root(),
                    created_at: now,
                    updated_at: now,
                }))
                .await
                .unwrap();
            let roots = SlateDbRootStore::new(Arc::clone(&store), branch_root.clone());
            let request = BranchCreateRequest {
                operation_id: Uuid::new_v4(),
                destination_id: Uuid::new_v4(),
                destination_name: format!("child-{persisted_boundary}"),
                source: source.clone(),
                created_at: now,
            };
            if persisted_boundary >= 1 {
                catalog
                    .apply(CatalogMutation::ReserveBranchCreate {
                        branch: BranchRecord {
                            id: request.destination_id,
                            revision: 1,
                            name: request.destination_name.clone(),
                            state: BranchState::Creating,
                            root: None,
                            parent_id: Some(parent.id),
                            origin_checkpoint_id: Some(source.checkpoint_id),
                            created_at: now,
                            updated_at: now,
                        },
                        operation: Box::new(BranchCreateOperation {
                            id: request.operation_id,
                            revision: 1,
                            destination_id: request.destination_id,
                            destination_name: request.destination_name.clone(),
                            source_checkpoint_id: source.checkpoint_id,
                            source_root: source.durable_root(),
                            parent_id: Some(parent.id),
                            phase: BranchCreatePhase::Reserved,
                            destination_root: None,
                            created_at: now,
                            updated_at: now,
                        }),
                    })
                    .await
                    .unwrap();
            }
            let destination_root = if persisted_boundary >= 2 {
                Some(
                    roots
                        .create_from_checkpoint(
                            request.operation_id,
                            request.destination_id,
                            &source,
                        )
                        .await
                        .unwrap(),
                )
            } else {
                None
            };
            if persisted_boundary >= 3 {
                catalog
                    .apply(CatalogMutation::RecordBranchCreateRoot {
                        operation_id: request.operation_id,
                        expected_revision: 1,
                        destination_root: destination_root.clone().unwrap(),
                        updated_at: now,
                    })
                    .await
                    .unwrap();
            }
            if persisted_boundary >= 4 {
                catalog
                    .apply(CatalogMutation::PublishBranchCreate {
                        operation_id: request.operation_id,
                        expected_revision: 2,
                        updated_at: now,
                    })
                    .await
                    .unwrap();
            }

            catalog.close().await.unwrap();
            drop(catalog);
            drop(roots);

            let restarted_catalog = Arc::new(
                SlateDbCatalog::open(catalog_path, Arc::clone(&store))
                    .await
                    .unwrap(),
            );
            let restarted_roots = SlateDbRootStore::new(Arc::clone(&store), branch_root);
            let restarted =
                BranchLifecycle::new(restarted_catalog.clone(), restarted_roots.clone());
            let ready = restarted
                .create_from_checkpoint(request.clone())
                .await
                .unwrap();
            assert_eq!(ready.id, request.destination_id);
            assert_eq!(ready.state, BranchState::Ready);
            restarted_roots
                .verify(ready.root.as_ref().unwrap())
                .await
                .unwrap();
            assert_eq!(
                restarted.create_from_checkpoint(request).await.unwrap(),
                ready
            );
            restarted_catalog.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn deep_lineage_descendant_remains_readable_and_writable_without_live_ancestors() {
        const LINEAGE_DEPTH: usize = 32;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("deep-lineage/catalog"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let now = catalog_timestamp(Utc::now());
        let source_path = Path::from("deep-lineage/source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        source_db.put(b"deep", b"value").await.unwrap();
        let first_checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let mut source = ImmutableCheckpoint {
            database_path: source_path.clone(),
            checkpoint_id: first_checkpoint.id,
            manifest_id: first_checkpoint.manifest_id,
        };
        let mut branch = BranchRecord {
            id: Uuid::new_v4(),
            revision: 1,
            name: "lineage-0".to_string(),
            state: BranchState::Ready,
            root: Some(source.durable_root()),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::CreateBranch(branch.clone()))
            .await
            .unwrap();
        let roots = SlateDbRootStore::new(Arc::clone(&store), Path::from("deep-lineage/branches"));
        let lifecycle = BranchLifecycle::new(catalog.clone(), roots.clone());

        for depth in 0..LINEAGE_DEPTH {
            let checkpoint = CheckpointRecord {
                id: source.checkpoint_id,
                revision: 1,
                branch_id: branch.id,
                name: format!("source-{depth}"),
                root: source.durable_root(),
                created_at: now,
                updated_at: now,
            };
            catalog
                .apply(CatalogMutation::CreateCheckpoint(checkpoint.clone()))
                .await
                .unwrap();
            let child = lifecycle
                .create_from_checkpoint(BranchCreateRequest {
                    operation_id: Uuid::new_v4(),
                    destination_id: Uuid::new_v4(),
                    destination_name: format!("lineage-{}", depth + 1),
                    source: source.clone(),
                    created_at: now,
                })
                .await
                .unwrap();
            lifecycle
                .deletions()
                .delete_checkpoint(CheckpointDeleteRequest {
                    checkpoint_id: checkpoint.id,
                    expected_revision: checkpoint.revision,
                    name: checkpoint.name,
                })
                .await
                .unwrap();
            assert!(matches!(
                lifecycle
                    .deletions()
                    .delete_branch(BranchDeleteRequest {
                        operation_id: Uuid::new_v4(),
                        branch_id: branch.id,
                        expected_revision: branch.revision,
                        name: branch.name,
                    })
                    .await
                    .unwrap(),
                BranchDeleteResult::Deleted(_)
            ));
            if depth == 0 {
                AdminBuilder::new(source_path.clone(), Arc::clone(&store))
                    .build()
                    .delete_checkpoint(first_checkpoint.id)
                    .await
                    .unwrap();
            }
            source = ImmutableCheckpoint::from_durable_root(child.root.as_ref().unwrap()).unwrap();
            branch = child;
        }

        let snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(snapshot.branches.len(), 1);
        assert_eq!(snapshot.branches.get(&branch.id), Some(&branch));
        assert_eq!(snapshot.tombstones.len(), LINEAGE_DEPTH * 2);
        roots.verify(branch.root.as_ref().unwrap()).await.unwrap();

        let descendant_path = Path::from(branch.root.as_ref().unwrap().identity.clone());
        let descendant = Db::open(descendant_path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        assert_eq!(
            descendant.get(b"deep").await.unwrap(),
            Some(bytes::Bytes::from_static(b"value"))
        );
        descendant.put(b"descendant", b"independent").await.unwrap();
        descendant.close().await.unwrap();
        let reopened = Db::open(descendant_path, Arc::clone(&store)).await.unwrap();
        assert_eq!(
            reopened.get(b"descendant").await.unwrap(),
            Some(bytes::Bytes::from_static(b"independent"))
        );
        reopened.close().await.unwrap();
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn branch_listing_is_deterministic_and_inspection_reports_authoritative_lineage() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("branch-inspection/catalog"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let now = catalog_timestamp(Utc::now());
        let parent_id = Uuid::new_v4();
        let checkpoint_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        let parent = BranchRecord {
            id: parent_id,
            revision: 1,
            name: "z-parent".to_string(),
            state: BranchState::Ready,
            root: Some(DurableRoot {
                identity: "branch-inspection/parent".to_string(),
                manifest_id: format!("{}@1", Uuid::new_v4()),
            }),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        };
        let checkpoint = CheckpointRecord {
            id: checkpoint_id,
            revision: 1,
            branch_id: parent_id,
            name: "source".to_string(),
            root: parent.root.clone().unwrap(),
            created_at: now,
            updated_at: now,
        };
        let child = BranchRecord {
            id: child_id,
            revision: 1,
            name: "a-child".to_string(),
            state: BranchState::Ready,
            root: Some(DurableRoot {
                identity: "branch-inspection/child".to_string(),
                manifest_id: format!("{}@1", Uuid::new_v4()),
            }),
            parent_id: Some(parent_id),
            origin_checkpoint_id: Some(checkpoint_id),
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::CreateBranch(parent))
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::CreateCheckpoint(checkpoint))
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::CreateBranch(child.clone()))
            .await
            .unwrap();
        let lifecycle = BranchLifecycle::new(
            catalog.clone(),
            SlateDbRootStore::new(store, Path::from("branch-inspection/branches")),
        );

        let first_admin_page = lifecycle
            .inspect_administrative_catalog(AdministrativeInspectionRequest {
                kind: AdministrativeInspectionKind::Branch,
                after: None,
                limit: 1,
            })
            .await
            .unwrap();
        assert_eq!(first_admin_page.records.len(), 1);
        let cursor = first_admin_page
            .next_after
            .expect("a bounded first page must report the remaining branch");
        let second_admin_page = lifecycle
            .inspect_administrative_catalog(AdministrativeInspectionRequest {
                kind: AdministrativeInspectionKind::Branch,
                after: Some(cursor),
                limit: 1,
            })
            .await
            .unwrap();
        assert_eq!(second_admin_page.records.len(), 1);
        assert_eq!(second_admin_page.next_after, None);
        assert!(matches!(
            lifecycle
                .inspect_administrative_catalog(AdministrativeInspectionRequest {
                    kind: AdministrativeInspectionKind::Branch,
                    after: None,
                    limit: 0,
                })
                .await,
            Err(BranchLifecycleError::Catalog(CatalogError::Invalid(_)))
        ));

        let listed = lifecycle.list_branches().await.unwrap();
        assert_eq!(listed.len(), 2);
        assert!(matches!(
            &listed[0],
            BranchInspection::Live { record, .. } if record.id == child_id
        ));
        let expected = BranchInspection::Live {
            record: child,
            lineage: BranchLineageInspection {
                parent: Some(HistoricalResource {
                    id: parent_id,
                    status: HistoricalResourceStatus::Live,
                }),
                origin_checkpoint: Some(HistoricalResource {
                    id: checkpoint_id,
                    status: HistoricalResourceStatus::Live,
                }),
            },
        };
        assert_eq!(lifecycle.inspect_branch(child_id).await.unwrap(), expected);
        assert_eq!(
            lifecycle.inspect_branch_by_name("a-child").await.unwrap(),
            expected
        );

        assert!(matches!(
            lifecycle
                .deletions()
                .delete_branch(BranchDeleteRequest {
                    operation_id: Uuid::new_v4(),
                    branch_id: child_id,
                    expected_revision: 1,
                    name: "a-child".to_string(),
                })
                .await
                .unwrap(),
            BranchDeleteResult::Deleted(_)
        ));
        let replacement_id = Uuid::new_v4();
        catalog
            .apply(CatalogMutation::CreateBranch(BranchRecord {
                id: replacement_id,
                revision: 1,
                name: "a-child".to_string(),
                state: BranchState::Ready,
                root: Some(DurableRoot {
                    identity: "branch-inspection/replacement".to_string(),
                    manifest_id: format!("{}@1", Uuid::new_v4()),
                }),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at: now,
                updated_at: now,
            }))
            .await
            .unwrap();
        assert!(matches!(
            lifecycle.inspect_branch(child_id).await.unwrap(),
            BranchInspection::Tombstoned { record, .. } if record.id == child_id
        ));
        assert!(matches!(
            lifecycle.inspect_branch_by_name("a-child").await.unwrap(),
            BranchInspection::Live { record, .. } if record.id == replacement_id
        ));
        let tombstones = lifecycle
            .inspect_administrative_catalog(AdministrativeInspectionRequest {
                kind: AdministrativeInspectionKind::Tombstone,
                after: None,
                limit: 1,
            })
            .await
            .unwrap();
        assert!(matches!(
            tombstones.records.as_slice(),
            [AdministrativeInspectionRecord::Tombstone(record)] if record.id == child_id
        ));
        let cleanup = lifecycle
            .cleanup_tombstones(crate::catalog::TombstoneCleanupPolicy {
                retain_after: now + chrono::Duration::minutes(1),
                scan_limit: 1,
                compact_limit: 1,
            })
            .await
            .unwrap();
        assert_eq!(cleanup.examined, 1);
        assert_eq!(cleanup.compacted, 1);
        assert_eq!(
            lifecycle.inspect_branch(child_id).await.unwrap(),
            BranchInspection::Retired { id: child_id }
        );
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn mount_grant_stays_on_exact_uuid_and_root_across_name_reuse() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog_path = Path::from("branch-mount/catalog");
        let source_path = Path::from("branch-mount/source");
        let source_db = Db::builder(source_path.clone(), Arc::clone(&store))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        let mount_key = KeyCodec::new().inode_key(71);
        source_db.put(&mount_key, b"value").await.unwrap();
        source_db.flush().await.unwrap();
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
        let roots = SlateDbRootStore::new(Arc::clone(&store), Path::from("branch-mount/branches"));
        let branch_id = Uuid::new_v4();
        let root = roots
            .create_from_checkpoint(Uuid::new_v4(), branch_id, &source)
            .await
            .unwrap();
        let catalog = Arc::new(
            SlateDbCatalog::open(catalog_path.clone(), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let now = catalog_timestamp(Utc::now());
        let branch = BranchRecord {
            id: branch_id,
            revision: 1,
            name: "mounted".to_string(),
            state: BranchState::Ready,
            root: Some(root.clone()),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::CreateBranch(branch.clone()))
            .await
            .unwrap();
        let lifecycle = BranchLifecycle::new(catalog.clone(), roots.clone());
        let request = BranchMountRequest {
            branch_name: branch.name.clone(),
            lease: LeaseAcquireRequest {
                lease_id: Uuid::new_v4(),
                renewal_token: Uuid::new_v4(),
                subject_id: branch_id,
                access_mode: LeaseAccessMode::Write,
                duration: chrono::Duration::minutes(2),
            },
        };
        let original = lifecycle
            .mount_branch_by_name(request.clone())
            .await
            .unwrap();
        assert_eq!(original.lease.lease.subject_id, branch_id);
        assert_eq!(original.lease.lease.root, root);

        // A process crash loses the in-memory lifecycle but not the grant. The
        // replacement process recovers the same exact capability from SlateDB.
        drop(lifecycle);
        catalog.close().await.unwrap();
        let catalog = Arc::new(
            SlateDbCatalog::open(catalog_path, Arc::clone(&store))
                .await
                .unwrap(),
        );
        let lifecycle = BranchLifecycle::new(catalog.clone(), roots.clone());
        assert_eq!(
            lifecycle
                .mount_branch_by_name(request.clone())
                .await
                .expect("an exact mount retry must survive a catalog process restart"),
            original
        );
        assert_eq!(
            lifecycle
                .leases()
                .recover_writer(request.lease.clone())
                .await
                .expect("writer recovery must resolve only the exact retained capability"),
            original.lease
        );
        let mut wrong_recovery = request.lease.clone();
        wrong_recovery.renewal_token = Uuid::new_v4();
        assert!(matches!(
            lifecycle.leases().recover_writer(wrong_recovery).await,
            Err(crate::catalog::LeaseLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));

        assert!(matches!(
            lifecycle
                .deletions()
                .delete_branch(BranchDeleteRequest {
                    operation_id: Uuid::new_v4(),
                    branch_id,
                    expected_revision: branch.revision,
                    name: branch.name.clone(),
                })
                .await,
            Err(crate::catalog::DeletionLifecycleError::Catalog(
                CatalogError::WriterLeaseActive(id)
            )) if id == branch_id
        ));
        let recovered_writer = Db::builder(Path::from(root.identity.clone()), Arc::clone(&store))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        recovered_writer.put(&mount_key, b"head").await.unwrap();
        recovered_writer.flush().await.unwrap();
        recovered_writer.close().await.unwrap();
        let published = lifecycle
            .publish_writer_head(&original.lease)
            .await
            .unwrap();
        assert!(matches!(
            lifecycle
                .deletions()
                .delete_branch(BranchDeleteRequest {
                    operation_id: Uuid::new_v4(),
                    branch_id,
                    expected_revision: published.revision,
                    name: branch.name.clone(),
                })
                .await
                .unwrap(),
            BranchDeleteResult::Deleted(_)
        ));
        let replacement_id = Uuid::new_v4();
        catalog
            .apply(CatalogMutation::CreateBranch(BranchRecord {
                id: replacement_id,
                revision: 1,
                name: branch.name.clone(),
                state: BranchState::Ready,
                root: Some(root.clone()),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at: now,
                updated_at: now,
            }))
            .await
            .unwrap();

        assert!(matches!(
            lifecycle.mount_branch_by_name(request.clone()).await,
            Err(BranchLifecycleError::Lease(
                crate::catalog::LeaseLifecycleError::Catalog(CatalogError::NotFound(_))
            ))
        ));
        let mut retarget = request;
        retarget.lease.lease_id = Uuid::new_v4();
        retarget.lease.renewal_token = Uuid::new_v4();
        assert!(matches!(
            lifecycle.mount_branch_by_name(retarget).await,
            Err(BranchLifecycleError::Lease(
                crate::catalog::LeaseLifecycleError::Catalog(CatalogError::NotFound(_))
            ))
        ));
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn writer_shutdown_publishes_head_and_releases_exact_capability_atomically() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("writer-shutdown/source");
        let source_db = Db::builder(source_path.clone(), Arc::clone(&store))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        let key = KeyCodec::new().inode_key(7);
        source_db.put(&key, b"before").await.unwrap();
        source_db.flush().await.unwrap();
        let checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();

        let branch_id = Uuid::new_v4();
        let roots =
            SlateDbRootStore::new(Arc::clone(&store), Path::from("writer-shutdown/branches"));
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
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("writer-shutdown/catalog"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let now = catalog_timestamp(Utc::now());
        catalog
            .apply(CatalogMutation::CreateBranch(BranchRecord {
                id: branch_id,
                revision: 1,
                name: "writable".to_string(),
                state: BranchState::Ready,
                root: Some(initial.clone()),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at: now,
                updated_at: now,
            }))
            .await
            .unwrap();
        let lifecycle = BranchLifecycle::new(catalog.clone(), roots.clone());
        let mount = lifecycle
            .mount_branch_by_name(BranchMountRequest {
                branch_name: "writable".to_string(),
                lease: LeaseAcquireRequest {
                    lease_id: Uuid::new_v4(),
                    renewal_token: Uuid::new_v4(),
                    subject_id: branch_id,
                    access_mode: LeaseAccessMode::Write,
                    duration: chrono::Duration::minutes(2),
                },
            })
            .await
            .unwrap();
        let writer = Db::builder(Path::from(initial.identity.clone()), Arc::clone(&store))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        writer.put(&key, b"after").await.unwrap();
        writer.flush().await.unwrap();
        writer.close().await.unwrap();

        let (left_publish, right_publish) = tokio::join!(
            lifecycle.publish_writer_head(&mount.lease),
            lifecycle.publish_writer_head(&mount.lease)
        );
        let published = left_publish.unwrap();
        assert_eq!(right_publish.unwrap(), published);
        assert_eq!(published.revision, 2);
        assert_ne!(published.root.as_ref(), Some(&initial));
        assert_eq!(
            lifecycle.publish_writer_head(&mount.lease).await.unwrap(),
            published
        );
        let snapshot = catalog.snapshot().await.unwrap();
        assert!(!snapshot.leases.contains_key(&mount.lease.lease.id));
        let publication = snapshot
            .lease_tombstones
            .get(&mount.lease.lease.id)
            .unwrap()
            .writer_head
            .as_ref()
            .unwrap();
        assert_eq!(publication.branch_id, branch_id);
        assert_eq!(publication.previous_root, initial);
        assert_eq!(publication.root, published.root.clone().unwrap());
        assert_eq!(
            roots
                .checkpoint_reader(published.root.as_ref().unwrap())
                .await
                .unwrap()
                .get(&key)
                .await
                .unwrap(),
            Some(bytes::Bytes::from_static(b"after"))
        );
        let next_request = BranchMountRequest {
            branch_name: "writable".to_string(),
            lease: LeaseAcquireRequest {
                lease_id: Uuid::new_v4(),
                renewal_token: Uuid::new_v4(),
                subject_id: branch_id,
                access_mode: LeaseAccessMode::Write,
                duration: chrono::Duration::microseconds(1),
            },
        };
        let next_mount = lifecycle
            .mount_branch_by_name(next_request.clone())
            .await
            .unwrap();
        assert_eq!(next_mount.lease.lease.root, published.root.unwrap());
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        assert!(matches!(
            lifecycle.mount_branch_by_name(next_request.clone()).await,
            Err(BranchLifecycleError::Lease(
                crate::catalog::LeaseLifecycleError::Catalog(CatalogError::OperationConflict(_))
            ))
        ));
        assert_eq!(
            lifecycle
                .leases()
                .recover_writer(next_request.lease)
                .await
                .expect("expired writer recovery must not extend the retained capability"),
            next_mount.lease
        );
        catalog.close().await.unwrap();
    }

    #[test]
    fn inspection_distinguishes_tombstoned_retired_and_missing_history() {
        let now = catalog_timestamp(Utc::now());
        let parent_id = Uuid::new_v4();
        let checkpoint_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        let mut snapshot = CatalogSnapshot::default();
        snapshot.branches.insert(
            child_id,
            BranchRecord {
                id: child_id,
                revision: 1,
                name: "child".to_string(),
                state: BranchState::Ready,
                root: Some(DurableRoot {
                    identity: "inspection-history/child".to_string(),
                    manifest_id: format!("{}@1", Uuid::new_v4()),
                }),
                parent_id: Some(parent_id),
                origin_checkpoint_id: Some(checkpoint_id),
                created_at: now,
                updated_at: now,
            },
        );
        snapshot.tombstones.insert(
            parent_id,
            TombstoneRecord {
                id: parent_id,
                kind: TombstoneKind::Branch,
                name: "parent".to_string(),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at: now,
                deleted_revision: Some(1),
                deletion_operation_id: Some(Uuid::new_v4()),
                deleted_generation: 1,
                deleted_at: now,
            },
        );
        snapshot.retired_catalog_ids.insert(
            checkpoint_id,
            super::super::RetiredCatalogId {
                id: checkpoint_id,
                kind: RetiredCatalogKind::Checkpoint,
            },
        );
        let BranchInspection::Live { lineage, .. } =
            inspect_branch_id(&snapshot, child_id).unwrap()
        else {
            panic!("live child must inspect as live");
        };
        assert_eq!(
            lineage.parent.unwrap().status,
            HistoricalResourceStatus::Tombstoned
        );
        assert_eq!(
            lineage.origin_checkpoint.unwrap().status,
            HistoricalResourceStatus::Retired
        );

        snapshot.tombstones.remove(&parent_id);
        assert_eq!(
            match inspect_branch_id(&snapshot, child_id).unwrap() {
                BranchInspection::Live { lineage, .. } => lineage.parent.unwrap().status,
                _ => panic!("live child must inspect as live"),
            },
            HistoricalResourceStatus::Missing
        );
    }

    #[tokio::test]
    async fn configured_lifecycle_features_default_off_and_enable_independently() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = crate::catalog::CatalogConfig {
            slatedb_path: "feature-controls/catalog".to_string(),
            ..crate::catalog::CatalogConfig::default()
        };
        let lifecycle = config
            .open_branch_lifecycle(
                Arc::clone(&store),
                Path::from("feature-controls/branches"),
                Path::from("feature-controls/segments"),
            )
            .await
            .unwrap();
        assert!(lifecycle.list_branches().await.unwrap().is_empty());

        let projected_id = Uuid::new_v4();
        let now = catalog_timestamp(Utc::now());
        lifecycle
            .catalog
            .apply(CatalogMutation::CreateBranch(BranchRecord {
                id: projected_id,
                revision: 1,
                name: "projected".to_string(),
                state: BranchState::Ready,
                root: Some(DurableRoot {
                    identity: "feature-controls/projected".to_string(),
                    manifest_id: format!("{}@1", Uuid::new_v4()),
                }),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at: now,
                updated_at: now,
            }))
            .await
            .unwrap();
        let projection_directory = tempfile::tempdir().unwrap();
        let projection =
            JsonCatalogProjection::new(projection_directory.path().join("catalog.json"));
        let volume_id = Uuid::new_v4();
        lifecycle
            .reconcile_projection(volume_id, &projection)
            .await
            .unwrap();
        let projected =
            super::super::CatalogProjection::record(&projection, volume_id, projected_id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(projected.resource_id, projected_id);
        assert_eq!(projected.name, "projected");
        assert!(matches!(
            lifecycle
                .reconcile_projection(Uuid::nil(), &projection)
                .await,
            Err(BranchLifecycleError::Catalog(CatalogError::Invalid(_)))
        ));

        let branch_id = Uuid::new_v4();
        let mount = BranchMountRequest {
            branch_name: "candidate".to_string(),
            lease: LeaseAcquireRequest {
                lease_id: Uuid::new_v4(),
                renewal_token: Uuid::new_v4(),
                subject_id: branch_id,
                access_mode: LeaseAccessMode::Read,
                duration: chrono::Duration::minutes(1),
            },
        };
        assert!(matches!(
            lifecycle.mount_branch_by_name(mount.clone()).await,
            Err(BranchLifecycleError::FeatureDisabled("branch mount"))
        ));
        assert!(matches!(
            lifecycle
                .leases()
                .acquire_branch_by_name(&mount.branch_name, mount.lease.clone())
                .await,
            Err(crate::catalog::LeaseLifecycleError::FeatureDisabled(
                "mount lease acquisition"
            ))
        ));
        assert!(matches!(
            lifecycle
                .create_from_checkpoint(BranchCreateRequest {
                    operation_id: Uuid::new_v4(),
                    destination_id: Uuid::new_v4(),
                    destination_name: "child".to_string(),
                    source: ImmutableCheckpoint {
                        database_path: Path::from("feature-controls/source"),
                        checkpoint_id: Uuid::new_v4(),
                        manifest_id: 1,
                    },
                    created_at: catalog_timestamp(Utc::now()),
                })
                .await,
            Err(BranchLifecycleError::FeatureDisabled("branch creation"))
        ));
        assert!(matches!(
            lifecycle
                .deletions()
                .delete_branch(BranchDeleteRequest {
                    operation_id: Uuid::new_v4(),
                    branch_id,
                    expected_revision: 1,
                    name: "candidate".to_string(),
                })
                .await,
            Err(crate::catalog::DeletionLifecycleError::FeatureDisabled(
                "branch deletion"
            ))
        ));
        assert!(matches!(
            lifecycle
                .deletions()
                .delete_checkpoint(CheckpointDeleteRequest {
                    checkpoint_id: Uuid::new_v4(),
                    expected_revision: 1,
                    name: "checkpoint".to_string(),
                })
                .await,
            Err(crate::catalog::DeletionLifecycleError::FeatureDisabled(
                "checkpoint deletion"
            ))
        ));

        let enabled = crate::catalog::CatalogConfig {
            slatedb_path: "feature-controls/enabled-catalog".to_string(),
            features: BranchFeatureConfig {
                mount: true,
                ..BranchFeatureConfig::default()
            },
        }
        .open_branch_lifecycle(
            store,
            Path::from("feature-controls/enabled-branches"),
            Path::from("feature-controls/enabled-segments"),
        )
        .await
        .unwrap();
        assert!(matches!(
            enabled.mount_branch_by_name(mount).await,
            Err(BranchLifecycleError::Lease(
                crate::catalog::LeaseLifecycleError::Catalog(CatalogError::NotFound(_))
            ))
        ));
        enabled.close().await.unwrap();
        lifecycle.close().await.unwrap();
    }

    #[test]
    fn administrative_inspection_reports_only_incomplete_operations() {
        let now = catalog_timestamp(Utc::now());
        let root = DurableRoot {
            identity: "admin/branch".to_string(),
            manifest_id: format!("{}@1", Uuid::new_v4()),
        };
        let mut snapshot = CatalogSnapshot {
            generation: 42,
            ..CatalogSnapshot::default()
        };
        for (value, phase) in [
            (1, BranchCreatePhase::Reserved),
            (2, BranchCreatePhase::RootCreated),
            (3, BranchCreatePhase::Published),
        ] {
            let id = Uuid::from_u128(value);
            snapshot.branch_create_operations.insert(
                id,
                BranchCreateOperation {
                    id,
                    revision: 1,
                    destination_id: Uuid::new_v4(),
                    destination_name: format!("create-{value}"),
                    source_checkpoint_id: Uuid::new_v4(),
                    source_root: root.clone(),
                    parent_id: Some(Uuid::new_v4()),
                    phase,
                    destination_root: None,
                    created_at: now,
                    updated_at: now,
                },
            );
        }
        for (value, phase) in [
            (4, BranchDeletePhase::Draining),
            (5, BranchDeletePhase::Published),
        ] {
            let id = Uuid::from_u128(value);
            snapshot.branch_delete_operations.insert(
                id,
                BranchDeleteOperation {
                    id,
                    revision: 1,
                    branch_id: Uuid::new_v4(),
                    branch_name: format!("delete-{value}"),
                    expected_branch_revision: 1,
                    root: root.clone(),
                    parent_id: None,
                    origin_checkpoint_id: None,
                    phase,
                    created_at: now,
                    updated_at: now,
                },
            );
        }

        let creates = administrative_inspection_page(
            &snapshot,
            AdministrativeInspectionRequest {
                kind: AdministrativeInspectionKind::IncompleteBranchCreate,
                after: None,
                limit: 10,
            },
        );
        assert_eq!(creates.generation, 42);
        assert_eq!(creates.records.len(), 2);
        assert!(creates.records.iter().all(|record| matches!(
            record,
            AdministrativeInspectionRecord::IncompleteBranchCreate(operation)
                if operation.phase != BranchCreatePhase::Published
        )));
        let deletes = administrative_inspection_page(
            &snapshot,
            AdministrativeInspectionRequest {
                kind: AdministrativeInspectionKind::IncompleteBranchDelete,
                after: None,
                limit: 10,
            },
        );
        assert!(matches!(
            deletes.records.as_slice(),
            [AdministrativeInspectionRecord::IncompleteBranchDelete(operation)]
                if operation.phase == BranchDeletePhase::Draining
        ));

        let lease_id = Uuid::from_u128(6);
        snapshot.leases.insert(
            lease_id,
            LeaseRecord {
                id: lease_id,
                revision: 1,
                subject_kind: crate::catalog::LeaseSubjectKind::Branch,
                subject_id: Uuid::new_v4(),
                root,
                access_mode: LeaseAccessMode::Read,
                token_hash: "a".repeat(64),
                issued_at: now,
                updated_at: now,
                expires_at: now + chrono::Duration::minutes(1),
            },
        );
        let leases = administrative_inspection_page(
            &snapshot,
            AdministrativeInspectionRequest {
                kind: AdministrativeInspectionKind::Lease,
                after: None,
                limit: 10,
            },
        );
        assert!(matches!(
            leases.records.as_slice(),
            [AdministrativeInspectionRecord::Lease(record)] if record.id == lease_id
        ));
        assert!(
            !serde_json::to_string(&leases)
                .unwrap()
                .contains("token_hash")
        );
    }
}
