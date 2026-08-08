use super::gc_mark::{GcMarkError, GcMarkStore};
use super::{
    Catalog, CatalogError, GcRootKind, GcRunPhase, GcRunRecord, ImmutableCheckpoint,
    RootStoreError, SlateDbRootStore, catalog_timestamp, gc_root_digest,
};
use chrono::Utc;
use futures::{StreamExt, stream};
use std::sync::Arc;
use uuid::Uuid;

const ROOT_VERIFY_CONCURRENCY: usize = 16;

#[derive(Clone)]
pub struct RootCaptureLifecycle {
    catalog: Arc<dyn Catalog>,
    roots: SlateDbRootStore,
}

impl std::fmt::Debug for RootCaptureLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RootCaptureLifecycle")
            .finish_non_exhaustive()
    }
}

impl RootCaptureLifecycle {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, roots: SlateDbRootStore) -> Self {
        Self { catalog, roots }
    }

    /// Authenticate every root at one catalog generation, then durably pin the
    /// exact typed list. A concurrent catalog mutation fails the generation
    /// fence and leaves no partial run record.
    pub async fn begin(&self, run_id: Uuid) -> Result<GcRunRecord, RootCaptureLifecycleError> {
        if run_id.is_nil() {
            return Err(CatalogError::Invalid("GC run UUID cannot be nil".to_string()).into());
        }
        if let Some(existing) = self.catalog.gc_run(run_id).await? {
            return Ok(existing);
        }
        let snapshot = self.catalog.snapshot().await?;
        let inventory_cutoff = catalog_timestamp(Utc::now());
        let pins = snapshot.gc_root_pins();
        let root_store = self.roots.clone();
        let mut verification = stream::iter(pins.iter().cloned())
            .map(move |pin| {
                let root_store = root_store.clone();
                async move {
                    match pin.kind {
                        GcRootKind::Branch => root_store.verify(&pin.root).await,
                        GcRootKind::Checkpoint => {
                            let checkpoint = ImmutableCheckpoint::from_durable_root(&pin.root)?;
                            root_store.verify_checkpoint(&checkpoint).await
                        }
                    }
                }
            })
            .buffer_unordered(ROOT_VERIFY_CONCURRENCY);
        while let Some(result) = verification.next().await {
            result?;
        }
        drop(verification);
        let run = GcRunRecord {
            id: run_id,
            revision: 1,
            catalog_generation: snapshot.generation,
            inventory_cutoff,
            root_digest: gc_root_digest(&pins)?,
            roots: pins,
            mark_shards: Vec::new(),
            mark_stats: None,
            phase: GcRunPhase::Captured,
            quarantine_at: None,
            created_at: inventory_cutoff,
            updated_at: inventory_cutoff,
        };
        let result = self
            .catalog
            .begin_gc_run(snapshot.generation, run.clone())
            .await;
        if let Err(error) = result {
            if let Some(existing) = self.catalog.gc_run(run_id).await? {
                if existing == run {
                    return Ok(existing);
                }
                return Err(CatalogError::OperationConflict(run_id.to_string()).into());
            }
            return Err(error.into());
        }
        Ok(run)
    }

    /// Stream every captured checkpoint's extent pointers into bounded sorted
    /// runs, merge/deduplicate them, and publish 256 independently verifiable
    /// mark shards atomically in the authoritative run record.
    pub async fn mark(&self, run_id: Uuid) -> Result<GcRunRecord, RootCaptureLifecycleError> {
        let run = self
            .catalog
            .gc_run(run_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(run_id.to_string()))?;
        let store = GcMarkStore::new(self.roots.clone());
        if run.phase == GcRunPhase::Marking {
            let digest = decode_digest(&run.root_digest)?;
            store.verify_all(run.id, digest, &run.mark_shards).await?;
            return Ok(run);
        }
        if run.phase != GcRunPhase::Captured || run.revision != 1 {
            return Err(CatalogError::OperationConflict(run_id.to_string()).into());
        }
        let build = store.build(&run).await?;
        let mark_shards = build.shards;
        let mark_stats = build.stats;
        let published = self
            .catalog
            .publish_gc_marks(
                run.id,
                run.revision,
                run.root_digest.clone(),
                mark_shards.clone(),
                mark_stats.clone(),
                catalog_timestamp(Utc::now()),
            )
            .await;
        match published {
            Ok(run) => Ok(run),
            Err(error) => {
                if let Some(existing) = self.catalog.gc_run(run_id).await?
                    && existing.phase == GcRunPhase::Marking
                    && existing.root_digest == run.root_digest
                    && existing.mark_shards == mark_shards
                    && existing.mark_stats.as_ref() == Some(&mark_stats)
                {
                    let digest = decode_digest(&existing.root_digest)?;
                    store
                        .verify_all(existing.id, digest, &existing.mark_shards)
                        .await?;
                    return Ok(existing);
                }
                Err(error.into())
            }
        }
    }
}

fn decode_digest(value: &str) -> Result<[u8; 32], RootCaptureLifecycleError> {
    if value.len() != 64 {
        return Err(CatalogError::Corrupt("invalid GC root digest".to_string()).into());
    }
    let mut output = [0u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| CatalogError::Corrupt("invalid GC root digest".to_string()))?;
    }
    Ok(output)
}

#[derive(Debug, thiserror::Error)]
pub enum RootCaptureLifecycleError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    RootStore(#[from] RootStoreError),
    #[error("GC marking failed: {0}")]
    Mark(String),
}

impl From<GcMarkError> for RootCaptureLifecycleError {
    fn from(error: GcMarkError) -> Self {
        Self::Mark(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        BranchRecord, BranchState, CatalogMutation, CatalogSnapshot, CheckpointRecord, DurableRoot,
        GcRootPin, LeaseAccessMode, LeaseRecord, LeaseSubjectKind, SlateDbCatalog,
    };
    use crate::fs::key_codec::KeyCodec;
    use crate::segment::{FrameLoc, Segid};
    use object_store::{ObjectStore, ObjectStoreExt};
    use slatedb::config::{CheckpointOptions, CheckpointScope};
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;
    use slatedb::{Db, WriteBatch};

    #[test]
    fn root_list_is_typed_canonical_and_deduplicated() {
        let now = catalog_timestamp(Utc::now());
        let branch_id = Uuid::new_v4();
        let branch_root = DurableRoot {
            identity: "branches/one".to_string(),
            manifest_id: format!("{}@1", Uuid::new_v4()),
        };
        let checkpoint_root = DurableRoot {
            identity: "checkpoints/one".to_string(),
            manifest_id: format!("{}@1", Uuid::new_v4()),
        };
        let mut snapshot = CatalogSnapshot::default();
        snapshot.branches.insert(
            branch_id,
            BranchRecord {
                id: branch_id,
                revision: 1,
                name: "one".to_string(),
                state: BranchState::Ready,
                root: Some(branch_root.clone()),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at: now,
                updated_at: now,
            },
        );
        let checkpoint_id = Uuid::new_v4();
        snapshot.checkpoints.insert(
            checkpoint_id,
            CheckpointRecord {
                id: checkpoint_id,
                revision: 1,
                branch_id,
                name: "point".to_string(),
                root: checkpoint_root.clone(),
                created_at: now,
                updated_at: now,
            },
        );
        let lease_id = Uuid::new_v4();
        snapshot.leases.insert(
            lease_id,
            LeaseRecord {
                id: lease_id,
                revision: 1,
                subject_kind: LeaseSubjectKind::Branch,
                subject_id: branch_id,
                root: branch_root.clone(),
                access_mode: LeaseAccessMode::Read,
                token_hash: "a".repeat(64),
                issued_at: now,
                updated_at: now,
                expires_at: now + chrono::Duration::minutes(1),
            },
        );
        assert_eq!(
            snapshot.gc_root_pins(),
            vec![
                GcRootPin {
                    kind: GcRootKind::Branch,
                    root: branch_root,
                },
                GcRootPin {
                    kind: GcRootKind::Checkpoint,
                    root: checkpoint_root,
                },
            ]
        );
    }

    #[test]
    fn unsupported_terminal_phase_fails_closed_and_keeps_roots_pinned() {
        let now = catalog_timestamp(Utc::now());
        let root = DurableRoot {
            identity: "branches/terminal".to_string(),
            manifest_id: format!("{}@1", Uuid::new_v4()),
        };
        let pins = vec![GcRootPin {
            kind: GcRootKind::Branch,
            root: root.clone(),
        }];
        let run = GcRunRecord {
            id: Uuid::new_v4(),
            revision: 2,
            catalog_generation: 0,
            inventory_cutoff: now,
            root_digest: gc_root_digest(&pins).unwrap(),
            roots: pins,
            mark_shards: Vec::new(),
            mark_stats: None,
            phase: GcRunPhase::Completed,
            quarantine_at: None,
            created_at: now,
            updated_at: now,
        };
        assert!(matches!(run.validate(), Err(CatalogError::Invalid(_))));
        let mut snapshot = CatalogSnapshot::default();
        snapshot.gc_runs.insert(run.id, run);
        assert!(snapshot.validate().is_err());
        assert!(snapshot.gc_roots().contains(&&root));
    }

    #[tokio::test]
    async fn empty_capture_is_durable_exact_and_generation_neutral() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("gc-capture-empty"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let lifecycle = RootCaptureLifecycle::new(
            catalog.clone(),
            SlateDbRootStore::new(store, Path::from("gc-capture-branches")),
        );
        let id = Uuid::new_v4();
        let run = lifecycle.begin(id).await.unwrap();
        assert_eq!(run.catalog_generation, 0);
        assert!(run.roots.is_empty());
        assert_eq!(catalog.snapshot().await.unwrap().generation, 0);
        assert_eq!(lifecycle.begin(id).await.unwrap(), run);
        let bad_id = Uuid::new_v4();
        let mut mismatched = run;
        mismatched.id = bad_id;
        mismatched.root_digest = "a".repeat(64);
        assert!(matches!(
            catalog.begin_gc_run(0, mismatched).await,
            Err(CatalogError::Invalid(_))
        ));
        assert!(catalog.gc_run(bad_id).await.unwrap().is_none());
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn unreadable_root_aborts_without_persisting_a_run() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("gc-capture-invalid"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let now = catalog_timestamp(Utc::now());
        catalog
            .apply(CatalogMutation::CreateBranch(BranchRecord {
                id: Uuid::new_v4(),
                revision: 1,
                name: "invalid-root".to_string(),
                state: BranchState::Ready,
                root: Some(DurableRoot {
                    identity: "gc-capture-branches/missing".to_string(),
                    manifest_id: format!("{}@1", Uuid::new_v4()),
                }),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at: now,
                updated_at: now,
            }))
            .await
            .unwrap();
        let id = Uuid::new_v4();
        let lifecycle = RootCaptureLifecycle::new(
            catalog.clone(),
            SlateDbRootStore::new(store, Path::from("gc-capture-branches")),
        );
        assert!(matches!(
            lifecycle.begin(id).await,
            Err(RootCaptureLifecycleError::RootStore(_))
        ));
        assert!(catalog.gc_run(id).await.unwrap().is_none());
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn generation_change_rejects_capture_without_partial_pins() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("gc-capture-generation"), store)
                .await
                .unwrap(),
        );
        let captured = catalog.snapshot().await.unwrap();
        let now = catalog_timestamp(Utc::now());
        catalog
            .apply(CatalogMutation::CreateBranch(BranchRecord {
                id: Uuid::new_v4(),
                revision: 1,
                name: "concurrent".to_string(),
                state: BranchState::Ready,
                root: Some(DurableRoot {
                    identity: "gc-capture-branches/concurrent".to_string(),
                    manifest_id: format!("{}@1", Uuid::new_v4()),
                }),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at: now,
                updated_at: now,
            }))
            .await
            .unwrap();
        let id = Uuid::new_v4();
        let pins = captured.gc_root_pins();
        let run = GcRunRecord {
            id,
            revision: 1,
            catalog_generation: captured.generation,
            inventory_cutoff: now,
            root_digest: gc_root_digest(&pins).unwrap(),
            roots: pins,
            mark_shards: Vec::new(),
            mark_stats: None,
            phase: GcRunPhase::Captured,
            quarantine_at: None,
            created_at: now,
            updated_at: now,
        };
        assert!(matches!(
            catalog.begin_gc_run(captured.generation, run).await,
            Err(CatalogError::RevisionConflict {
                expected: 0,
                actual: 1
            })
        ));
        assert!(catalog.gc_run(id).await.unwrap().is_none());
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn marking_streams_exact_checkpoint_and_publishes_verified_shards() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let data_path = Path::from("gc-mark-data");
        let db = Db::builder(data_path.clone(), Arc::clone(&store))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        let codec = KeyCodec::new();
        let mut batch = WriteBatch::new();
        for index in 0..9_000u64 {
            let segment = Segid::new(7, index % 1_000);
            batch.put(
                codec.extent_key(1, index),
                FrameLoc {
                    segid: segment,
                    frame_index: 0,
                    byte_offset: 0,
                    byte_len: 1,
                }
                .encode(),
            );
        }
        db.write(batch).await.unwrap();
        db.flush().await.unwrap();
        let checkpoint = db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        db.put(
            codec.extent_key(2, 0),
            FrameLoc {
                segid: Segid::new(99, 99_999),
                frame_index: 0,
                byte_offset: 0,
                byte_len: 1,
            }
            .encode(),
        )
        .await
        .unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        let root = ImmutableCheckpoint {
            database_path: data_path,
            checkpoint_id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        }
        .durable_root();
        let pins = vec![crate::catalog::GcRootPin {
            kind: GcRootKind::Checkpoint,
            root,
        }];
        let now = catalog_timestamp(Utc::now());
        let run = GcRunRecord {
            id: Uuid::new_v4(),
            revision: 1,
            catalog_generation: 0,
            inventory_cutoff: now,
            root_digest: gc_root_digest(&pins).unwrap(),
            roots: pins,
            mark_shards: Vec::new(),
            mark_stats: None,
            phase: GcRunPhase::Captured,
            quarantine_at: None,
            created_at: now,
            updated_at: now,
        };
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("gc-mark-catalog"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        catalog.begin_gc_run(0, run.clone()).await.unwrap();
        let artifact_store = Arc::clone(&store);
        let lifecycle = RootCaptureLifecycle::new(
            catalog.clone(),
            SlateDbRootStore::new(store, Path::from("gc-mark-branches")),
        );
        let marked = lifecycle.mark(run.id).await.unwrap();
        assert_eq!(marked.phase, GcRunPhase::Marking);
        assert_eq!(marked.revision, 2);
        assert_eq!(marked.mark_shards.len(), 256);
        assert_eq!(
            marked
                .mark_shards
                .iter()
                .map(|shard| shard.segment_count)
                .sum::<u64>(),
            1_000
        );
        let stats = marked.mark_stats.as_ref().unwrap();
        assert_eq!(stats.roots_enumerated, 1);
        assert_eq!(stats.references_enumerated, 9_000);
        assert_eq!(stats.intermediate_runs, 512);
        assert_eq!(stats.unique_segments, 1_000);
        assert_eq!(lifecycle.mark(run.id).await.unwrap(), marked);
        assert_eq!(catalog.snapshot().await.unwrap().generation, 0);
        artifact_store
            .put(
                &Path::from(marked.mark_shards[0].location.clone()),
                bytes::Bytes::from_static(b"corrupt").into(),
            )
            .await
            .unwrap();
        assert!(matches!(
            lifecycle.mark(run.id).await,
            Err(RootCaptureLifecycleError::Mark(_))
        ));
        catalog.close().await.unwrap();
    }
}
