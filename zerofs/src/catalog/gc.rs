use super::gc_inventory::{GcInventoryError, GcInventoryStore};
use super::gc_mark::{GcMarkError, GcMarkStore};
use super::{
    Catalog, CatalogError, GcBlockerKind, GcQuarantinePublication, GcRevalidationCapture,
    GcRevalidationPublication, GcRevalidationRecord, GcRootKind, GcRunPhase, GcRunRecord,
    ImmutableCheckpoint, RootStoreError, SlateDbRootStore, catalog_timestamp, gc_root_digest,
};
use chrono::{DateTime, Duration, Utc};
use futures::{StreamExt, stream};
use std::sync::Arc;
use uuid::Uuid;

const ROOT_VERIFY_CONCURRENCY: usize = 16;
/// Five-minute maximum lease + 30-second skew + one-minute propagation bound.
pub(crate) const MIN_REVALIDATION_GRACE_SECONDS: u64 = 390;

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
            segment_pool: self.roots.segment_pool_root().to_string(),
            roots: pins,
            mark_shards: Vec::new(),
            mark_stats: None,
            quarantine_shards: Vec::new(),
            inventory_stats: None,
            phase: GcRunPhase::Captured,
            quarantine_at: None,
            revalidation: None,
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
        if matches!(run.phase, GcRunPhase::Marking | GcRunPhase::Quarantined) {
            let digest = decode_digest(&run.root_digest)?;
            if let Err(error) = store.verify_all(run.id, digest, &run.mark_shards).await {
                self.record_blocker(run.id, classify_mark_error(&error), error.to_string())
                    .await;
                return Err(error.into());
            }
            return Ok(run);
        }
        if run.phase != GcRunPhase::Captured || run.revision != 1 {
            return Err(CatalogError::OperationConflict(run_id.to_string()).into());
        }
        let build = match store.build(&run).await {
            Ok(build) => build,
            Err(error) => {
                self.record_blocker(run.id, classify_mark_error(&error), error.to_string())
                    .await;
                return Err(error.into());
            }
        };
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

    /// Stream the physical segment pool, exclude objects newer than the
    /// captured cutoff, merge-join it against the authoritative mark shards,
    /// and publish a durable first unreachable observation. No segment is
    /// physically deleted by this transition.
    pub async fn quarantine(&self, run_id: Uuid) -> Result<GcRunRecord, RootCaptureLifecycleError> {
        let run = self
            .catalog
            .gc_run(run_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(run_id.to_string()))?;
        let digest = decode_digest(&run.root_digest)?;
        let marks = GcMarkStore::new(self.roots.clone());
        if let Err(error) = marks.verify_all(run.id, digest, &run.mark_shards).await {
            self.record_blocker(run.id, classify_mark_error(&error), error.to_string())
                .await;
            return Err(error.into());
        }
        let inventory = GcInventoryStore::new(&self.roots);
        if run.phase == GcRunPhase::Quarantined {
            if let Err(error) = inventory
                .verify_all(&run, digest, &run.quarantine_shards)
                .await
            {
                self.record_blocker(run.id, classify_inventory_error(&error), error.to_string())
                    .await;
                return Err(error.into());
            }
            return Ok(run);
        }
        if run.phase != GcRunPhase::Marking || run.revision != 2 {
            return Err(CatalogError::OperationConflict(run_id.to_string()).into());
        }
        let build = match inventory.build(&run).await {
            Ok(build) => build,
            Err(error) => {
                self.record_blocker(run.id, classify_inventory_error(&error), error.to_string())
                    .await;
                return Err(error.into());
            }
        };
        let quarantine_at = catalog_timestamp(Utc::now());
        let published = self
            .catalog
            .publish_gc_quarantine(GcQuarantinePublication {
                id: run.id,
                expected_revision: run.revision,
                expected_generation: run.catalog_generation,
                root_digest: run.root_digest.clone(),
                quarantine_shards: build.shards.clone(),
                inventory_stats: build.stats.clone(),
                quarantine_at,
            })
            .await;
        match published {
            Ok(run) => Ok(run),
            Err(error) => {
                if let Some(existing) = self.catalog.gc_run(run_id).await?
                    && existing.phase == GcRunPhase::Quarantined
                    && existing.root_digest == run.root_digest
                    && existing.quarantine_shards == build.shards
                    && existing.inventory_stats.as_ref() == Some(&build.stats)
                    && existing.quarantine_at == Some(quarantine_at)
                {
                    inventory
                        .verify_all(&existing, digest, &existing.quarantine_shards)
                        .await?;
                    return Ok(existing);
                }
                if self
                    .catalog
                    .snapshot()
                    .await
                    .is_ok_and(|snapshot| snapshot.generation != run.catalog_generation)
                {
                    self.record_blocker(
                        run.id,
                        GcBlockerKind::GenerationChanged,
                        format!(
                            "catalog generation changed after capture at {}",
                            run.catalog_generation
                        ),
                    )
                    .await;
                }
                Err(error.into())
            }
        }
    }

    /// After the mandatory grace, capture and authenticate a fresh catalog
    /// generation, independently mark it, then retain only candidates still
    /// unreachable and byte-for-byte unchanged in the physical pool.
    pub async fn revalidate(
        &self,
        run_id: Uuid,
        grace: Duration,
    ) -> Result<GcRunRecord, RootCaptureLifecycleError> {
        self.revalidate_at(run_id, grace, catalog_timestamp(Utc::now()))
            .await
    }

    async fn revalidate_at(
        &self,
        run_id: Uuid,
        grace: Duration,
        now: DateTime<Utc>,
    ) -> Result<GcRunRecord, RootCaptureLifecycleError> {
        let grace_seconds = grace.num_seconds();
        if grace_seconds < MIN_REVALIDATION_GRACE_SECONDS as i64
            || grace != Duration::seconds(grace_seconds)
        {
            return Err(CatalogError::Invalid(format!(
                "GC revalidation grace must be at least {MIN_REVALIDATION_GRACE_SECONDS} whole seconds"
            ))
            .into());
        }
        let grace_seconds = u64::try_from(grace_seconds)
            .map_err(|_| CatalogError::Invalid("GC revalidation grace is invalid".to_string()))?;
        let mut run = self
            .catalog
            .gc_run(run_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(run_id.to_string()))?;
        if run
            .revalidation
            .as_ref()
            .is_some_and(|observation| observation.grace_seconds != grace_seconds)
        {
            return Err(CatalogError::OperationConflict(run_id.to_string()).into());
        }
        let marks = GcMarkStore::new(self.roots.clone());
        let inventory = GcInventoryStore::new(&self.roots);
        if run.phase == GcRunPhase::Validated {
            let observation = run.revalidation.as_ref().ok_or_else(|| {
                CatalogError::Corrupt("validated GC run has no revalidation".to_string())
            })?;
            marks
                .verify_all(
                    observation.id,
                    decode_digest(&observation.root_digest)?,
                    &observation.mark_shards,
                )
                .await?;
            inventory.verify_revalidation(observation).await?;
            return Ok(run);
        }
        if run.phase == GcRunPhase::Quarantined {
            let quarantine_at = run.quarantine_at.ok_or_else(|| {
                CatalogError::Corrupt("quarantined GC run has no timestamp".to_string())
            })?;
            let not_before = quarantine_at
                .checked_add_signed(grace)
                .ok_or_else(|| CatalogError::Invalid("GC grace overflows time".to_string()))?;
            if now < not_before {
                return Err(RootCaptureLifecycleError::GracePeriod {
                    not_before,
                    observed_at: now,
                });
            }
            let snapshot = self.catalog.snapshot().await?;
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
            let observation = GcRevalidationRecord {
                id: Uuid::new_v4(),
                catalog_generation: snapshot.generation,
                grace_seconds,
                not_before,
                inventory_cutoff: now,
                roots: pins.clone(),
                root_digest: gc_root_digest(&pins)?,
                mark_shards: Vec::new(),
                mark_stats: None,
                candidate_shards: Vec::new(),
                stats: None,
                captured_at: now,
                completed_at: None,
            };
            let captured = self
                .catalog
                .begin_gc_revalidation(GcRevalidationCapture {
                    run_id,
                    expected_revision: run.revision,
                    expected_generation: snapshot.generation,
                    observation: observation.clone(),
                    updated_at: now,
                })
                .await;
            run = match captured {
                Ok(run) => run,
                Err(error) => {
                    if let Some(existing) = self.catalog.gc_run(run_id).await?
                        && matches!(
                            existing.phase,
                            GcRunPhase::Revalidating | GcRunPhase::Validated
                        )
                        && existing.revalidation.as_ref().is_some_and(|stored| {
                            stored.id == observation.id
                                && stored.catalog_generation == observation.catalog_generation
                                && stored.grace_seconds == observation.grace_seconds
                                && stored.not_before == observation.not_before
                                && stored.inventory_cutoff == observation.inventory_cutoff
                                && stored.roots == observation.roots
                                && stored.root_digest == observation.root_digest
                                && stored.captured_at == observation.captured_at
                        })
                    {
                        existing
                    } else {
                        return Err(error.into());
                    }
                }
            };
        }
        if run.phase == GcRunPhase::Validated {
            let stored = run.revalidation.as_ref().ok_or_else(|| {
                CatalogError::Corrupt("validated GC run has no revalidation".to_string())
            })?;
            marks
                .verify_all(
                    stored.id,
                    decode_digest(&stored.root_digest)?,
                    &stored.mark_shards,
                )
                .await?;
            inventory.verify_revalidation(stored).await?;
            return Ok(run);
        }
        if run.phase != GcRunPhase::Revalidating || run.revision != 4 {
            return Err(CatalogError::OperationConflict(run_id.to_string()).into());
        }
        let observation = run.revalidation.clone().ok_or_else(|| {
            CatalogError::Corrupt("revalidating GC run has no observation".to_string())
        })?;
        let build = match marks
            .build_observation(observation.id, &observation.root_digest, &observation.roots)
            .await
        {
            Ok(build) => build,
            Err(error) => {
                self.record_blocker(run.id, classify_mark_error(&error), error.to_string())
                    .await;
                return Err(error.into());
            }
        };
        let candidates = match inventory
            .revalidate(&run, &observation, &build.shards)
            .await
        {
            Ok(candidates) => candidates,
            Err(error) => {
                self.record_blocker(run.id, classify_inventory_error(&error), error.to_string())
                    .await;
                return Err(error.into());
            }
        };
        let completed_at = catalog_timestamp(Utc::now()).max(now);
        let published = self
            .catalog
            .publish_gc_revalidation(GcRevalidationPublication {
                run_id,
                expected_revision: run.revision,
                expected_generation: observation.catalog_generation,
                observation_id: observation.id,
                root_digest: observation.root_digest.clone(),
                mark_shards: build.shards.clone(),
                mark_stats: build.stats.clone(),
                candidate_shards: candidates.shards.clone(),
                stats: candidates.stats.clone(),
                completed_at,
            })
            .await;
        match published {
            Ok(run) => Ok(run),
            Err(error) => {
                if let Some(existing) = self.catalog.gc_run(run_id).await?
                    && existing.phase == GcRunPhase::Validated
                    && existing.revalidation.as_ref().is_some_and(|stored| {
                        stored.id == observation.id
                            && stored.catalog_generation == observation.catalog_generation
                            && stored.root_digest == observation.root_digest
                            && stored.mark_shards == build.shards
                            && stored.mark_stats.as_ref() == Some(&build.stats)
                            && stored.candidate_shards == candidates.shards
                            && stored.stats.as_ref() == Some(&candidates.stats)
                            && stored.completed_at == Some(completed_at)
                    })
                {
                    let stored = existing.revalidation.as_ref().expect("checked above");
                    marks
                        .verify_all(
                            stored.id,
                            decode_digest(&stored.root_digest)?,
                            &stored.mark_shards,
                        )
                        .await?;
                    inventory.verify_revalidation(stored).await?;
                    return Ok(existing);
                }
                if self
                    .catalog
                    .snapshot()
                    .await
                    .is_ok_and(|snapshot| snapshot.generation != observation.catalog_generation)
                {
                    self.record_blocker(
                        run_id,
                        GcBlockerKind::GenerationChanged,
                        format!(
                            "catalog generation changed during revalidation at {}",
                            observation.catalog_generation
                        ),
                    )
                    .await;
                }
                Err(error.into())
            }
        }
    }

    async fn record_blocker(&self, run_id: Uuid, kind: GcBlockerKind, mut detail: String) {
        if detail.len() > super::MAX_ROOT_IDENTIFIER_BYTES {
            let mut end = super::MAX_ROOT_IDENTIFIER_BYTES;
            while !detail.is_char_boundary(end) {
                end -= 1;
            }
            detail.truncate(end);
        }
        let _ = self
            .catalog
            .record_gc_blocker(run_id, kind, detail, catalog_timestamp(Utc::now()))
            .await;
    }
}

fn classify_mark_error(error: &GcMarkError) -> GcBlockerKind {
    match error {
        GcMarkError::RootStore(_) => GcBlockerKind::MissingRoot,
        GcMarkError::Corrupt(_) => GcBlockerKind::CorruptMetadata,
        GcMarkError::SlateDb(_) | GcMarkError::ObjectStore(_) | GcMarkError::Io(_) => {
            GcBlockerKind::StorageUnavailable
        }
    }
}

fn classify_inventory_error(error: &GcInventoryError) -> GcBlockerKind {
    match error {
        GcInventoryError::Corrupt(_) => GcBlockerKind::CorruptMetadata,
        GcInventoryError::Mark(error) => classify_mark_error(error),
        GcInventoryError::ObjectStore(_) | GcInventoryError::Io(_) => {
            GcBlockerKind::StorageUnavailable
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
    #[error("GC inventory failed: {0}")]
    Inventory(String),
    #[error(
        "GC revalidation grace has not elapsed: not before {not_before}, observed {observed_at}"
    )]
    GracePeriod {
        not_before: DateTime<Utc>,
        observed_at: DateTime<Utc>,
    },
}

impl From<GcMarkError> for RootCaptureLifecycleError {
    fn from(error: GcMarkError) -> Self {
        Self::Mark(error.to_string())
    }
}

impl From<GcInventoryError> for RootCaptureLifecycleError {
    fn from(error: GcInventoryError) -> Self {
        Self::Inventory(error.to_string())
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
            segment_pool: String::new(),
            roots: pins,
            mark_shards: Vec::new(),
            mark_stats: None,
            quarantine_shards: Vec::new(),
            inventory_stats: None,
            phase: GcRunPhase::Completed,
            quarantine_at: None,
            revalidation: None,
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
            segment_pool: String::new(),
            roots: pins,
            mark_shards: Vec::new(),
            mark_stats: None,
            quarantine_shards: Vec::new(),
            inventory_stats: None,
            phase: GcRunPhase::Captured,
            quarantine_at: None,
            revalidation: None,
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
    async fn revalidation_enforces_grace_and_filters_absent_candidates_without_deleting() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let data_path = Path::from("gc-revalidation-data");
        let db = Db::builder(data_path.clone(), Arc::clone(&store))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        let reachable = Segid::new(11, 0);
        db.put(
            KeyCodec::new().extent_key(1, 0),
            FrameLoc {
                segid: reachable,
                frame_index: 0,
                byte_offset: 0,
                byte_len: 1,
            }
            .encode(),
        )
        .await
        .unwrap();
        db.flush().await.unwrap();
        let checkpoint = db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        db.close().await.unwrap();
        let root = ImmutableCheckpoint {
            database_path: data_path,
            checkpoint_id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        }
        .durable_root();
        let pins = vec![GcRootPin {
            kind: GcRootKind::Checkpoint,
            root,
        }];
        let segment_pool = Path::from("gc-revalidation-pool");
        let became_reachable = Segid::new(12, 256);
        let absent = Segid::new(12, 512);
        let retained = Segid::new(12, 768);
        for segment in [reachable, became_reachable, absent, retained] {
            store
                .put(
                    &Path::from(format!("{segment_pool}/{}", segment.object_key())),
                    bytes::Bytes::from_static(b"x").into(),
                )
                .await
                .unwrap();
        }
        let now = catalog_timestamp(Utc::now());
        let inventory_cutoff = now + Duration::seconds(1);
        let run = GcRunRecord {
            id: Uuid::new_v4(),
            revision: 1,
            catalog_generation: 0,
            inventory_cutoff,
            root_digest: gc_root_digest(&pins).unwrap(),
            segment_pool: segment_pool.to_string(),
            roots: pins,
            mark_shards: Vec::new(),
            mark_stats: None,
            quarantine_shards: Vec::new(),
            inventory_stats: None,
            phase: GcRunPhase::Captured,
            quarantine_at: None,
            revalidation: None,
            created_at: now,
            updated_at: now,
        };
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("gc-revalidation-catalog"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        catalog.begin_gc_run(0, run.clone()).await.unwrap();
        let lifecycle = RootCaptureLifecycle::new(
            catalog.clone(),
            SlateDbRootStore::new(Arc::clone(&store), Path::from("gc-revalidation-branches"))
                .with_segment_pool_root(segment_pool.clone()),
        );
        lifecycle.mark(run.id).await.unwrap();
        let quarantined = lifecycle.quarantine(run.id).await.unwrap();
        assert_eq!(
            quarantined
                .inventory_stats
                .as_ref()
                .unwrap()
                .candidate_objects,
            3
        );
        store
            .delete(&Path::from(format!(
                "{segment_pool}/{}",
                absent.object_key()
            )))
            .await
            .unwrap();
        let new_data_path = Path::from("gc-revalidation-new-root");
        let db = Db::builder(new_data_path.clone(), Arc::clone(&store))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        db.put(
            KeyCodec::new().extent_key(2, 0),
            FrameLoc {
                segid: became_reachable,
                frame_index: 0,
                byte_offset: 0,
                byte_len: 1,
            }
            .encode(),
        )
        .await
        .unwrap();
        db.flush().await.unwrap();
        let new_checkpoint = db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        db.close().await.unwrap();
        let new_pins = vec![GcRootPin {
            kind: GcRootKind::Checkpoint,
            root: ImmutableCheckpoint {
                database_path: new_data_path,
                checkpoint_id: new_checkpoint.id,
                manifest_id: new_checkpoint.manifest_id,
            }
            .durable_root(),
        }];
        catalog
            .begin_gc_run(
                0,
                GcRunRecord {
                    id: Uuid::new_v4(),
                    revision: 1,
                    catalog_generation: 0,
                    inventory_cutoff: now,
                    root_digest: gc_root_digest(&new_pins).unwrap(),
                    segment_pool: segment_pool.to_string(),
                    roots: new_pins,
                    mark_shards: Vec::new(),
                    mark_stats: None,
                    quarantine_shards: Vec::new(),
                    inventory_stats: None,
                    phase: GcRunPhase::Captured,
                    quarantine_at: None,
                    revalidation: None,
                    created_at: now,
                    updated_at: now,
                },
            )
            .await
            .unwrap();
        let quarantine_at = quarantined.quarantine_at.unwrap();
        assert!(matches!(
            lifecycle
                .revalidate_at(
                    run.id,
                    Duration::seconds(MIN_REVALIDATION_GRACE_SECONDS as i64),
                    quarantine_at + Duration::seconds(MIN_REVALIDATION_GRACE_SECONDS as i64 - 1),
                )
                .await,
            Err(RootCaptureLifecycleError::GracePeriod { .. })
        ));
        let validated = lifecycle
            .revalidate_at(
                run.id,
                Duration::seconds(MIN_REVALIDATION_GRACE_SECONDS as i64),
                quarantine_at + Duration::seconds(MIN_REVALIDATION_GRACE_SECONDS as i64),
            )
            .await
            .unwrap();
        assert_eq!(validated.phase, GcRunPhase::Validated);
        assert_eq!(validated.revision, 5);
        let stats = validated
            .revalidation
            .as_ref()
            .unwrap()
            .stats
            .as_ref()
            .unwrap();
        assert_eq!(stats.first_observation_candidates, 3);
        assert_eq!(stats.became_reachable, 1);
        assert_eq!(stats.already_absent, 1);
        assert_eq!(stats.retained_candidates, 1);
        let mut malformed = validated.clone();
        malformed
            .revalidation
            .as_mut()
            .unwrap()
            .stats
            .as_mut()
            .unwrap()
            .first_observation_candidates = 2;
        assert!(matches!(
            malformed.validate(),
            Err(CatalogError::Invalid(_))
        ));
        assert!(
            store
                .head(&Path::from(format!(
                    "{segment_pool}/{}",
                    retained.object_key()
                )))
                .await
                .is_ok(),
            "revalidation must not physically delete retained candidates"
        );
        assert_eq!(
            lifecycle
                .revalidate(
                    run.id,
                    Duration::seconds(MIN_REVALIDATION_GRACE_SECONDS as i64)
                )
                .await
                .unwrap(),
            validated
        );
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
        let segment_pool = Path::from(".zerofs/segment-pool");
        for counter in 0..1_000u64 {
            let segment = Segid::new(7, counter);
            store
                .put(
                    &Path::from(format!("{segment_pool}/{}", segment.object_key())),
                    bytes::Bytes::from_static(b"r").into(),
                )
                .await
                .unwrap();
        }
        for index in 0..9_000u64 {
            let segment = Segid::new(8, index * 256);
            store
                .put(
                    &Path::from(format!("{segment_pool}/{}", segment.object_key())),
                    bytes::Bytes::from_static(b"q").into(),
                )
                .await
                .unwrap();
        }
        let now = catalog_timestamp(Utc::now());
        let run = GcRunRecord {
            id: Uuid::new_v4(),
            revision: 1,
            catalog_generation: 0,
            inventory_cutoff: now,
            root_digest: gc_root_digest(&pins).unwrap(),
            segment_pool: segment_pool.to_string(),
            roots: pins,
            mark_shards: Vec::new(),
            mark_stats: None,
            quarantine_shards: Vec::new(),
            inventory_stats: None,
            phase: GcRunPhase::Captured,
            quarantine_at: None,
            revalidation: None,
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
            SlateDbRootStore::new(store, Path::from("gc-mark-branches"))
                .with_segment_pool_root(segment_pool.clone()),
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
        let newer = Segid::new(9, 1);
        artifact_store
            .put(
                &Path::from(format!("{segment_pool}/{}", newer.object_key())),
                bytes::Bytes::from_static(b"n").into(),
            )
            .await
            .unwrap();
        let quarantined = lifecycle.quarantine(run.id).await.unwrap();
        assert_eq!(quarantined.phase, GcRunPhase::Quarantined);
        assert_eq!(quarantined.revision, 3);
        assert_eq!(quarantined.quarantine_shards.len(), 256);
        let inventory = quarantined.inventory_stats.as_ref().unwrap();
        assert_eq!(inventory.objects_seen, 10_001);
        assert_eq!(inventory.objects_newer_than_cutoff, 1);
        assert_eq!(inventory.reachable_objects, 1_000);
        assert_eq!(inventory.candidate_objects, 9_000);
        assert_eq!(inventory.candidate_bytes, 9_000);
        assert_eq!(inventory.intermediate_runs, 257);
        assert_eq!(lifecycle.quarantine(run.id).await.unwrap(), quarantined);
        assert!(
            artifact_store
                .head(&Path::from(format!(
                    "{segment_pool}/{}",
                    Segid::new(8, 0).object_key()
                )))
                .await
                .is_ok(),
            "first observation must not delete candidates"
        );
        artifact_store
            .put(
                &Path::from(quarantined.quarantine_shards[0].location.clone()),
                bytes::Bytes::from_static(b"corrupt").into(),
            )
            .await
            .unwrap();
        assert!(matches!(
            lifecycle.quarantine(run.id).await,
            Err(RootCaptureLifecycleError::Inventory(_))
        ));
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
        let blockers = catalog.gc_blockers(run.id).await.unwrap();
        assert_eq!(blockers.len(), 1);
        assert_eq!(blockers[0].kind, GcBlockerKind::CorruptMetadata);
        assert_eq!(blockers[0].occurrences, 2);
        catalog.close().await.unwrap();
    }
}
