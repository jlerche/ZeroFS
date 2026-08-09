use super::gc_inventory::{GcInventoryError, GcInventoryStore};
use super::gc_mark::{GcMarkError, GcMarkStore};
use super::{
    Catalog, CatalogError, GcBlockerKind, GcDeletionProgress, GcDeletionPublication,
    GcInventoryStats, GcMarkStats, GcQuarantinePublication, GcReportPublication,
    GcRevalidationCapture, GcRevalidationPublication, GcRevalidationRecord, GcRevalidationStats,
    GcRootKind, GcRunPhase, GcRunRecord, ImmutableCheckpoint, RootStoreError, SlateDbRootStore,
    catalog_timestamp, gc_root_digest, validate_timestamp,
};
use chrono::{DateTime, Duration, Utc};
use futures::{StreamExt, stream};
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use uuid::Uuid;

const ROOT_VERIFY_CONCURRENCY: usize = 16;
/// Five-minute maximum lease + 30-second skew + one-minute propagation bound.
pub(crate) const MIN_REVALIDATION_GRACE_SECONDS: u64 = 390;
pub const DEFAULT_GC_DELETION_MIN_GRACE_SECONDS: u64 = 24 * 60 * 60;
pub(crate) const MAX_DELETE_BATCH_SIZE: u32 = 4_096;
pub(crate) const MAX_ARTIFACT_CLEANUP_OBJECTS: usize = 4_096;
const GC_ARTIFACT_PREFIX: &str = "__zerofs_gc";
static ACTIVE_GC_QUARANTINES: OnceLock<Mutex<BTreeMap<Uuid, i64>>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcDeletionPolicy {
    pub enabled: bool,
    pub batch_size: u32,
    /// Minimum separation the durable second observation must have required
    /// from the first quarantine. The production default is 24 hours and the
    /// invariant floor remains the lease/skew/propagation bound.
    pub minimum_revalidation_grace_seconds: u64,
}

impl Default for GcDeletionPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            batch_size: 64,
            minimum_revalidation_grace_seconds: DEFAULT_GC_DELETION_MIN_GRACE_SECONDS,
        }
    }
}

/// Process-local, rapidly revocable authority for physical global-GC deletes.
///
/// Per-call policy remains necessary, but can never override this default-off
/// kill switch. Clones share one atomic state so an operator control can stop
/// the next bounded batch without disrupting capture, marking, or reporting.
#[derive(Clone, Debug, Default)]
pub struct GcDeletionControl {
    enabled: Arc<AtomicBool>,
}

impl GcDeletionControl {
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcArtifactCleanupPolicy {
    pub enabled: bool,
    pub retention_seconds: u64,
    pub max_objects: usize,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GcArtifactCleanupReport {
    pub examined: u64,
    pub deleted: u64,
    pub already_absent: u64,
    pub retained_too_young: u64,
    pub has_more: bool,
}

#[derive(Clone)]
pub struct RootCaptureLifecycle {
    catalog: Arc<dyn Catalog>,
    roots: SlateDbRootStore,
    deletion_control: GcDeletionControl,
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
        Self {
            catalog,
            roots,
            deletion_control: GcDeletionControl::default(),
        }
    }

    pub fn with_deletion_control(mut self, deletion_control: GcDeletionControl) -> Self {
        self.deletion_control = deletion_control;
        self
    }

    async fn gc_run_for_phase(&self, run_id: Uuid) -> Result<Option<GcRunRecord>, CatalogError> {
        // `Invalid` from this read describes a malformed durable record, not a
        // caller policy error, so the shared catalog classifier may safely
        // expose it as corrupt metadata.
        match self.catalog.gc_run(run_id).await {
            Ok(run) => Ok(run),
            Err(error) => {
                record_gc_retained_on_error(classify_catalog_error(&error), 1);
                Err(error)
            }
        }
    }

    async fn snapshot_for_phase(&self) -> Result<super::CatalogSnapshot, CatalogError> {
        // Snapshot validation failures likewise describe authoritative state;
        // request validation happens before this helper is called.
        match self.catalog.snapshot().await {
            Ok(snapshot) => Ok(snapshot),
            Err(error) => {
                record_gc_retained_on_error(classify_catalog_error(&error), 1);
                Err(error)
            }
        }
    }

    /// Authenticate every root at one catalog generation, then durably pin the
    /// exact typed list. A concurrent catalog mutation fails the generation
    /// fence and leaves no partial run record.
    pub async fn begin(&self, run_id: Uuid) -> Result<GcRunRecord, RootCaptureLifecycleError> {
        let _phase_timer = GcPhaseTimer::start("capture");
        if run_id.is_nil() {
            return Err(CatalogError::Invalid("GC run UUID cannot be nil".to_string()).into());
        }
        let existing = match self.catalog.gc_run(run_id).await {
            Ok(existing) => existing,
            Err(error) => {
                record_gc_capture_abort(classify_catalog_error(&error));
                return Err(error.into());
            }
        };
        if let Some(existing) = existing {
            return Ok(existing);
        }
        let snapshot = match self.catalog.snapshot().await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                record_gc_capture_abort(classify_catalog_error(&error));
                return Err(error.into());
            }
        };
        if let Some(branch_id) = mutable_writer_branch(&snapshot) {
            record_gc_capture_abort(GcBlockerKind::LeaseUncertainty);
            return Err(CatalogError::WriterLeaseActive(branch_id).into());
        }
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
            if let Err(error) = result {
                let kind = classify_root_store_error(&error);
                record_gc_root_open_failure("capture", kind);
                record_gc_capture_abort(kind);
                return Err(error.into());
            }
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
            deletion: None,
            created_at: inventory_cutoff,
            updated_at: inventory_cutoff,
        };
        let result = self
            .catalog
            .begin_gc_run(snapshot.generation, run.clone())
            .await;
        if let Err(error) = result {
            let existing = match self.catalog.gc_run(run_id).await {
                Ok(existing) => existing,
                Err(reconcile_error) => {
                    record_gc_capture_abort(classify_catalog_error(&reconcile_error));
                    return Err(reconcile_error.into());
                }
            };
            if let Some(existing) = existing {
                if existing == run {
                    return Ok(existing);
                }
                record_gc_capture_abort(GcBlockerKind::GenerationChanged);
                return Err(CatalogError::OperationConflict(run_id.to_string()).into());
            }
            record_gc_capture_abort(classify_catalog_error(&error));
            return Err(error.into());
        }
        Ok(run)
    }

    /// Stream every captured checkpoint's extent pointers into bounded sorted
    /// runs, merge/deduplicate them, and publish 256 independently verifiable
    /// mark shards atomically in the authoritative run record.
    pub async fn mark(&self, run_id: Uuid) -> Result<GcRunRecord, RootCaptureLifecycleError> {
        let _phase_timer = GcPhaseTimer::start("mark");
        let run = self
            .gc_run_for_phase(run_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(run_id.to_string()))?;
        let store = GcMarkStore::new(self.roots.clone());
        if matches!(
            run.phase,
            GcRunPhase::Marking | GcRunPhase::Reported | GcRunPhase::Quarantined
        ) {
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
            Ok(run) => {
                record_gc_mark_metrics(&mark_stats);
                Ok(run)
            }
            Err(error) => {
                if let Some(existing) = self.gc_run_for_phase(run_id).await?
                    && existing.phase == GcRunPhase::Marking
                    && existing.root_digest == run.root_digest
                    && existing.mark_shards == mark_shards
                    && existing.mark_stats.as_ref() == Some(&mark_stats)
                {
                    let digest = decode_digest(&existing.root_digest)?;
                    store
                        .verify_all(existing.id, digest, &existing.mark_shards)
                        .await?;
                    record_gc_mark_metrics(&mark_stats);
                    return Ok(existing);
                }
                Err(error.into())
            }
        }
    }

    /// Build and publish a terminal mark/inventory report without entering
    /// quarantine. The immutable candidate shards remain available for audit,
    /// but the transition atomically releases the run's root pins and has no
    /// path to revalidation or physical deletion.
    pub async fn report(&self, run_id: Uuid) -> Result<GcRunRecord, RootCaptureLifecycleError> {
        let _phase_timer = GcPhaseTimer::start("inventory_report");
        let run = self
            .gc_run_for_phase(run_id)
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
        if run.phase == GcRunPhase::Reported {
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
        let reported_at = catalog_timestamp(Utc::now());
        let published = self
            .catalog
            .publish_gc_report(GcReportPublication {
                id: run.id,
                expected_revision: run.revision,
                expected_generation: run.catalog_generation,
                root_digest: run.root_digest.clone(),
                candidate_shards: build.shards.clone(),
                inventory_stats: build.stats.clone(),
                reported_at,
            })
            .await;
        match published {
            Ok(run) => {
                record_gc_report_metrics(&build.stats);
                Ok(run)
            }
            Err(error) => {
                if let Some(existing) = self.gc_run_for_phase(run_id).await?
                    && existing.phase == GcRunPhase::Reported
                    && existing.root_digest == run.root_digest
                    && existing.quarantine_shards == build.shards
                    && existing.inventory_stats.as_ref() == Some(&build.stats)
                    && existing.quarantine_at == Some(reported_at)
                {
                    inventory
                        .verify_all(&existing, digest, &existing.quarantine_shards)
                        .await?;
                    record_gc_report_metrics(&build.stats);
                    return Ok(existing);
                }
                if self
                    .snapshot_for_phase()
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

    /// Stream the physical segment pool, exclude objects newer than the
    /// captured cutoff, merge-join it against the authoritative mark shards,
    /// and publish a durable first unreachable observation. No segment is
    /// physically deleted by this transition.
    pub async fn quarantine(&self, run_id: Uuid) -> Result<GcRunRecord, RootCaptureLifecycleError> {
        let _phase_timer = GcPhaseTimer::start("inventory_quarantine");
        let run = self
            .gc_run_for_phase(run_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(run_id.to_string()))?;
        record_gc_quarantine_state(&run);
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
            Ok(run) => {
                record_gc_quarantine_state(&run);
                record_gc_inventory_metrics(&build.stats);
                Ok(run)
            }
            Err(error) => {
                if let Some(existing) = self.gc_run_for_phase(run_id).await?
                    && existing.phase == GcRunPhase::Quarantined
                    && existing.root_digest == run.root_digest
                    && existing.quarantine_shards == build.shards
                    && existing.inventory_stats.as_ref() == Some(&build.stats)
                    && existing.quarantine_at == Some(quarantine_at)
                {
                    inventory
                        .verify_all(&existing, digest, &existing.quarantine_shards)
                        .await?;
                    record_gc_quarantine_state(&existing);
                    record_gc_inventory_metrics(&build.stats);
                    return Ok(existing);
                }
                if self
                    .snapshot_for_phase()
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
        let _phase_timer = GcPhaseTimer::start("revalidate");
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
            .gc_run_for_phase(run_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(run_id.to_string()))?;
        record_gc_quarantine_state(&run);
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
            let snapshot = self.snapshot_for_phase().await?;
            if let Some(branch_id) = mutable_writer_branch(&snapshot) {
                self.record_blocker(
                    run_id,
                    GcBlockerKind::LeaseUncertainty,
                    format!("branch {branch_id} has an unreconciled mutable writer"),
                )
                .await;
                return Err(CatalogError::WriterLeaseActive(branch_id).into());
            }
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
                if let Err(error) = result {
                    let kind = classify_root_store_error(&error);
                    record_gc_root_open_failure("revalidate", kind);
                    self.record_blocker(run_id, kind, error.to_string()).await;
                    return Err(error.into());
                }
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
                    if let Some(existing) = self.gc_run_for_phase(run_id).await?
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
            Ok(run) => {
                record_gc_revalidation_metrics(&build.stats, &candidates.stats);
                Ok(run)
            }
            Err(error) => {
                if let Some(existing) = self.gc_run_for_phase(run_id).await?
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
                    record_gc_revalidation_metrics(&build.stats, &candidates.stats);
                    return Ok(existing);
                }
                if self
                    .snapshot_for_phase()
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

    /// Delete at most one explicitly enabled, bounded batch and durably publish
    /// its next cursor. Repeated calls resume from catalog authority.
    pub async fn delete_batch(
        &self,
        run_id: Uuid,
        policy: GcDeletionPolicy,
    ) -> Result<GcRunRecord, RootCaptureLifecycleError> {
        let _phase_timer = GcPhaseTimer::start("delete");
        if !policy.enabled {
            return Err(CatalogError::Invalid(
                "physical GC deletion is disabled by policy".to_string(),
            )
            .into());
        }
        if policy.batch_size == 0 || policy.batch_size > MAX_DELETE_BATCH_SIZE {
            return Err(CatalogError::Invalid(format!(
                "GC delete batch size must be between 1 and {MAX_DELETE_BATCH_SIZE}"
            ))
            .into());
        }
        if policy.minimum_revalidation_grace_seconds < MIN_REVALIDATION_GRACE_SECONDS {
            return Err(CatalogError::Invalid(format!(
                "GC delete minimum revalidation grace must be at least {MIN_REVALIDATION_GRACE_SECONDS} seconds"
            ))
            .into());
        }
        let mut run = self
            .gc_run_for_phase(run_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(run_id.to_string()))?;
        record_gc_quarantine_state(&run);
        if run.phase == GcRunPhase::Completed {
            return Ok(run);
        }
        if !self.deletion_control.is_enabled() {
            return Err(CatalogError::Invalid(
                "physical GC deletion is disabled by the rapid kill switch".to_string(),
            )
            .into());
        }
        let observation = run.revalidation.clone().ok_or_else(|| {
            CatalogError::OperationConflict(format!("{run_id}: revalidation is incomplete"))
        })?;
        if observation.grace_seconds < policy.minimum_revalidation_grace_seconds {
            return Err(CatalogError::OperationConflict(format!(
                "GC run {run_id} used {} seconds of revalidation grace, below deletion policy minimum {}",
                observation.grace_seconds, policy.minimum_revalidation_grace_seconds
            ))
            .into());
        }
        // Valid root publication is forward-closed: it can inherit only from
        // roots captured at H or allocate a globally fresh pool segment ID.
        // Thus a post-H mutation cannot resurrect one of H's unreachable IDs;
        // the preflight still rejects known drift conservatively.
        let current_generation = self.snapshot_for_phase().await?.generation;
        if current_generation != observation.catalog_generation {
            self.record_blocker(
                run_id,
                GcBlockerKind::GenerationChanged,
                format!(
                    "catalog generation changed after second observation at {}",
                    observation.catalog_generation
                ),
            )
            .await;
            return Err(CatalogError::RevisionConflict {
                expected: observation.catalog_generation,
                actual: current_generation,
            }
            .into());
        }
        let inventory = GcInventoryStore::new(&self.roots);
        if let Err(error) = inventory.verify_revalidation(&observation).await {
            self.record_blocker(run_id, classify_inventory_error(&error), error.to_string())
                .await;
            return Err(error.into());
        }
        if run.phase == GcRunPhase::Validated {
            let started_at = catalog_timestamp(Utc::now()).max(run.updated_at);
            let initial = GcDeletionProgress {
                batch_size: policy.batch_size,
                next_shard: 0,
                next_record: 0,
                deleted_objects: 0,
                deleted_bytes: 0,
                already_absent: 0,
                started_at,
                completed_at: None,
            };
            run = self
                .catalog
                .publish_gc_deletion(GcDeletionPublication {
                    run_id,
                    expected_revision: run.revision,
                    expected_generation: observation.catalog_generation,
                    progress: initial,
                    updated_at: started_at,
                })
                .await?;
        }
        if run.phase != GcRunPhase::Deleting {
            return Err(CatalogError::OperationConflict(run_id.to_string()).into());
        }
        let previous = run
            .deletion
            .clone()
            .ok_or_else(|| CatalogError::Corrupt("deleting GC run has no progress".to_string()))?;
        if previous.batch_size != policy.batch_size {
            return Err(CatalogError::OperationConflict(run_id.to_string()).into());
        }
        let batch = match inventory
            .delete_batch(
                &run.segment_pool,
                &observation,
                previous.next_shard,
                previous.next_record,
                policy.batch_size,
            )
            .await
        {
            Ok(batch) => batch,
            Err(error) => {
                self.record_blocker(run_id, classify_inventory_error(&error), error.to_string())
                    .await;
                return Err(error.into());
            }
        };
        let updated_at = catalog_timestamp(Utc::now()).max(run.updated_at);
        let completed_at = (batch.next_shard == 256).then_some(updated_at);
        let progress = GcDeletionProgress {
            batch_size: previous.batch_size,
            next_shard: batch.next_shard,
            next_record: batch.next_record,
            deleted_objects: previous
                .deleted_objects
                .checked_add(batch.deleted_objects)
                .ok_or_else(|| CatalogError::Corrupt("GC deleted count overflow".to_string()))?,
            deleted_bytes: previous
                .deleted_bytes
                .checked_add(batch.deleted_bytes)
                .ok_or_else(|| CatalogError::Corrupt("GC deleted bytes overflow".to_string()))?,
            already_absent: previous
                .already_absent
                .checked_add(batch.already_absent)
                .ok_or_else(|| CatalogError::Corrupt("GC absent count overflow".to_string()))?,
            started_at: previous.started_at,
            completed_at,
        };
        let expected_progress = progress.clone();
        let published = self
            .catalog
            .publish_gc_deletion(GcDeletionPublication {
                run_id,
                expected_revision: run.revision,
                expected_generation: observation.catalog_generation,
                progress,
                updated_at,
            })
            .await;
        let published = match published {
            Ok(published) => published,
            Err(error) => {
                if let Some(existing) = self.gc_run_for_phase(run_id).await?
                    && existing.deletion.as_ref() == Some(&expected_progress)
                    && matches!(existing.phase, GcRunPhase::Deleting | GcRunPhase::Completed)
                {
                    existing
                } else {
                    return Err(error.into());
                }
            }
        };
        metrics::counter!("zerofs_gc_reclaimed_objects_total").increment(batch.deleted_objects);
        metrics::counter!("zerofs_gc_reclaimed_bytes_total").increment(batch.deleted_bytes);
        metrics::counter!("zerofs_gc_backlog_upper_bound_bytes_resolved_total")
            .increment(batch.deleted_bytes);
        record_gc_quarantine_state(&published);
        Ok(published)
    }

    /// Delete a bounded batch from one terminal run's isolated artifact
    /// namespace after its retention period. Relisting the shrinking prefix is
    /// the durable cursor: crashes and ambiguous already-absent deletes resume
    /// without catalog-side per-object state.
    pub async fn cleanup_artifacts(
        &self,
        run_id: Uuid,
        policy: GcArtifactCleanupPolicy,
    ) -> Result<GcArtifactCleanupReport, RootCaptureLifecycleError> {
        let (retention_duration, object_cutoff) = validate_cleanup_policy(policy)?;
        let run = self
            .gc_run_for_phase(run_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(run_id.to_string()))?;
        run.validate()?;
        let completed_at = match run.phase {
            GcRunPhase::Reported => run.updated_at,
            GcRunPhase::Completed => run
                .deletion
                .as_ref()
                .and_then(|progress| progress.completed_at)
                .ok_or_else(|| {
                    CatalogError::Corrupt(format!(
                        "completed GC run {run_id} has no completion timestamp"
                    ))
                })?,
            _ => {
                return Err(CatalogError::OperationConflict(format!(
                    "GC run {run_id} is not terminal"
                ))
                .into());
            }
        };
        let retain_until = completed_at
            .checked_add_signed(retention_duration)
            .ok_or_else(|| {
                CatalogError::Invalid("GC artifact retention deadline overflows".to_string())
            })?;
        if policy.observed_at < retain_until {
            return Err(CatalogError::OperationConflict(format!(
                "GC run {run_id} artifacts are retained until {retain_until}"
            ))
            .into());
        }
        let mut prefixes = vec![object_store::path::Path::from(format!(
            "{GC_ARTIFACT_PREFIX}/{run_id}"
        ))];
        if let Some(observation) = &run.revalidation
            && observation.id != run_id
        {
            prefixes.push(object_store::path::Path::from(format!(
                "{GC_ARTIFACT_PREFIX}/{}",
                observation.id
            )));
        }
        let report = cleanup_artifact_prefixes(
            self.roots.object_store(),
            &prefixes,
            object_cutoff,
            policy.max_objects,
        )
        .await?;
        super::record_cleanup_metrics(
            "completed_gc_artifacts",
            report.examined,
            report.deleted,
            report.already_absent,
            report.retained_too_young,
            u64::from(report.has_more),
        );
        Ok(report)
    }

    /// Delete bounded, phase-obsolete build artifacts while retaining every
    /// shard still needed to retry or complete the active run. Terminal runs
    /// use [`Self::cleanup_artifacts`].
    pub async fn cleanup_obsolete_artifacts(
        &self,
        run_id: Uuid,
        policy: GcArtifactCleanupPolicy,
    ) -> Result<GcArtifactCleanupReport, RootCaptureLifecycleError> {
        let (retention_duration, object_cutoff) = validate_cleanup_policy(policy)?;
        let run = self
            .gc_run_for_phase(run_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(run_id.to_string()))?;
        run.validate()?;
        if matches!(run.phase, GcRunPhase::Reported | GcRunPhase::Completed) {
            return Err(CatalogError::OperationConflict(format!(
                "GC run {run_id} is terminal; use terminal artifact cleanup"
            ))
            .into());
        }
        let retain_until = run
            .updated_at
            .checked_add_signed(retention_duration)
            .ok_or_else(|| {
                CatalogError::Invalid("GC artifact retention deadline overflows".to_string())
            })?;
        if policy.observed_at < retain_until {
            return Err(CatalogError::OperationConflict(format!(
                "GC run {run_id} phase artifacts are retained until {retain_until}"
            ))
            .into());
        }

        let parent = format!("{GC_ARTIFACT_PREFIX}/{run_id}");
        let mut prefixes = Vec::new();
        if matches!(
            run.phase,
            GcRunPhase::Marking
                | GcRunPhase::Quarantined
                | GcRunPhase::Revalidating
                | GcRunPhase::Validated
                | GcRunPhase::Deleting
        ) {
            for class in ["mark-runs", "mark-merge", "mark-online"] {
                prefixes.push(object_store::path::Path::from(format!("{parent}/{class}")));
            }
        }
        if matches!(
            run.phase,
            GcRunPhase::Quarantined
                | GcRunPhase::Revalidating
                | GcRunPhase::Validated
                | GcRunPhase::Deleting
        ) {
            for class in ["inventory-runs", "inventory-online", "inventory"] {
                prefixes.push(object_store::path::Path::from(format!("{parent}/{class}")));
            }
        }
        if matches!(run.phase, GcRunPhase::Validated | GcRunPhase::Deleting) {
            for class in ["marks", "quarantine"] {
                prefixes.push(object_store::path::Path::from(format!("{parent}/{class}")));
            }
            let observation = run.revalidation.as_ref().ok_or_else(|| {
                CatalogError::Corrupt("validated GC run has no revalidation".to_string())
            })?;
            let observation_parent = format!("{GC_ARTIFACT_PREFIX}/{}", observation.id);
            for class in ["mark-runs", "mark-merge", "mark-online"] {
                prefixes.push(object_store::path::Path::from(format!(
                    "{observation_parent}/{class}"
                )));
            }
        }

        let report = cleanup_artifact_prefixes(
            self.roots.object_store(),
            &prefixes,
            object_cutoff,
            policy.max_objects,
        )
        .await?;
        super::record_cleanup_metrics(
            "obsolete_gc_artifacts",
            report.examined,
            report.deleted,
            report.already_absent,
            report.retained_too_young,
            u64::from(report.has_more),
        );
        Ok(report)
    }

    async fn record_blocker(&self, run_id: Uuid, kind: GcBlockerKind, mut detail: String) {
        record_gc_retained_on_error(kind, 1);
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

struct GcPhaseTimer {
    phase: &'static str,
    started: Instant,
}

impl GcPhaseTimer {
    fn start(phase: &'static str) -> Self {
        metrics::gauge!("zerofs_gc_phase_active", "phase" => phase).increment(1.0);
        Self {
            phase,
            started: Instant::now(),
        }
    }
}

impl Drop for GcPhaseTimer {
    fn drop(&mut self) {
        metrics::histogram!("zerofs_gc_phase_duration_seconds", "phase" => self.phase)
            .record(self.started.elapsed().as_secs_f64());
        metrics::gauge!("zerofs_gc_phase_active", "phase" => self.phase).decrement(1.0);
    }
}

fn record_gc_mark_metrics(stats: &GcMarkStats) {
    metrics::counter!("zerofs_gc_roots_scanned_total").increment(stats.roots_enumerated);
    metrics::counter!("zerofs_gc_references_scanned_total").increment(stats.references_enumerated);
    metrics::counter!("zerofs_gc_unique_segments_marked_total").increment(stats.unique_segments);
}

fn record_gc_inventory_metrics(stats: &GcInventoryStats) {
    metrics::counter!("zerofs_gc_inventory_objects_total").increment(stats.objects_seen);
    metrics::counter!("zerofs_gc_quarantined_objects_total").increment(stats.candidate_objects);
    metrics::counter!("zerofs_gc_quarantined_bytes_total").increment(stats.candidate_bytes);
    metrics::counter!("zerofs_gc_backlog_upper_bound_bytes_added_total")
        .increment(stats.candidate_bytes);
}

fn record_gc_report_metrics(stats: &GcInventoryStats) {
    metrics::counter!("zerofs_gc_inventory_objects_total").increment(stats.objects_seen);
    metrics::counter!("zerofs_gc_reported_candidate_objects_total")
        .increment(stats.candidate_objects);
    metrics::counter!("zerofs_gc_reported_candidate_bytes_total").increment(stats.candidate_bytes);
}

fn record_gc_revalidation_metrics(mark: &GcMarkStats, stats: &GcRevalidationStats) {
    record_gc_mark_metrics(mark);
    metrics::counter!("zerofs_gc_candidates_became_reachable_total")
        .increment(stats.became_reachable);
    metrics::counter!("zerofs_gc_candidates_already_absent_total").increment(stats.already_absent);
}

fn record_gc_capture_abort(kind: GcBlockerKind) {
    record_gc_retained_on_error(kind, 1);
    metrics::counter!("zerofs_gc_aborted_runs_total", "phase" => "capture").increment(1);
}

fn record_gc_root_open_failure(phase: &'static str, kind: GcBlockerKind) {
    metrics::counter!(
        "zerofs_gc_root_open_failures_total",
        "phase" => phase,
        "reason" => blocker_metric_label(kind)
    )
    .increment(1);
}

fn record_gc_quarantine_state(run: &GcRunRecord) {
    let timestamp = if matches!(
        run.phase,
        GcRunPhase::Reported | GcRunPhase::Completed | GcRunPhase::Aborted
    ) {
        None
    } else {
        run.quarantine_at.map(|value| value.timestamp())
    };
    let oldest = update_gc_quarantine_tracking(run.id, timestamp);
    metrics::gauge!("zerofs_gc_oldest_quarantine_timestamp_seconds").set(oldest as f64);
}

fn update_gc_quarantine_tracking(run_id: Uuid, timestamp: Option<i64>) -> i64 {
    let quarantines = ACTIVE_GC_QUARANTINES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut quarantines = quarantines
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(timestamp) = timestamp {
        quarantines.insert(run_id, timestamp);
    } else {
        quarantines.remove(&run_id);
    }
    quarantines.values().copied().min().unwrap_or_default()
}

fn record_gc_retained_on_error(kind: GcBlockerKind, lower_bound: u64) {
    let kind = blocker_metric_label(kind);
    metrics::counter!("zerofs_gc_retained_on_error_lower_bound_total", "reason" => kind)
        .increment(lower_bound);
    metrics::counter!("zerofs_gc_aborted_phase_attempts_total", "reason" => kind).increment(1);
}

fn blocker_metric_label(kind: GcBlockerKind) -> &'static str {
    match kind {
        GcBlockerKind::MissingRoot => "missing_root",
        GcBlockerKind::CorruptMetadata => "corrupt_metadata",
        GcBlockerKind::GenerationChanged => "generation_changed",
        GcBlockerKind::LeaseUncertainty => "lease_uncertainty",
        GcBlockerKind::StorageUnavailable => "storage_unavailable",
    }
}

fn validate_cleanup_policy(
    policy: GcArtifactCleanupPolicy,
) -> Result<(Duration, DateTime<Utc>), RootCaptureLifecycleError> {
    validate_timestamp(policy.observed_at, "GC artifact cleanup observation")?;
    if !policy.enabled {
        return Err(
            CatalogError::Invalid("GC artifact cleanup is disabled by policy".to_string()).into(),
        );
    }
    if policy.max_objects == 0 || policy.max_objects > MAX_ARTIFACT_CLEANUP_OBJECTS {
        return Err(CatalogError::Invalid(format!(
            "GC artifact cleanup batch must be between 1 and {MAX_ARTIFACT_CLEANUP_OBJECTS}"
        ))
        .into());
    }
    let retention = i64::try_from(policy.retention_seconds)
        .map_err(|_| CatalogError::Invalid("GC artifact retention is too large".to_string()))?;
    let retention_duration = Duration::try_seconds(retention)
        .ok_or_else(|| CatalogError::Invalid("GC artifact retention is too large".to_string()))?;
    let object_cutoff = policy
        .observed_at
        .checked_sub_signed(retention_duration)
        .ok_or_else(|| {
            CatalogError::Invalid("GC artifact retention cutoff underflows".to_string())
        })?;
    Ok((retention_duration, object_cutoff))
}

async fn cleanup_artifact_prefixes(
    store: Arc<dyn ObjectStore>,
    prefixes: &[object_store::path::Path],
    object_cutoff: DateTime<Utc>,
    max_objects: usize,
) -> Result<GcArtifactCleanupReport, RootCaptureLifecycleError> {
    let mut report = GcArtifactCleanupReport::default();
    for prefix in prefixes {
        if report.examined == max_objects as u64 {
            report.has_more = true;
            return Ok(report);
        }
        let mut listing = store.list(Some(prefix));
        while report.examined < max_objects as u64 {
            let Some(next) = listing.next().await else {
                break;
            };
            let meta =
                next.map_err(|error| RootCaptureLifecycleError::Artifact(error.to_string()))?;
            report.examined += 1;
            if meta.last_modified > object_cutoff {
                report.retained_too_young += 1;
                continue;
            }
            let delete_result = store.delete(&meta.location).await;
            match store.head(&meta.location).await {
                Err(object_store::Error::NotFound { .. }) => match delete_result {
                    Ok(_) => report.deleted += 1,
                    Err(_) => report.already_absent += 1,
                },
                Ok(_) => {
                    return Err(RootCaptureLifecycleError::Artifact(format!(
                        "artifact {} remains after delete",
                        meta.location
                    )));
                }
                Err(error) => {
                    return Err(RootCaptureLifecycleError::Artifact(format!(
                        "artifact {} absence confirmation failed: {error}",
                        meta.location
                    )));
                }
            }
        }
    }
    report.has_more = report.retained_too_young != 0 || report.examined == max_objects as u64;
    Ok(report)
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

fn classify_root_store_error(error: &RootStoreError) -> GcBlockerKind {
    match error {
        RootStoreError::MissingOwner(_)
        | RootStoreError::MissingResult(_)
        | RootStoreError::MissingSourceCheckpoint(_)
        | RootStoreError::MissingManifest(_)
        | RootStoreError::MissingRootCheckpoint(_)
        | RootStoreError::MissingExternalPin { .. }
        | RootStoreError::MissingSst(_) => GcBlockerKind::MissingRoot,
        RootStoreError::ObjectStore(_) | RootStoreError::SlateDb(_) => {
            GcBlockerKind::StorageUnavailable
        }
        RootStoreError::Invalid(_)
        | RootStoreError::UnownedDestination(_)
        | RootStoreError::OwnershipConflict(_)
        | RootStoreError::SourceManifestMismatch { .. }
        | RootStoreError::SourceCheckpointNameMismatch { .. }
        | RootStoreError::MissingSourceCheckpointName(_)
        | RootStoreError::DuplicateSourceCheckpointName(_)
        | RootStoreError::SourceCheckpointNameIdentityMismatch { .. }
        | RootStoreError::ExpiringSourceCheckpoint(_)
        | RootStoreError::SourceCheckpointCreateTimeMismatch { .. }
        | RootStoreError::WrongSource { .. }
        | RootStoreError::NonCanonicalRoot(_)
        | RootStoreError::StaleWriterIncarnation { .. }
        | RootStoreError::WriterIncarnationMismatch { .. }
        | RootStoreError::RootManifestMismatch { .. }
        | RootStoreError::WalDependency { .. }
        | RootStoreError::Uninitialized(_)
        | RootStoreError::ExternalPinCoverage { .. }
        | RootStoreError::Clone(_)
        | RootStoreError::Json(_) => GcBlockerKind::CorruptMetadata,
    }
}

fn classify_catalog_error(error: &CatalogError) -> GcBlockerKind {
    match error {
        CatalogError::WriterLeaseActive(_) => GcBlockerKind::LeaseUncertainty,
        CatalogError::OperationConflict(_)
        | CatalogError::RevisionConflict { .. }
        | CatalogError::AlreadyExists(_)
        | CatalogError::Capacity { .. } => GcBlockerKind::GenerationChanged,
        CatalogError::NotFound(_) => GcBlockerKind::MissingRoot,
        CatalogError::Invalid(_) | CatalogError::Corrupt(_) | CatalogError::Json(_) => {
            GcBlockerKind::CorruptMetadata
        }
        CatalogError::Io(_)
        | CatalogError::SlateDb(_)
        | CatalogError::Postgres(_)
        | CatalogError::PostgresTls(_) => GcBlockerKind::StorageUnavailable,
    }
}

fn mutable_writer_branch(snapshot: &super::CatalogSnapshot) -> Option<Uuid> {
    snapshot
        .leases
        .values()
        .find(|lease| lease.access_mode == super::LeaseAccessMode::Write)
        .map(|lease| lease.subject_id)
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
    #[error("GC artifact cleanup failed: {0}")]
    Artifact(String),
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
    use crate::fault_store::FaultStore;
    use crate::fs::key_codec::KeyCodec;
    use crate::segment::{FrameLoc, Segid};
    use object_store::{ObjectStore, ObjectStoreExt};
    use slatedb::config::{CheckpointOptions, CheckpointScope};
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;
    use slatedb::{Db, WriteBatch};

    #[derive(Debug)]
    struct GcPipelineProbe {
        roots: u64,
        references: u64,
        inventory_objects: u64,
        mark_gets: u64,
        mark_puts: u64,
        mark_lists: u64,
        mark_get_bytes: u64,
        mark_put_bytes: u64,
        mark_listed_objects: u64,
        mark_listed_object_bytes: u64,
        mark_multipart_initiates: u64,
        mark_multipart_parts: u64,
        mark_multipart_completes: u64,
        mark_multipart_bytes: u64,
        report_gets: u64,
        report_puts: u64,
        report_lists: u64,
        report_get_bytes: u64,
        report_put_bytes: u64,
        report_listed_objects: u64,
        report_listed_object_bytes: u64,
        report_multipart_initiates: u64,
        report_multipart_parts: u64,
        report_multipart_completes: u64,
        report_multipart_bytes: u64,
        mark_ms: u128,
        report_ms: u128,
    }

    async fn gc_pipeline_probe(
        label: &str,
        root_count: usize,
        reference_count: usize,
        inventory_count: usize,
        reference_counter_stride: u64,
    ) -> GcPipelineProbe {
        let (counting, counters) = FaultStore::new(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = counting;
        let data_path = Path::from(format!("gc-scale/{label}/data"));
        let db = Db::builder(data_path.clone(), Arc::clone(&store))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        let codec = KeyCodec::new();
        let mut batch = WriteBatch::new();
        for index in 0..reference_count as u64 {
            batch.put(
                codec.extent_key(1, index),
                FrameLoc {
                    segid: Segid::new(7, index * reference_counter_stride),
                    frame_index: 0,
                    byte_offset: 0,
                    byte_len: 1,
                }
                .encode(),
            );
        }
        db.write(batch).await.unwrap();
        db.flush().await.unwrap();
        let mut pins = Vec::with_capacity(root_count);
        for _ in 0..root_count {
            let checkpoint = db
                .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
                .await
                .unwrap();
            pins.push(GcRootPin {
                kind: GcRootKind::Checkpoint,
                root: ImmutableCheckpoint {
                    database_path: data_path.clone(),
                    checkpoint_id: checkpoint.id,
                    manifest_id: checkpoint.manifest_id,
                }
                .durable_root(),
            });
        }
        pins.sort_by(|left, right| {
            (&left.root.identity, &left.root.manifest_id)
                .cmp(&(&right.root.identity, &right.root.manifest_id))
        });
        pins.dedup();
        db.close().await.unwrap();

        let segment_pool = Path::from(format!("gc-scale/{label}/segment-pool"));
        futures::stream::iter(0..inventory_count as u64)
            .map(|index| {
                let store = Arc::clone(&store);
                let path = Path::from(format!(
                    "{segment_pool}/{}",
                    Segid::new(8, index).object_key()
                ));
                async move {
                    store
                        .put(&path, bytes::Bytes::from_static(b"candidate").into())
                        .await
                        .unwrap();
                }
            })
            .buffer_unordered(64)
            .collect::<Vec<_>>()
            .await;

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
            deletion: None,
            created_at: now,
            updated_at: now,
        };
        let catalog = Arc::new(
            SlateDbCatalog::open(
                Path::from(format!("gc-scale/{label}/catalog")),
                Arc::clone(&store),
            )
            .await
            .unwrap(),
        );
        catalog.begin_gc_run(0, run.clone()).await.unwrap();
        let lifecycle = RootCaptureLifecycle::new(
            catalog.clone(),
            SlateDbRootStore::new(
                Arc::clone(&store),
                Path::from(format!("gc-scale/{label}/branches")),
            )
            .with_segment_pool_root(segment_pool),
        );

        counters.reset_counts();
        let mark_started = Instant::now();
        let marked = lifecycle.mark(run.id).await.unwrap();
        let mark_ms = mark_started.elapsed().as_millis();
        let mark_gets = counters.get_count() as u64;
        let mark_puts = counters.put_count() as u64;
        let mark_lists = counters.list_count() as u64;
        let mark_get_bytes = counters.get_bytes();
        let mark_put_bytes = counters.put_bytes();
        let mark_listed_objects = counters.listed_objects();
        let mark_listed_object_bytes = counters.listed_object_bytes();
        let mark_multipart_initiates = counters.multipart_initiate_count() as u64;
        let mark_multipart_parts = counters.multipart_part_count() as u64;
        let mark_multipart_completes = counters.multipart_complete_count() as u64;
        let mark_multipart_bytes = counters.multipart_bytes();

        counters.reset_counts();
        let report_started = Instant::now();
        let reported = lifecycle.report(run.id).await.unwrap();
        let report_ms = report_started.elapsed().as_millis();
        let report_gets = counters.get_count() as u64;
        let report_puts = counters.put_count() as u64;
        let report_lists = counters.list_count() as u64;
        let report_get_bytes = counters.get_bytes();
        let report_put_bytes = counters.put_bytes();
        let report_listed_objects = counters.listed_objects();
        let report_listed_object_bytes = counters.listed_object_bytes();
        let report_multipart_initiates = counters.multipart_initiate_count() as u64;
        let report_multipart_parts = counters.multipart_part_count() as u64;
        let report_multipart_completes = counters.multipart_complete_count() as u64;
        let report_multipart_bytes = counters.multipart_bytes();
        let mark = marked.mark_stats.unwrap();
        let inventory = reported.inventory_stats.unwrap();
        catalog.close().await.unwrap();
        GcPipelineProbe {
            roots: mark.roots_enumerated,
            references: mark.references_enumerated,
            inventory_objects: inventory.objects_seen,
            mark_gets,
            mark_puts,
            mark_lists,
            mark_get_bytes,
            mark_put_bytes,
            mark_listed_objects,
            mark_listed_object_bytes,
            mark_multipart_initiates,
            mark_multipart_parts,
            mark_multipart_completes,
            mark_multipart_bytes,
            report_gets,
            report_puts,
            report_lists,
            report_get_bytes,
            report_put_bytes,
            report_listed_objects,
            report_listed_object_bytes,
            report_multipart_initiates,
            report_multipart_parts,
            report_multipart_completes,
            report_multipart_bytes,
            mark_ms,
            report_ms,
        }
    }

    /// Release-mode object-store work characterization. Each pair doubles one
    /// independent dimension while retaining the others. The JSON result is
    /// evidence for selecting and verifying a derived production work bound;
    /// it deliberately makes no linearity claim by itself.
    #[tokio::test]
    #[ignore = "release-mode GC external-work characterization"]
    async fn gc_external_work_amplification_probe() {
        let roots_small = gc_pipeline_probe("roots-small", 2, 1_024, 1, 1).await;
        let roots_large = gc_pipeline_probe("roots-large", 4, 1_024, 1, 1).await;
        assert_eq!(roots_large.roots, roots_small.roots * 2);
        assert_eq!(roots_large.references, roots_small.references * 2);

        let references_small = gc_pipeline_probe("references-small", 1, 20_000, 1, 256).await;
        let references_large = gc_pipeline_probe("references-large", 1, 40_000, 1, 256).await;
        assert_eq!(references_large.roots, references_small.roots);
        assert_eq!(references_large.references, references_small.references * 2);
        assert!(references_small.mark_multipart_parts > 0);
        assert!(references_large.mark_multipart_parts > 0);

        let inventory_small = gc_pipeline_probe("inventory-small", 1, 1, 2_048, 1).await;
        let inventory_large = gc_pipeline_probe("inventory-large", 1, 1, 4_096, 1).await;
        assert_eq!(
            inventory_large.inventory_objects,
            inventory_small.inventory_objects * 2
        );
        assert_eq!(
            inventory_large.report_listed_objects,
            inventory_small.report_listed_objects * 2
        );
        assert_eq!(
            inventory_large.report_listed_object_bytes,
            inventory_small.report_listed_object_bytes * 2
        );

        let render = |probe: &GcPipelineProbe| {
            serde_json::json!({
                "roots": probe.roots,
                "references": probe.references,
                "inventory_objects": probe.inventory_objects,
                "mark": {
                    "ms": probe.mark_ms,
                    "gets": probe.mark_gets,
                    "puts": probe.mark_puts,
                    "lists": probe.mark_lists,
                    "get_bytes": probe.mark_get_bytes,
                    "put_bytes": probe.mark_put_bytes,
                    "listed_objects": probe.mark_listed_objects,
                    "listed_object_bytes": probe.mark_listed_object_bytes,
                    "multipart_initiates": probe.mark_multipart_initiates,
                    "multipart_parts": probe.mark_multipart_parts,
                    "multipart_completes": probe.mark_multipart_completes,
                    "multipart_bytes": probe.mark_multipart_bytes,
                },
                "report": {
                    "ms": probe.report_ms,
                    "gets": probe.report_gets,
                    "puts": probe.report_puts,
                    "lists": probe.report_lists,
                    "get_bytes": probe.report_get_bytes,
                    "put_bytes": probe.report_put_bytes,
                    "listed_objects": probe.report_listed_objects,
                    "listed_object_bytes": probe.report_listed_object_bytes,
                    "multipart_initiates": probe.report_multipart_initiates,
                    "multipart_parts": probe.report_multipart_parts,
                    "multipart_completes": probe.report_multipart_completes,
                    "multipart_bytes": probe.report_multipart_bytes,
                },
            })
        };
        println!(
            "{}",
            serde_json::json!({
                "roots_small": render(&roots_small),
                "roots_large": render(&roots_large),
                "references_small": render(&references_small),
                "references_large": render(&references_large),
                "inventory_small": render(&inventory_small),
                "inventory_large": render(&inventory_large),
            })
        );
    }

    async fn reopen_gc_lifecycle(
        catalog: Arc<SlateDbCatalog>,
        store: Arc<dyn ObjectStore>,
        catalog_path: &Path,
        branch_root: &Path,
        segment_pool: &Path,
    ) -> (Arc<SlateDbCatalog>, RootCaptureLifecycle) {
        catalog.close().await.unwrap();
        drop(catalog);
        let catalog = Arc::new(
            SlateDbCatalog::open(catalog_path.clone(), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let lifecycle = RootCaptureLifecycle::new(
            catalog.clone(),
            SlateDbRootStore::new(store, branch_root.clone())
                .with_segment_pool_root(segment_pool.clone()),
        );
        (catalog, lifecycle)
    }

    #[test]
    fn gc_operability_metrics_cover_work_backlog_errors_and_phase_time() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            let timer = GcPhaseTimer::start("mark");
            record_gc_mark_metrics(&GcMarkStats {
                roots_enumerated: 2,
                references_enumerated: 7,
                intermediate_runs: 1,
                unique_segments: 5,
            });
            record_gc_inventory_metrics(&GcInventoryStats {
                objects_seen: 11,
                objects_newer_than_cutoff: 1,
                reachable_objects: 6,
                candidate_objects: 4,
                candidate_bytes: 4096,
                intermediate_runs: 1,
            });
            record_gc_report_metrics(&GcInventoryStats {
                objects_seen: 3,
                objects_newer_than_cutoff: 0,
                reachable_objects: 1,
                candidate_objects: 2,
                candidate_bytes: 2048,
                intermediate_runs: 1,
            });
            record_gc_retained_on_error(GcBlockerKind::CorruptMetadata, 1);
            record_gc_root_open_failure("capture", GcBlockerKind::MissingRoot);
            record_gc_capture_abort(GcBlockerKind::StorageUnavailable);
            drop(timer);
        });
        let rendered = handle.render();
        assert!(rendered.contains("zerofs_gc_references_scanned_total 7"));
        assert!(rendered.contains("zerofs_gc_inventory_objects_total 14"));
        assert!(rendered.contains("zerofs_gc_reported_candidate_objects_total 2"));
        assert!(rendered.contains("zerofs_gc_reported_candidate_bytes_total 2048"));
        assert!(rendered.contains("zerofs_gc_quarantined_bytes_total 4096"));
        assert!(rendered.contains("zerofs_gc_backlog_upper_bound_bytes_added_total 4096"));
        assert!(rendered.contains(
            "zerofs_gc_retained_on_error_lower_bound_total{reason=\"corrupt_metadata\"} 1"
        ));
        assert!(rendered.contains(
            "zerofs_gc_root_open_failures_total{phase=\"capture\",reason=\"missing_root\"} 1"
        ));
        assert!(rendered.contains("zerofs_gc_phase_active{phase=\"mark\"} 0"));
        assert!(rendered.contains("zerofs_gc_aborted_runs_total{phase=\"capture\"} 1"));
        assert!(rendered.contains("zerofs_gc_phase_duration_seconds_sum{phase=\"mark\"}"));
        assert_eq!(
            classify_root_store_error(&RootStoreError::MissingResult("root".to_string())),
            GcBlockerKind::MissingRoot
        );
        assert_eq!(
            classify_root_store_error(&RootStoreError::Invalid("root".to_string())),
            GcBlockerKind::CorruptMetadata
        );
    }

    #[test]
    fn oldest_quarantine_tracking_is_bounded_to_active_runs() {
        let older = Uuid::new_v4();
        let newer = Uuid::new_v4();
        assert_eq!(update_gc_quarantine_tracking(newer, Some(2)), 2);
        assert_eq!(update_gc_quarantine_tracking(older, Some(1)), 1);
        assert_eq!(update_gc_quarantine_tracking(older, None), 2);
        update_gc_quarantine_tracking(newer, None);
    }

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
            phase: GcRunPhase::Reported,
            quarantine_at: None,
            revalidation: None,
            deletion: None,
            created_at: now,
            updated_at: now,
        };
        for phase in [GcRunPhase::Reported, GcRunPhase::Completed] {
            let mut invalid = run.clone();
            invalid.phase = phase;
            assert!(matches!(invalid.validate(), Err(CatalogError::Invalid(_))));
            let mut snapshot = CatalogSnapshot::default();
            snapshot.gc_runs.insert(invalid.id, invalid);
            assert!(snapshot.validate().is_err());
            assert!(snapshot.gc_roots().contains(&&root));
        }
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
        assert!(matches!(
            lifecycle
                .cleanup_artifacts(
                    id,
                    GcArtifactCleanupPolicy {
                        enabled: true,
                        retention_seconds: (i64::MAX / 1_000 + 1) as u64,
                        max_objects: 1,
                        observed_at: run.updated_at,
                    },
                )
                .await,
            Err(RootCaptureLifecycleError::Catalog(CatalogError::Invalid(_)))
        ));
        assert!(matches!(
            lifecycle
                .cleanup_artifacts(
                    id,
                    GcArtifactCleanupPolicy {
                        enabled: true,
                        retention_seconds: 0,
                        max_objects: 1,
                        observed_at: run.updated_at,
                    },
                )
                .await,
            Err(RootCaptureLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));
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
    async fn collector_reopens_and_resumes_from_every_persisted_phase() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog_path = Path::from("gc-phase-restart/catalog");
        let branch_root = Path::from("gc-phase-restart/branches");
        let segment_pool = Path::from("gc-phase-restart/pool");
        let candidate = Segid::new(41, 0);
        let candidate_path = Path::from(format!("{segment_pool}/{}", candidate.object_key()));
        store
            .put(
                &candidate_path,
                bytes::Bytes::from_static(b"candidate").into(),
            )
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;

        let mut catalog = Arc::new(
            SlateDbCatalog::open(catalog_path.clone(), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let mut lifecycle = RootCaptureLifecycle::new(
            catalog.clone(),
            SlateDbRootStore::new(Arc::clone(&store), branch_root.clone())
                .with_segment_pool_root(segment_pool.clone()),
        );
        let run_id = Uuid::new_v4();

        let captured = lifecycle.begin(run_id).await.unwrap();
        assert_eq!(captured.phase, GcRunPhase::Captured);
        drop(lifecycle);
        (catalog, lifecycle) = reopen_gc_lifecycle(
            catalog,
            Arc::clone(&store),
            &catalog_path,
            &branch_root,
            &segment_pool,
        )
        .await;
        assert_eq!(lifecycle.begin(run_id).await.unwrap(), captured);

        let marking = lifecycle.mark(run_id).await.unwrap();
        assert_eq!(marking.phase, GcRunPhase::Marking);
        drop(lifecycle);
        (catalog, lifecycle) = reopen_gc_lifecycle(
            catalog,
            Arc::clone(&store),
            &catalog_path,
            &branch_root,
            &segment_pool,
        )
        .await;
        assert_eq!(lifecycle.mark(run_id).await.unwrap(), marking);

        let quarantined = lifecycle.quarantine(run_id).await.unwrap();
        assert_eq!(quarantined.phase, GcRunPhase::Quarantined);
        assert_eq!(
            quarantined
                .inventory_stats
                .as_ref()
                .unwrap()
                .candidate_objects,
            1
        );
        drop(lifecycle);
        (catalog, lifecycle) = reopen_gc_lifecycle(
            catalog,
            Arc::clone(&store),
            &catalog_path,
            &branch_root,
            &segment_pool,
        )
        .await;
        assert_eq!(lifecycle.quarantine(run_id).await.unwrap(), quarantined);

        let grace = Duration::seconds(MIN_REVALIDATION_GRACE_SECONDS as i64);
        let not_before = quarantined.quarantine_at.unwrap() + grace;
        let observation = GcRevalidationRecord {
            id: Uuid::new_v4(),
            catalog_generation: quarantined.catalog_generation,
            grace_seconds: MIN_REVALIDATION_GRACE_SECONDS,
            not_before,
            inventory_cutoff: not_before,
            roots: Vec::new(),
            root_digest: gc_root_digest(&[]).unwrap(),
            mark_shards: Vec::new(),
            mark_stats: None,
            candidate_shards: Vec::new(),
            stats: None,
            captured_at: not_before,
            completed_at: None,
        };
        let revalidating = catalog
            .begin_gc_revalidation(GcRevalidationCapture {
                run_id,
                expected_revision: quarantined.revision,
                expected_generation: quarantined.catalog_generation,
                observation,
                updated_at: not_before,
            })
            .await
            .unwrap();
        assert_eq!(revalidating.phase, GcRunPhase::Revalidating);
        drop(lifecycle);
        (catalog, lifecycle) = reopen_gc_lifecycle(
            catalog,
            Arc::clone(&store),
            &catalog_path,
            &branch_root,
            &segment_pool,
        )
        .await;

        let validated = lifecycle
            .revalidate_at(run_id, grace, not_before)
            .await
            .unwrap();
        assert_eq!(validated.phase, GcRunPhase::Validated);
        drop(lifecycle);
        (catalog, lifecycle) = reopen_gc_lifecycle(
            catalog,
            Arc::clone(&store),
            &catalog_path,
            &branch_root,
            &segment_pool,
        )
        .await;
        assert_eq!(
            lifecycle
                .revalidate_at(run_id, grace, not_before)
                .await
                .unwrap(),
            validated
        );

        let started_at = catalog_timestamp(Utc::now()).max(validated.updated_at);
        let deleting = catalog
            .publish_gc_deletion(GcDeletionPublication {
                run_id,
                expected_revision: validated.revision,
                expected_generation: validated.catalog_generation,
                progress: GcDeletionProgress {
                    batch_size: 1,
                    next_shard: 0,
                    next_record: 0,
                    deleted_objects: 0,
                    deleted_bytes: 0,
                    already_absent: 0,
                    started_at,
                    completed_at: None,
                },
                updated_at: started_at,
            })
            .await
            .unwrap();
        assert_eq!(deleting.phase, GcRunPhase::Deleting);
        drop(lifecycle);
        (catalog, lifecycle) = reopen_gc_lifecycle(
            catalog,
            Arc::clone(&store),
            &catalog_path,
            &branch_root,
            &segment_pool,
        )
        .await;

        let policy = GcDeletionPolicy {
            enabled: true,
            batch_size: 1,
            minimum_revalidation_grace_seconds: MIN_REVALIDATION_GRACE_SECONDS,
        };
        lifecycle.deletion_control.enable();
        let completed = lifecycle.delete_batch(run_id, policy).await.unwrap();
        assert_eq!(completed.phase, GcRunPhase::Completed);
        assert_eq!(completed.deletion.as_ref().unwrap().deleted_objects, 1);
        assert!(matches!(
            store.head(&candidate_path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        drop(lifecycle);
        (catalog, lifecycle) = reopen_gc_lifecycle(
            catalog,
            Arc::clone(&store),
            &catalog_path,
            &branch_root,
            &segment_pool,
        )
        .await;
        assert_eq!(
            lifecycle.delete_batch(run_id, policy).await.unwrap(),
            completed
        );

        drop(lifecycle);
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
    async fn mutable_writer_fences_root_capture_before_stale_checkpoint_marking() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("gc-writer-fence"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let now = catalog_timestamp(Utc::now());
        let branch = BranchRecord {
            id: Uuid::new_v4(),
            revision: 1,
            name: "mutable-writer".to_string(),
            state: BranchState::Ready,
            root: Some(DurableRoot {
                identity: "gc-writer-branches/stale-root".to_string(),
                manifest_id: format!("{}@1", Uuid::new_v4()),
            }),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::CreateBranch(branch.clone()))
            .await
            .unwrap();
        let writer = LeaseRecord {
            id: Uuid::new_v4(),
            revision: 1,
            subject_kind: LeaseSubjectKind::Branch,
            subject_id: branch.id,
            root: branch.root.clone().unwrap(),
            access_mode: LeaseAccessMode::Write,
            token_hash: "a".repeat(64),
            issued_at: now,
            updated_at: now,
            expires_at: now + Duration::minutes(5),
        };
        catalog
            .apply(CatalogMutation::AcquireLease {
                expected_subject_revision: branch.revision,
                lease: writer,
            })
            .await
            .unwrap();
        let run_id = Uuid::new_v4();
        let lifecycle = RootCaptureLifecycle::new(
            catalog.clone(),
            SlateDbRootStore::new(store, Path::from("gc-writer-branches")),
        );
        assert!(matches!(
            lifecycle.begin(run_id).await,
            Err(RootCaptureLifecycleError::Catalog(
                CatalogError::WriterLeaseActive(id)
            )) if id == branch.id
        ));
        assert!(catalog.gc_run(run_id).await.unwrap().is_none());
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
            deletion: None,
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
    async fn catalog_roots_created_around_cutoff_fence_stale_inventory() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let segment_pool = Path::from("gc-cutoff-pool");
        let mut roots = Vec::new();
        for index in 0..4u64 {
            let data_path = Path::from(format!("gc-cutoff-root-{index}"));
            let db = Db::builder(data_path.clone(), Arc::clone(&store))
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .unwrap();
            let segment = Segid::new(20, index * 256);
            db.put(
                KeyCodec::new().extent_key(index + 1, 0),
                FrameLoc {
                    segid: segment,
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
            store
                .put(
                    &Path::from(format!("{segment_pool}/{}", segment.object_key())),
                    bytes::Bytes::from_static(b"x").into(),
                )
                .await
                .unwrap();
            roots.push((
                ImmutableCheckpoint {
                    database_path: data_path,
                    checkpoint_id: checkpoint.id,
                    manifest_id: checkpoint.manifest_id,
                }
                .durable_root(),
                segment,
            ));
        }
        let root_store =
            SlateDbRootStore::new(Arc::clone(&store), Path::from("gc-cutoff-branches"))
                .with_segment_pool_root(segment_pool.clone());
        let branch_ids = [Uuid::new_v4(), Uuid::new_v4()];
        for (index, destination_id) in [(0usize, branch_ids[0]), (2, branch_ids[1])] {
            let source = ImmutableCheckpoint::from_durable_root(&roots[index].0).unwrap();
            roots[index].0 = root_store
                .create_from_checkpoint(Uuid::new_v4(), destination_id, &source)
                .await
                .unwrap();
            assert_eq!(
                roots[index].0.identity,
                format!("gc-cutoff-branches/{destination_id}")
            );
        }

        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("gc-cutoff-catalog"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let before = catalog_timestamp(Utc::now());
        let before_branch = BranchRecord {
            id: branch_ids[0],
            revision: 1,
            name: "before-cutoff".to_string(),
            state: BranchState::Ready,
            root: Some(roots[0].0.clone()),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: before,
            updated_at: before,
        };
        catalog
            .apply(CatalogMutation::CreateBranch(before_branch.clone()))
            .await
            .unwrap();
        let before_checkpoint = CheckpointRecord {
            id: ImmutableCheckpoint::from_durable_root(&roots[1].0)
                .unwrap()
                .checkpoint_id,
            revision: 1,
            branch_id: before_branch.id,
            name: "before-checkpoint".to_string(),
            root: roots[1].0.clone(),
            created_at: before,
            updated_at: before,
        };
        catalog
            .apply(CatalogMutation::CreateCheckpoint(before_checkpoint.clone()))
            .await
            .unwrap();

        let lifecycle = RootCaptureLifecycle::new(catalog.clone(), root_store);
        let run = lifecycle.begin(Uuid::new_v4()).await.unwrap();
        assert!(run.inventory_cutoff >= before);
        assert!(run.roots.contains(&GcRootPin {
            kind: GcRootKind::Branch,
            root: roots[0].0.clone(),
        }));
        assert!(run.roots.contains(&GcRootPin {
            kind: GcRootKind::Checkpoint,
            root: roots[1].0.clone(),
        }));

        let after = run.inventory_cutoff + Duration::microseconds(1);
        let after_branch = BranchRecord {
            id: branch_ids[1],
            revision: 1,
            name: "after-cutoff".to_string(),
            state: BranchState::Ready,
            root: Some(roots[2].0.clone()),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: after,
            updated_at: after,
        };
        catalog
            .apply(CatalogMutation::CreateBranch(after_branch.clone()))
            .await
            .unwrap();
        let after_checkpoint = CheckpointRecord {
            id: ImmutableCheckpoint::from_durable_root(&roots[3].0)
                .unwrap()
                .checkpoint_id,
            revision: 1,
            branch_id: after_branch.id,
            name: "after-checkpoint".to_string(),
            root: roots[3].0.clone(),
            created_at: after,
            updated_at: after,
        };
        catalog
            .apply(CatalogMutation::CreateCheckpoint(after_checkpoint))
            .await
            .unwrap();
        assert!(!run.roots.iter().any(|pin| pin.root == roots[2].0));
        assert!(!run.roots.iter().any(|pin| pin.root == roots[3].0));

        lifecycle.mark(run.id).await.unwrap();
        assert!(matches!(
            lifecycle.quarantine(run.id).await,
            Err(RootCaptureLifecycleError::Catalog(
                CatalogError::RevisionConflict { .. }
            ))
        ));
        assert_eq!(
            catalog.gc_run(run.id).await.unwrap().unwrap().phase,
            GcRunPhase::Marking
        );
        for (_, segment) in &roots {
            assert!(
                store
                    .head(&Path::from(format!(
                        "{segment_pool}/{}",
                        segment.object_key()
                    )))
                    .await
                    .is_ok(),
                "a generation race must retain every pool object"
            );
        }

        let fresh = lifecycle.begin(Uuid::new_v4()).await.unwrap();
        for expected in [
            (GcRootKind::Branch, &roots[0].0),
            (GcRootKind::Checkpoint, &roots[1].0),
            (GcRootKind::Branch, &roots[2].0),
            (GcRootKind::Checkpoint, &roots[3].0),
        ] {
            assert!(fresh.roots.contains(&GcRootPin {
                kind: expected.0,
                root: expected.1.clone(),
            }));
        }
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn post_quarantine_roots_enter_revalidation_and_post_validation_changes_stop_delete() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let segment_pool = Path::from("gc-catalog-phase-pool");
        let candidate = Segid::new(42, 0);
        let candidate_path = Path::from(format!("{segment_pool}/{}", candidate.object_key()));
        store
            .put(
                &candidate_path,
                bytes::Bytes::from_static(b"candidate").into(),
            )
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;

        let branch_root = Path::from("gc-catalog-phase-branches");
        let root_store = SlateDbRootStore::new(Arc::clone(&store), branch_root.clone())
            .with_segment_pool_root(segment_pool.clone());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("gc-catalog-phase-catalog"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let lifecycle = RootCaptureLifecycle::new(catalog.clone(), root_store.clone());
        let run_id = Uuid::new_v4();
        lifecycle.begin(run_id).await.unwrap();
        lifecycle.mark(run_id).await.unwrap();
        let quarantined = lifecycle.quarantine(run_id).await.unwrap();
        assert_eq!(quarantined.catalog_generation, 0);
        assert_eq!(
            quarantined
                .inventory_stats
                .as_ref()
                .unwrap()
                .candidate_objects,
            1
        );

        let source_path = Path::from("gc-catalog-phase-source");
        let source_db = Db::builder(source_path.clone(), Arc::clone(&store))
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap();
        source_db
            .put(KeyCodec::new().system_counter_key(), b"value")
            .await
            .unwrap();
        source_db.flush().await.unwrap();
        let checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let branch_id = Uuid::new_v4();
        let published_root = root_store
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
        let published_at = catalog_timestamp(Utc::now());
        let branch = BranchRecord {
            id: branch_id,
            revision: 1,
            name: "post-quarantine".to_string(),
            state: BranchState::Ready,
            root: Some(published_root.clone()),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: published_at,
            updated_at: published_at,
        };
        catalog
            .apply(CatalogMutation::CreateBranch(branch.clone()))
            .await
            .unwrap();

        let grace = Duration::seconds(MIN_REVALIDATION_GRACE_SECONDS as i64);
        let not_before = quarantined.quarantine_at.unwrap() + grace;
        let writer = LeaseRecord {
            id: Uuid::new_v4(),
            revision: 1,
            subject_kind: LeaseSubjectKind::Branch,
            subject_id: branch.id,
            root: branch.root.clone().unwrap(),
            access_mode: LeaseAccessMode::Write,
            token_hash: "b".repeat(64),
            issued_at: published_at,
            updated_at: published_at,
            expires_at: published_at + Duration::minutes(5),
        };
        catalog
            .apply(CatalogMutation::AcquireLease {
                expected_subject_revision: branch.revision,
                lease: writer.clone(),
            })
            .await
            .unwrap();
        assert!(matches!(
            lifecycle.revalidate_at(run_id, grace, not_before).await,
            Err(RootCaptureLifecycleError::Catalog(
                CatalogError::WriterLeaseActive(id)
            )) if id == branch.id
        ));
        assert!(
            catalog
                .gc_blockers(run_id)
                .await
                .unwrap()
                .iter()
                .any(|blocker| blocker.kind == GcBlockerKind::LeaseUncertainty)
        );
        let writer_db = Db::builder(
            Path::from(published_root.identity.clone()),
            Arc::clone(&store),
        )
        .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
        .build()
        .await
        .unwrap();
        writer_db
            .put(KeyCodec::new().system_counter_key(), b"advanced")
            .await
            .unwrap();
        writer_db.flush().await.unwrap();
        writer_db.close().await.unwrap();
        let advanced_root = root_store
            .publish_writer_head(branch.id, writer.id, &writer.root)
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::PublishWriterHead {
                lease_id: writer.id,
                expected_lease_revision: writer.revision,
                token_hash: writer.token_hash,
                previous_root: writer.root,
                root: advanced_root.clone(),
                published_at: published_at + Duration::microseconds(1),
            })
            .await
            .unwrap();
        let validated = lifecycle
            .revalidate_at(run_id, grace, not_before)
            .await
            .unwrap();
        assert_eq!(validated.phase, GcRunPhase::Validated);
        let observation = validated.revalidation.as_ref().unwrap();
        assert_eq!(observation.catalog_generation, 3);
        assert!(observation.roots.contains(&GcRootPin {
            kind: GcRootKind::Branch,
            root: advanced_root.clone(),
        }));
        assert!(store.head(&candidate_path).await.is_ok());

        let changed_at = validated.updated_at + Duration::microseconds(1);
        let mut changed_branch = branch;
        changed_branch.revision = 3;
        changed_branch.root = Some(advanced_root);
        changed_branch.updated_at = changed_at;
        catalog
            .apply(CatalogMutation::ReplaceBranch {
                expected_revision: 2,
                record: changed_branch,
            })
            .await
            .unwrap();
        lifecycle.deletion_control.enable();
        assert!(matches!(
            lifecycle
                .delete_batch(
                    run_id,
                    GcDeletionPolicy {
                        enabled: true,
                        batch_size: 1,
                        minimum_revalidation_grace_seconds: MIN_REVALIDATION_GRACE_SECONDS,
                    },
                )
                .await,
            Err(RootCaptureLifecycleError::Catalog(
                CatalogError::RevisionConflict {
                    expected: 3,
                    actual: 4
                }
            ))
        ));
        let retained = catalog.gc_run(run_id).await.unwrap().unwrap();
        assert_eq!(retained.phase, GcRunPhase::Validated);
        assert!(retained.deletion.is_none());
        assert!(store.head(&candidate_path).await.is_ok());
        assert!(
            catalog
                .gc_blockers(run_id)
                .await
                .unwrap()
                .iter()
                .any(|blocker| blocker.kind == GcBlockerKind::GenerationChanged)
        );
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn collector_matches_ideal_two_observation_model_and_cutoff() {
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
        let retained_two = Segid::new(12, 1_024);
        let newer_than_cutoff = Segid::new(12, 1_280);
        for segment in [reachable, became_reachable, absent, retained, retained_two] {
            store
                .put(
                    &Path::from(format!("{segment_pool}/{}", segment.object_key())),
                    bytes::Bytes::from_static(b"x").into(),
                )
                .await
                .unwrap();
        }
        let mut newest_existing = DateTime::<Utc>::MIN_UTC;
        for segment in [reachable, became_reachable, absent, retained, retained_two] {
            newest_existing = newest_existing.max(
                store
                    .head(&Path::from(format!(
                        "{segment_pool}/{}",
                        segment.object_key()
                    )))
                    .await
                    .unwrap()
                    .last_modified,
            );
        }
        let inventory_cutoff = catalog_timestamp(newest_existing + Duration::microseconds(1));
        let now = inventory_cutoff;
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let newer_path = Path::from(format!("{segment_pool}/{}", newer_than_cutoff.object_key()));
        store
            .put(&newer_path, bytes::Bytes::from_static(b"x").into())
            .await
            .unwrap();
        assert!(
            store.head(&newer_path).await.unwrap().last_modified > inventory_cutoff,
            "the post-cutoff fixture must actually be newer than the immutable cutoff"
        );
        let inventory_eligible = std::collections::BTreeSet::from([
            reachable,
            became_reachable,
            absent,
            retained,
            retained_two,
        ]);
        let first_reachable = std::collections::BTreeSet::from([reachable]);
        let first_candidates = inventory_eligible
            .difference(&first_reachable)
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let second_reachable = std::collections::BTreeSet::from([reachable, became_reachable]);
        let absent_before_revalidation = std::collections::BTreeSet::from([absent]);
        let ideal_deletions = first_candidates
            .difference(&second_reachable)
            .copied()
            .filter(|segment| !absent_before_revalidation.contains(segment))
            .collect::<std::collections::BTreeSet<_>>();
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
            deletion: None,
            created_at: now,
            updated_at: now,
        };
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("gc-revalidation-catalog"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        catalog.begin_gc_run(0, run.clone()).await.unwrap();
        let deletion_control = GcDeletionControl::default();
        let lifecycle = RootCaptureLifecycle::new(
            catalog.clone(),
            SlateDbRootStore::new(Arc::clone(&store), Path::from("gc-revalidation-branches"))
                .with_segment_pool_root(segment_pool.clone()),
        )
        .with_deletion_control(deletion_control.clone());
        let marked = lifecycle.mark(run.id).await.unwrap();
        let obsolete_policy = GcArtifactCleanupPolicy {
            enabled: true,
            retention_seconds: 1,
            max_objects: MAX_ARTIFACT_CLEANUP_OBJECTS,
            observed_at: marked.updated_at + Duration::seconds(1),
        };
        assert!(matches!(
            lifecycle
                .cleanup_obsolete_artifacts(
                    run.id,
                    GcArtifactCleanupPolicy {
                        observed_at: marked.updated_at,
                        ..obsolete_policy
                    },
                )
                .await,
            Err(RootCaptureLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));
        assert!(
            !lifecycle
                .cleanup_obsolete_artifacts(run.id, obsolete_policy)
                .await
                .unwrap()
                .has_more
        );
        for class in ["mark-runs", "mark-merge", "mark-online"] {
            let prefix = Path::from(format!("{GC_ARTIFACT_PREFIX}/{}/{class}", run.id));
            assert!(store.list(Some(&prefix)).next().await.is_none());
        }
        for shard in &marked.mark_shards {
            assert!(
                store
                    .head(&Path::from(shard.location.clone()))
                    .await
                    .is_ok()
            );
        }
        assert_eq!(lifecycle.mark(run.id).await.unwrap(), marked);
        let quarantined = lifecycle.quarantine(run.id).await.unwrap();
        assert_eq!(
            quarantined
                .inventory_stats
                .as_ref()
                .unwrap()
                .candidate_objects,
            first_candidates.len() as u64
        );
        let inventory_stats = quarantined.inventory_stats.as_ref().unwrap();
        assert_eq!(
            inventory_stats.objects_seen,
            inventory_eligible.len() as u64 + 1
        );
        assert_eq!(inventory_stats.objects_newer_than_cutoff, 1);
        assert_eq!(
            inventory_stats.reachable_objects,
            first_reachable.len() as u64
        );
        let obsolete_policy = GcArtifactCleanupPolicy {
            observed_at: quarantined.updated_at + Duration::seconds(1),
            ..obsolete_policy
        };
        assert!(
            !lifecycle
                .cleanup_obsolete_artifacts(run.id, obsolete_policy)
                .await
                .unwrap()
                .has_more
        );
        for class in ["inventory-runs", "inventory-online", "inventory"] {
            let prefix = Path::from(format!("{GC_ARTIFACT_PREFIX}/{}/{class}", run.id));
            assert!(store.list(Some(&prefix)).next().await.is_none());
        }
        for shard in &quarantined.quarantine_shards {
            assert!(
                store
                    .head(&Path::from(shard.location.clone()))
                    .await
                    .is_ok()
            );
        }
        assert_eq!(lifecycle.quarantine(run.id).await.unwrap(), quarantined);
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
                    deletion: None,
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
        assert_eq!(
            stats.first_observation_candidates,
            first_candidates.len() as u64
        );
        assert_eq!(
            stats.became_reachable,
            first_candidates.intersection(&second_reachable).count() as u64
        );
        assert_eq!(
            stats.already_absent,
            absent_before_revalidation.len() as u64
        );
        assert_eq!(stats.retained_candidates, ideal_deletions.len() as u64);
        let observation = validated.revalidation.as_ref().unwrap();
        let observation_prefix = Path::from(format!("{GC_ARTIFACT_PREFIX}/{}", observation.id));
        assert!(store.list(Some(&observation_prefix)).next().await.is_some());
        let obsolete_policy = GcArtifactCleanupPolicy {
            observed_at: validated.updated_at + Duration::seconds(1),
            ..obsolete_policy
        };
        assert!(
            !lifecycle
                .cleanup_obsolete_artifacts(run.id, obsolete_policy)
                .await
                .unwrap()
                .has_more
        );
        for path in marked
            .mark_shards
            .iter()
            .map(|shard| &shard.location)
            .chain(
                quarantined
                    .quarantine_shards
                    .iter()
                    .map(|shard| &shard.location),
            )
        {
            assert!(matches!(
                store.head(&Path::from(path.clone())).await,
                Err(object_store::Error::NotFound { .. })
            ));
        }
        for path in observation
            .mark_shards
            .iter()
            .map(|shard| &shard.location)
            .chain(
                observation
                    .candidate_shards
                    .iter()
                    .map(|shard| &shard.location),
            )
        {
            assert!(store.head(&Path::from(path.clone())).await.is_ok());
        }
        assert_eq!(
            lifecycle
                .cleanup_obsolete_artifacts(run.id, obsolete_policy)
                .await
                .unwrap(),
            GcArtifactCleanupReport::default()
        );
        let mut malformed = validated.clone();
        malformed
            .revalidation
            .as_mut()
            .unwrap()
            .stats
            .as_mut()
            .unwrap()
            .first_observation_candidates = 3;
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
        assert!(matches!(
            lifecycle
                .delete_batch(run.id, GcDeletionPolicy::default())
                .await,
            Err(RootCaptureLifecycleError::Catalog(CatalogError::Invalid(_)))
        ));
        let policy = GcDeletionPolicy {
            enabled: true,
            batch_size: 1,
            minimum_revalidation_grace_seconds: MIN_REVALIDATION_GRACE_SECONDS,
        };
        assert!(matches!(
            lifecycle.delete_batch(run.id, policy).await,
            Err(RootCaptureLifecycleError::Catalog(CatalogError::Invalid(ref message)))
                if message.contains("rapid kill switch")
        ));
        deletion_control.enable();
        let conservative_policy = GcDeletionPolicy {
            minimum_revalidation_grace_seconds: MIN_REVALIDATION_GRACE_SECONDS + 1,
            ..policy
        };
        assert!(matches!(
            lifecycle.delete_batch(run.id, conservative_policy).await,
            Err(RootCaptureLifecycleError::Catalog(
                CatalogError::OperationConflict(ref message)
            )) if message.contains("below deletion policy minimum")
        ));
        assert_eq!(catalog.gc_run(run.id).await.unwrap().unwrap(), validated);
        let partial = lifecycle.delete_batch(run.id, policy).await.unwrap();
        assert_eq!(partial.phase, GcRunPhase::Deleting);
        assert_eq!(partial.revision, 7);
        assert_eq!(partial.deletion.as_ref().unwrap().deleted_objects, 1);
        let mut skipped = partial.clone();
        skipped.deletion.as_mut().unwrap().next_record += 1;
        assert!(matches!(skipped.validate(), Err(CatalogError::Invalid(_))));
        assert_eq!(
            catalog
                .publish_gc_deletion(GcDeletionPublication {
                    run_id: run.id,
                    expected_revision: 6,
                    expected_generation: 0,
                    progress: partial.deletion.clone().unwrap(),
                    updated_at: partial.updated_at,
                })
                .await
                .unwrap(),
            partial,
            "an exact retry must reconcile a lost progress response"
        );
        deletion_control.disable();
        assert!(matches!(
            lifecycle.delete_batch(run.id, policy).await,
            Err(RootCaptureLifecycleError::Catalog(CatalogError::Invalid(ref message)))
                if message.contains("rapid kill switch")
        ));
        assert_eq!(catalog.gc_run(run.id).await.unwrap().unwrap(), partial);
        deletion_control.enable();
        let completed = lifecycle.delete_batch(run.id, policy).await.unwrap();
        assert_eq!(completed.phase, GcRunPhase::Completed);
        assert_eq!(completed.revision, 8);
        let deletion = completed.deletion.as_ref().unwrap();
        assert_eq!(deletion.next_shard, 256);
        assert_eq!(deletion.deleted_objects, ideal_deletions.len() as u64);
        assert_eq!(deletion.deleted_bytes, ideal_deletions.len() as u64);
        assert_eq!(deletion.already_absent, 0);
        assert!(deletion.completed_at.is_some());
        assert!(matches!(
            store
                .head(&Path::from(format!(
                    "{segment_pool}/{}",
                    retained.object_key()
                )))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
        for segment in [
            reachable,
            became_reachable,
            absent,
            retained,
            retained_two,
            newer_than_cutoff,
        ] {
            let object = Path::from(format!("{segment_pool}/{}", segment.object_key()));
            let should_be_absent =
                absent_before_revalidation.contains(&segment) || ideal_deletions.contains(&segment);
            assert_eq!(
                matches!(
                    store.head(&object).await,
                    Err(object_store::Error::NotFound { .. })
                ),
                should_be_absent,
                "collector decision for {segment:?} disagrees with the ideal two-observation model"
            );
        }
        assert!(matches!(
            store
                .head(&Path::from(format!(
                    "{segment_pool}/{}",
                    retained_two.object_key()
                )))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert_eq!(
            lifecycle.delete_batch(run.id, policy).await.unwrap(),
            completed
        );
        assert!(matches!(
            lifecycle
                .cleanup_obsolete_artifacts(
                    run.id,
                    GcArtifactCleanupPolicy {
                        observed_at: completed.updated_at + Duration::seconds(1),
                        ..obsolete_policy
                    },
                )
                .await,
            Err(RootCaptureLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));
        let artifact_prefix = Path::from(format!("{GC_ARTIFACT_PREFIX}/{}", run.id));
        assert!(store.list(Some(&artifact_prefix)).next().await.is_some());
        assert!(store.list(Some(&observation_prefix)).next().await.is_some());
        let other_run_artifact =
            Path::from(format!("{GC_ARTIFACT_PREFIX}/{}/keep.bin", Uuid::new_v4()));
        store
            .put(
                &other_run_artifact,
                bytes::Bytes::from_static(b"other-run").into(),
            )
            .await
            .unwrap();
        let completed_at = deletion.completed_at.unwrap();
        assert!(matches!(
            lifecycle
                .cleanup_artifacts(
                    run.id,
                    GcArtifactCleanupPolicy {
                        enabled: false,
                        retention_seconds: 1,
                        max_objects: 128,
                        observed_at: completed_at + Duration::seconds(1),
                    },
                )
                .await,
            Err(RootCaptureLifecycleError::Catalog(CatalogError::Invalid(_)))
        ));
        assert!(matches!(
            lifecycle
                .cleanup_artifacts(
                    run.id,
                    GcArtifactCleanupPolicy {
                        enabled: true,
                        retention_seconds: 1,
                        max_objects: 128,
                        observed_at: completed_at,
                    },
                )
                .await,
            Err(RootCaptureLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));
        let cleanup_policy = GcArtifactCleanupPolicy {
            enabled: true,
            retention_seconds: 1,
            max_objects: 128,
            observed_at: completed_at + Duration::seconds(1),
        };
        let mut cleanup_complete = false;
        for _ in 0..32 {
            let report = lifecycle
                .cleanup_artifacts(run.id, cleanup_policy)
                .await
                .unwrap();
            assert!(report.examined <= cleanup_policy.max_objects as u64);
            assert!(report.deleted + report.already_absent <= report.examined);
            if !report.has_more {
                cleanup_complete = true;
                break;
            }
        }
        assert!(cleanup_complete, "bounded artifact cleanup must converge");
        assert!(store.list(Some(&artifact_prefix)).next().await.is_none());
        assert!(store.list(Some(&observation_prefix)).next().await.is_none());
        assert!(store.head(&other_run_artifact).await.is_ok());
        assert!(
            store
                .head(&Path::from(format!(
                    "{segment_pool}/{}",
                    became_reachable.object_key()
                )))
                .await
                .is_ok(),
            "artifact cleanup must not cross into the segment pool"
        );
        assert_eq!(
            lifecycle
                .cleanup_artifacts(run.id, cleanup_policy)
                .await
                .unwrap(),
            GcArtifactCleanupReport::default()
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
            deletion: None,
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
        let reported = lifecycle.report(run.id).await.unwrap();
        assert_eq!(reported.phase, GcRunPhase::Reported);
        assert_eq!(reported.revision, 3);
        assert_eq!(reported.quarantine_shards.len(), 256);
        let inventory = reported.inventory_stats.as_ref().unwrap();
        assert_eq!(inventory.objects_seen, 10_001);
        assert_eq!(inventory.objects_newer_than_cutoff, 1);
        assert_eq!(inventory.reachable_objects, 1_000);
        assert_eq!(inventory.candidate_objects, 9_000);
        assert_eq!(inventory.candidate_bytes, 9_000);
        assert_eq!(inventory.intermediate_runs, 257);
        assert_eq!(lifecycle.report(run.id).await.unwrap(), reported);
        assert!(catalog.snapshot().await.unwrap().gc_roots().is_empty());
        assert!(matches!(
            lifecycle.quarantine(run.id).await,
            Err(RootCaptureLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));
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
                &Path::from(reported.quarantine_shards[0].location.clone()),
                bytes::Bytes::from_static(b"corrupt").into(),
            )
            .await
            .unwrap();
        assert!(matches!(
            lifecycle.report(run.id).await,
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
        assert!(
            artifact_store
                .head(&Path::from(format!(
                    "{segment_pool}/{}",
                    Segid::new(8, 0).object_key()
                )))
                .await
                .is_ok(),
            "corrupt authoritative artifacts must fail closed without deleting a candidate"
        );
        let blockers = catalog.gc_blockers(run.id).await.unwrap();
        assert_eq!(blockers.len(), 1);
        assert_eq!(blockers[0].kind, GcBlockerKind::CorruptMetadata);
        assert_eq!(blockers[0].occurrences, 2);
        let cleanup_policy = GcArtifactCleanupPolicy {
            enabled: true,
            retention_seconds: 0,
            max_objects: MAX_ARTIFACT_CLEANUP_OBJECTS,
            observed_at: reported.updated_at + Duration::seconds(1),
        };
        loop {
            let cleanup = lifecycle
                .cleanup_artifacts(run.id, cleanup_policy)
                .await
                .unwrap();
            if !cleanup.has_more {
                break;
            }
        }
        assert_eq!(catalog.gc_run(run.id).await.unwrap(), Some(reported));
        catalog.close().await.unwrap();
    }
}
