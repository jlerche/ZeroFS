use super::lease::LEASE_CLOCK_SKEW;
use super::{
    BranchCreateOperation, BranchCreatePhase, BranchDeleteOperation, BranchDeletePhase,
    BranchRecord, BranchState, CATALOG_SCHEMA_VERSION, Catalog, CatalogError, CatalogMutation,
    CatalogSnapshot, CheckpointRecord, GcBlockerKind, GcBlockerRecord, GcDeletionPublication,
    GcMarkShard, GcMarkStats, GcQuarantinePublication, GcReportPublication, GcRevalidationCapture,
    GcRevalidationPublication, GcRunPhase, GcRunRecord, LeaseAccessMode, LeaseRecord,
    LeaseSubjectKind, LeaseTombstone, LocalGcGuardRecord, LocalGcProgressRecord,
    MAX_ACTIVE_LEASES_PER_BRANCH, MAX_BRANCH_LINEAGE_DEPTH, MAX_LIVE_BRANCHES,
    MAX_TOMBSTONE_CLEANUP_SCAN, PrivateEpochRecord, PrivateEpochState, PrivateGcGuardView,
    PrivateGcOwnerView, RetiredCatalogId, RetiredCatalogKind, TombstoneCleanupPolicy,
    TombstoneCleanupReport, TombstoneKind, TombstoneRecord, validate_name, validate_root,
    validate_timestamp,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use serde::{Serialize, de::DeserializeOwned};
use slatedb::config::WriteOptions;
use slatedb::object_store::path::Path;
use slatedb::{Db, WriteBatch};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

const STATE_KEY: &[u8] = b"catalog/state";
const BRANCH_PREFIX: &[u8] = b"catalog/branch/";
const BRANCH_NAME_PREFIX: &[u8] = b"catalog/branch-name/";
const CHECKPOINT_PREFIX: &[u8] = b"catalog/checkpoint/";
const CHECKPOINT_NAME_PREFIX: &[u8] = b"catalog/checkpoint-name/";
const TOMBSTONE_PREFIX: &[u8] = b"catalog/tombstone/";
const BRANCH_CREATE_OPERATION_PREFIX: &[u8] = b"catalog/branch-create-operation/";
const BRANCH_DELETE_OPERATION_PREFIX: &[u8] = b"catalog/branch-delete-operation/";
const GC_RUN_PREFIX: &[u8] = b"catalog/gc-run/";
const GC_BLOCKER_PREFIX: &[u8] = b"catalog/gc-blocker/";
const BRANCH_CREATE_SOURCE_PREFIX: &[u8] = b"catalog/branch-create-source/";
const LEASE_PREFIX: &[u8] = b"catalog/lease/";
const LEASE_TOMBSTONE_PREFIX: &[u8] = b"catalog/lease-tombstone/";
const PRIVATE_EPOCH_PREFIX: &[u8] = b"catalog/private-epoch/";
const LOCAL_GC_GUARD_PREFIX: &[u8] = b"catalog/local-gc-guard/";
const LOCAL_GC_PROGRESS_PREFIX: &[u8] = b"catalog/local-gc-progress/";
const PRIVATE_GC_BRANCH_BLOCKER_PREFIX: &[u8] = b"catalog/private-gc-branch-blocker/";
const PRIVATE_GC_GLOBAL_BLOCKER_KEY: &[u8] = b"catalog/private-gc-global-blocker";
const RETIRED_CATALOG_ID_PREFIX: &[u8] = b"catalog/retired-id/";
const BRANCH_LINEAGE_DEPTH_PREFIX: &[u8] = b"catalog/private-branch-lineage-depth/";
#[allow(dead_code)] // Used by the bounded cleanup entry point as server wiring lands.
const TOMBSTONE_CLEANUP_CURSOR_KEY: &[u8] = b"catalog/tombstone-cleanup-cursor";
const LEGACY_SCHEMA_VERSION: u32 = 1;
const PREVIOUS_SCHEMA_VERSION: u32 = 2;
const OPERATION_SCHEMA_VERSION: u32 = 3;
const LEASE_SCHEMA_VERSION: u32 = 4;
const DELETION_SCHEMA_VERSION: u32 = 5;
const BRANCH_DELETION_SCHEMA_VERSION: u32 = 6;
const GC_CAPTURE_SCHEMA_VERSION: u32 = 7;
const GC_MARK_SCHEMA_VERSION: u32 = 8;
const GC_QUARANTINE_SCHEMA_VERSION: u32 = 9;
const GC_REVALIDATION_SCHEMA_VERSION: u32 = 10;
const GC_DELETION_SCHEMA_VERSION: u32 = 11;
const PRIVATE_EPOCH_SCHEMA_VERSION: u32 = 12;
const LOCAL_GC_GUARD_SCHEMA_VERSION: u32 = 13;
const TARGETED_PRIVATE_GC_VIEW_SCHEMA_VERSION: u32 = 14;
const PRIVATE_GC_BLOCKER_SCHEMA_VERSION: u32 = 15;
const TOMBSTONE_CLEANUP_SCHEMA_VERSION: u32 = 16;
const SERVER_CATALOG_SCHEMA_VERSION: u32 = 17;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CatalogState {
    schema_version: u32,
    generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct BranchLineageDepth {
    branch_id: Uuid,
    depth: u16,
}

impl BranchLineageDepth {
    fn validate(&self) -> Result<(), CatalogError> {
        if self.branch_id.is_nil() || self.depth as usize > MAX_BRANCH_LINEAGE_DEPTH {
            return Err(CatalogError::Corrupt(
                "private branch lineage depth is invalid".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[allow(dead_code)] // Used by the bounded cleanup entry point as server wiring lands.
struct TombstoneCleanupCursor {
    after: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct PrivateGcBranchBlockers {
    branch_id: Uuid,
    checkpoints: u64,
    leases: u64,
    incomplete_children: u64,
}

impl PrivateGcBranchBlockers {
    fn empty(branch_id: Uuid) -> Self {
        Self {
            branch_id,
            checkpoints: 0,
            leases: 0,
            incomplete_children: 0,
        }
    }

    fn validate(&self) -> Result<(), CatalogError> {
        if self.branch_id.is_nil() {
            return Err(CatalogError::Corrupt(
                "private GC blocker branch UUID is nil".to_string(),
            ));
        }
        Ok(())
    }

    fn is_clear(&self) -> bool {
        self.checkpoints == 0 && self.leases == 0 && self.incomplete_children == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct PrivateGcGlobalBlockers {
    root_retaining_gc_runs: u64,
}

impl PrivateGcGlobalBlockers {
    fn empty() -> Self {
        Self {
            root_retaining_gc_runs: 0,
        }
    }
}

/// Authoritative production catalog stored in a dedicated SlateDB database.
///
/// Each live record, name index, and tombstone has an independent key. One
/// atomic write batch updates the touched entries and generation; no mutation
/// rewrites the full catalog.
pub struct SlateDbCatalog {
    db: Arc<Db>,
    /// SlateDB admits one writer for a database path. This lock also gives
    /// multi-key point lookups and full snapshots a process-local consistent
    /// view relative to catalog mutations.
    lock: Mutex<()>,
}

impl std::fmt::Debug for SlateDbCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SlateDbCatalog")
            .finish_non_exhaustive()
    }
}

impl SlateDbCatalog {
    pub async fn open(
        path: Path,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, CatalogError> {
        let db = Arc::new(slatedb::DbBuilder::new(path, object_store).build().await?);
        let catalog = Self {
            db,
            lock: Mutex::new(()),
        };
        let _guard = catalog.lock.lock().await;
        if catalog.db.get(STATE_KEY).await?.is_none() {
            let state = CatalogState {
                schema_version: CATALOG_SCHEMA_VERSION,
                generation: 0,
            };
            let mut batch = WriteBatch::new();
            put_json(&mut batch, Bytes::from_static(STATE_KEY), &state)?;
            put_json(
                &mut batch,
                Bytes::from_static(PRIVATE_GC_GLOBAL_BLOCKER_KEY),
                &PrivateGcGlobalBlockers::empty(),
            )?;
            catalog
                .db
                .write_with_options(batch, &durable_write_options())
                .await?;
        } else {
            catalog.migrate_unlocked().await?;
        }
        drop(_guard);
        Ok(catalog)
    }

    #[allow(dead_code)] // Explicit shutdown is used by owners once server wiring lands.
    pub async fn close(&self) -> Result<(), CatalogError> {
        self.db.close().await?;
        Ok(())
    }

    async fn state_unlocked(&self) -> Result<CatalogState, CatalogError> {
        let bytes =
            self.db.get(STATE_KEY).await?.ok_or_else(|| {
                CatalogError::Corrupt("missing SlateDB catalog state".to_string())
            })?;
        let state = serde_json::from_slice::<CatalogState>(&bytes)?;
        if state.schema_version != CATALOG_SCHEMA_VERSION {
            return Err(CatalogError::Corrupt(format!(
                "unsupported SlateDB catalog schema version {}",
                state.schema_version
            )));
        }
        Ok(state)
    }

    async fn migrate_unlocked(&self) -> Result<(), CatalogError> {
        let bytes =
            self.db.get(STATE_KEY).await?.ok_or_else(|| {
                CatalogError::Corrupt("missing SlateDB catalog state".to_string())
            })?;
        let state = serde_json::from_slice::<CatalogState>(&bytes)?;
        if state.schema_version == CATALOG_SCHEMA_VERSION {
            return Ok(());
        }
        if ![
            LEGACY_SCHEMA_VERSION,
            PREVIOUS_SCHEMA_VERSION,
            OPERATION_SCHEMA_VERSION,
            LEASE_SCHEMA_VERSION,
            DELETION_SCHEMA_VERSION,
            BRANCH_DELETION_SCHEMA_VERSION,
            GC_CAPTURE_SCHEMA_VERSION,
            GC_MARK_SCHEMA_VERSION,
            GC_QUARANTINE_SCHEMA_VERSION,
            GC_REVALIDATION_SCHEMA_VERSION,
            GC_DELETION_SCHEMA_VERSION,
            PRIVATE_EPOCH_SCHEMA_VERSION,
            LOCAL_GC_GUARD_SCHEMA_VERSION,
            TARGETED_PRIVATE_GC_VIEW_SCHEMA_VERSION,
            PRIVATE_GC_BLOCKER_SCHEMA_VERSION,
            TOMBSTONE_CLEANUP_SCHEMA_VERSION,
            SERVER_CATALOG_SCHEMA_VERSION,
        ]
        .contains(&state.schema_version)
        {
            return Err(CatalogError::Corrupt(format!(
                "unsupported SlateDB catalog schema version {}",
                state.schema_version
            )));
        }

        if state.schema_version == LEGACY_SCHEMA_VERSION {
            for prefix in [BRANCH_PREFIX, CHECKPOINT_PREFIX] {
                let mut iterator = self.db.scan_prefix(prefix, ..).await?;
                while let Some(entry) = iterator.next().await? {
                    let mut value = serde_json::from_slice::<serde_json::Value>(&entry.value)?;
                    let object = value.as_object_mut().ok_or_else(|| {
                        CatalogError::Corrupt("catalog record is not a JSON object".to_string())
                    })?;
                    object
                        .entry("revision")
                        .or_insert_with(|| serde_json::Value::from(1));
                    self.db
                        .put_with_options(
                            entry.key,
                            serde_json::to_vec(&value)?,
                            &slatedb::config::PutOptions::default(),
                            &durable_write_options(),
                        )
                        .await?;
                }
            }
            let mut iterator = self.db.scan_prefix(TOMBSTONE_PREFIX, ..).await?;
            while let Some(entry) = iterator.next().await? {
                let mut value = serde_json::from_slice::<serde_json::Value>(&entry.value)?;
                let object = value.as_object_mut().ok_or_else(|| {
                    CatalogError::Corrupt("catalog tombstone is not a JSON object".to_string())
                })?;
                object.entry("parent_id").or_insert(serde_json::Value::Null);
                object
                    .entry("origin_checkpoint_id")
                    .or_insert(serde_json::Value::Null);
                let deleted_at = object.get("deleted_at").cloned().ok_or_else(|| {
                    CatalogError::Corrupt("catalog tombstone is missing deleted_at".to_string())
                })?;
                object.entry("created_at").or_insert(deleted_at);
                self.db
                    .put_with_options(
                        entry.key,
                        serde_json::to_vec(&value)?,
                        &slatedb::config::PutOptions::default(),
                        &durable_write_options(),
                    )
                    .await?;
            }
        }
        self.rebuild_private_gc_blockers_unlocked()
            .await
            .map_err(|error| {
                CatalogError::Invalid(format!(
                    "SlateDB catalog v{} cannot migrate to v{CATALOG_SCHEMA_VERSION}: {error}",
                    state.schema_version
                ))
            })?;
        self.validate_production_capacity_unlocked()
            .await
            .map_err(|error| {
                CatalogError::Invalid(format!(
                    "SlateDB catalog v{} cannot apply v{CATALOG_SCHEMA_VERSION} production limits: {error}",
                    state.schema_version
                ))
            })?;
        self.rebuild_branch_lineage_depths_unlocked()
            .await
            .map_err(|error| {
                CatalogError::Invalid(format!(
                    "SlateDB catalog v{} cannot establish production lineage limits for v{CATALOG_SCHEMA_VERSION}: {error}",
                    state.schema_version
                ))
            })?;
        self.snapshot_unlocked(CatalogState {
            schema_version: CATALOG_SCHEMA_VERSION,
            generation: state.generation,
        })
        .await
        .map_err(|error| {
            CatalogError::Invalid(format!(
                "SlateDB catalog v{} cannot migrate to v{CATALOG_SCHEMA_VERSION}: {error}",
                state.schema_version
            ))
        })?;
        self.db
            .put_with_options(
                STATE_KEY,
                serde_json::to_vec(&CatalogState {
                    schema_version: CATALOG_SCHEMA_VERSION,
                    generation: state.generation,
                })?,
                &slatedb::config::PutOptions::default(),
                &durable_write_options(),
            )
            .await?;
        Ok(())
    }

    async fn snapshot_unlocked(
        &self,
        state: CatalogState,
    ) -> Result<CatalogSnapshot, CatalogError> {
        let branches = self.scan_records::<BranchRecord>(BRANCH_PREFIX).await?;
        let checkpoints = self
            .scan_records::<CheckpointRecord>(CHECKPOINT_PREFIX)
            .await?;
        let branch_create_operations = self
            .scan_records::<BranchCreateOperation>(BRANCH_CREATE_OPERATION_PREFIX)
            .await?;
        let branch_delete_operations = self
            .scan_records::<BranchDeleteOperation>(BRANCH_DELETE_OPERATION_PREFIX)
            .await?;
        let gc_runs = self.scan_records::<GcRunRecord>(GC_RUN_PREFIX).await?;
        let expected_source_holds = branch_create_operations
            .iter()
            .filter(|operation| operation.phase == BranchCreatePhase::Reserved)
            .map(|operation| branch_create_source_key(operation.source_checkpoint_id, operation.id))
            .collect::<BTreeSet<_>>();
        let mut source_hold_iterator = self.db.scan_prefix(BRANCH_CREATE_SOURCE_PREFIX, ..).await?;
        let mut actual_source_holds = BTreeSet::new();
        while let Some(entry) = source_hold_iterator.next().await? {
            actual_source_holds.insert(entry.key);
        }
        if actual_source_holds != expected_source_holds {
            return Err(CatalogError::Corrupt(
                "branch-create source-hold index disagrees with incomplete operations".to_string(),
            ));
        }
        let tombstones = self
            .scan_records::<TombstoneRecord>(TOMBSTONE_PREFIX)
            .await?;
        let retired_catalog_ids = self
            .scan_records::<RetiredCatalogId>(RETIRED_CATALOG_ID_PREFIX)
            .await?;
        let leases = self.scan_records::<LeaseRecord>(LEASE_PREFIX).await?;
        let lease_tombstones = self
            .scan_records::<LeaseTombstone>(LEASE_TOMBSTONE_PREFIX)
            .await?;
        let private_epochs = self
            .scan_records::<PrivateEpochRecord>(PRIVATE_EPOCH_PREFIX)
            .await?;
        let local_gc_guards = self
            .scan_records::<LocalGcGuardRecord>(LOCAL_GC_GUARD_PREFIX)
            .await?;
        let local_gc_progress = self
            .scan_records::<LocalGcProgressRecord>(LOCAL_GC_PROGRESS_PREFIX)
            .await?;
        let snapshot = CatalogSnapshot {
            schema_version: state.schema_version,
            generation: state.generation,
            branches: branches
                .into_iter()
                .map(|record| (record.id, record))
                .collect(),
            checkpoints: checkpoints
                .into_iter()
                .map(|record| (record.id, record))
                .collect(),
            branch_create_operations: branch_create_operations
                .into_iter()
                .map(|record| (record.id, record))
                .collect(),
            branch_delete_operations: branch_delete_operations
                .into_iter()
                .map(|record| (record.id, record))
                .collect(),
            gc_runs: gc_runs
                .into_iter()
                .map(|record| (record.id, record))
                .collect(),
            leases: leases
                .into_iter()
                .map(|record| (record.id, record))
                .collect(),
            lease_tombstones: lease_tombstones
                .into_iter()
                .map(|record| (record.id, record))
                .collect(),
            tombstones: tombstones
                .into_iter()
                .map(|record| (record.id, record))
                .collect(),
            retired_catalog_ids: retired_catalog_ids
                .into_iter()
                .map(|record| (record.id, record))
                .collect(),
            private_epochs: private_epochs
                .into_iter()
                .map(|record| (record.epoch, record))
                .collect(),
            local_gc_guards: local_gc_guards
                .into_iter()
                .map(|record| (record.id, record))
                .collect(),
            local_gc_progress: local_gc_progress
                .into_iter()
                .map(|record| (record.id, record))
                .collect(),
        };
        snapshot.validate()?;
        self.audit_private_gc_blockers_unlocked(&snapshot).await?;
        Ok(snapshot)
    }

    async fn get_record<T: DeserializeOwned>(&self, key: Bytes) -> Result<Option<T>, CatalogError> {
        self.db
            .get(key)
            .await?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
            .transpose()
    }

    async fn has_local_gc_guard_for_branch(&self, branch_id: Uuid) -> Result<bool, CatalogError> {
        Ok(self
            .scan_records::<LocalGcGuardRecord>(LOCAL_GC_GUARD_PREFIX)
            .await?
            .iter()
            .any(|guard| guard.branch_id == branch_id))
    }

    async fn has_local_gc_guard_for_epoch(&self, epoch: u64) -> Result<bool, CatalogError> {
        Ok(self
            .scan_records::<LocalGcGuardRecord>(LOCAL_GC_GUARD_PREFIX)
            .await?
            .iter()
            .any(|guard| guard.epoch == epoch))
    }

    #[allow(dead_code)] // Called by crate-private lookup methods used by later API slices.
    async fn id_by_name(&self, key: Bytes) -> Result<Option<Uuid>, CatalogError> {
        self.db
            .get(key)
            .await?
            .map(|bytes| {
                std::str::from_utf8(&bytes)
                    .map_err(|error| CatalogError::Corrupt(error.to_string()))
                    .and_then(|text| {
                        Uuid::parse_str(text)
                            .map_err(|error| CatalogError::Corrupt(error.to_string()))
                    })
            })
            .transpose()
    }

    async fn scan_records<T: DeserializeOwned>(
        &self,
        prefix: &'static [u8],
    ) -> Result<Vec<T>, CatalogError> {
        let mut iterator = self.db.scan_prefix(prefix, ..).await?;
        let mut records = Vec::new();
        while let Some(entry) = iterator.next().await? {
            records.push(serde_json::from_slice(&entry.value)?);
        }
        Ok(records)
    }

    async fn validate_private_gc_owner_unlocked(
        &self,
        branch_id: Uuid,
        database_identity: &str,
    ) -> Result<BranchRecord, CatalogError> {
        let branch = self
            .get_record::<BranchRecord>(branch_key(branch_id))
            .await?
            .ok_or_else(|| {
                CatalogError::Corrupt(format!("private GC owner branch {branch_id} is missing"))
            })?;
        branch.validate()?;
        if branch.id != branch_id {
            return Err(CatalogError::Corrupt(format!(
                "private GC owner branch key {branch_id} contains {}",
                branch.id
            )));
        }
        if branch.state != BranchState::Ready
            || branch
                .root
                .as_ref()
                .is_none_or(|root| root.identity != database_identity)
        {
            return Err(CatalogError::OperationConflict(format!(
                "private GC owner branch {branch_id} is not the exact ready database"
            )));
        }
        Ok(branch)
    }

    async fn lease_branch_id_unlocked(&self, lease: &LeaseRecord) -> Result<Uuid, CatalogError> {
        match lease.subject_kind {
            LeaseSubjectKind::Branch => Ok(lease.subject_id),
            LeaseSubjectKind::Checkpoint => {
                if let Some(checkpoint) = self
                    .get_record::<CheckpointRecord>(checkpoint_key(lease.subject_id))
                    .await?
                {
                    checkpoint.validate()?;
                    return Ok(checkpoint.branch_id);
                }
                let tombstone = self
                    .get_record::<TombstoneRecord>(tombstone_key(lease.subject_id))
                    .await?
                    .filter(|record| record.kind == TombstoneKind::Checkpoint)
                    .ok_or_else(|| {
                        CatalogError::Corrupt(format!(
                            "checkpoint lease {} has no live or deleted subject",
                            lease.id
                        ))
                    })?;
                tombstone.parent_id.ok_or_else(|| {
                    CatalogError::Corrupt(format!(
                        "checkpoint lease {} tombstone lost its branch",
                        lease.id
                    ))
                })
            }
        }
    }

    async fn private_gc_branch_blockers_unlocked(
        &self,
        branch_id: Uuid,
    ) -> Result<PrivateGcBranchBlockers, CatalogError> {
        let record = self
            .get_record::<PrivateGcBranchBlockers>(private_gc_branch_blocker_key(branch_id))
            .await?
            .ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "private GC blocker record for branch {branch_id} is missing"
                ))
            })?;
        record.validate()?;
        if record.branch_id != branch_id {
            return Err(CatalogError::Corrupt(format!(
                "private GC blocker key {branch_id} contains {}",
                record.branch_id
            )));
        }
        Ok(record)
    }

    async fn private_gc_global_blockers_unlocked(
        &self,
    ) -> Result<PrivateGcGlobalBlockers, CatalogError> {
        self.get_record::<PrivateGcGlobalBlockers>(Bytes::from_static(
            PRIVATE_GC_GLOBAL_BLOCKER_KEY,
        ))
        .await?
        .ok_or_else(|| {
            CatalogError::Corrupt("private GC global blocker record is missing".to_string())
        })
    }

    async fn ensure_live_branch_capacity_unlocked(&self) -> Result<(), CatalogError> {
        let mut iterator = self.db.scan_prefix(BRANCH_PREFIX, ..).await?;
        for _ in 0..MAX_LIVE_BRANCHES {
            if iterator.next().await?.is_none() {
                return Ok(());
            }
        }
        Err(CatalogError::Capacity {
            resource: "live branch",
            limit: MAX_LIVE_BRANCHES,
        })
    }

    async fn validate_production_capacity_unlocked(&self) -> Result<(), CatalogError> {
        let mut branches = self.db.scan_prefix(BRANCH_PREFIX, ..).await?;
        let mut branch_count = 0usize;
        while branches.next().await?.is_some() {
            branch_count = branch_count
                .checked_add(1)
                .ok_or_else(|| CatalogError::Corrupt("live branch count overflow".to_string()))?;
            if branch_count > MAX_LIVE_BRANCHES {
                return Err(CatalogError::Capacity {
                    resource: "live branch",
                    limit: MAX_LIVE_BRANCHES,
                });
            }
        }
        let mut blockers = self
            .db
            .scan_prefix(PRIVATE_GC_BRANCH_BLOCKER_PREFIX, ..)
            .await?;
        while let Some(entry) = blockers.next().await? {
            let blocker = serde_json::from_slice::<PrivateGcBranchBlockers>(&entry.value)?;
            blocker.validate()?;
            if entry.key != private_gc_branch_blocker_key(blocker.branch_id) {
                return Err(CatalogError::Corrupt(format!(
                    "private GC blocker key disagrees with branch {}",
                    blocker.branch_id
                )));
            }
            if blocker.checkpoints > super::MAX_CHECKPOINTS_PER_BRANCH as u64 {
                return Err(CatalogError::Capacity {
                    resource: "checkpoint per branch",
                    limit: super::MAX_CHECKPOINTS_PER_BRANCH,
                });
            }
            if blocker.leases > MAX_ACTIVE_LEASES_PER_BRANCH as u64 {
                return Err(CatalogError::Capacity {
                    resource: "active lease per branch",
                    limit: MAX_ACTIVE_LEASES_PER_BRANCH,
                });
            }
        }
        Ok(())
    }

    async fn next_lineage_depth_unlocked(
        &self,
        parent_id: Option<Uuid>,
    ) -> Result<u16, CatalogError> {
        let Some(parent_id) = parent_id else {
            return Ok(0);
        };
        let parent = self
            .get_record::<BranchLineageDepth>(branch_lineage_depth_key(parent_id))
            .await?
            .ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "branch lineage depth for parent {parent_id} is missing"
                ))
            })?;
        parent.validate()?;
        if parent.branch_id != parent_id {
            return Err(CatalogError::Corrupt(format!(
                "branch lineage depth key {parent_id} contains {}",
                parent.branch_id
            )));
        }
        let depth = parent.depth as usize + 1;
        if depth > MAX_BRANCH_LINEAGE_DEPTH {
            return Err(CatalogError::Capacity {
                resource: "branch lineage depth",
                limit: MAX_BRANCH_LINEAGE_DEPTH,
            });
        }
        Ok(depth as u16)
    }

    async fn rebuild_branch_lineage_depths_unlocked(&self) -> Result<(), CatalogError> {
        let mut parents = BTreeMap::new();
        for branch in self.scan_records::<BranchRecord>(BRANCH_PREFIX).await? {
            branch.validate()?;
            parents.insert(branch.id, branch.parent_id);
        }
        for tombstone in self
            .scan_records::<TombstoneRecord>(TOMBSTONE_PREFIX)
            .await?
            .into_iter()
            .filter(|record| record.kind == TombstoneKind::Branch)
        {
            tombstone.validate()?;
            parents.insert(tombstone.id, tombstone.parent_id);
        }
        let mut depths = BTreeMap::new();
        for id in parents.keys().copied() {
            let mut path = Vec::new();
            let mut current = id;
            let base = loop {
                if let Some(depth) = depths.get(&current).copied() {
                    break depth;
                }
                if path.contains(&current) {
                    return Err(CatalogError::Corrupt(format!(
                        "branch lineage contains a cycle at {current}"
                    )));
                }
                path.push(current);
                match parents.get(&current).copied().flatten() {
                    Some(parent) if parents.contains_key(&parent) => current = parent,
                    Some(parent) => {
                        return Err(CatalogError::Corrupt(format!(
                            "branch lineage parent {parent} was compacted before its depth was recorded"
                        )));
                    }
                    None => break 0u16,
                }
            };
            let mut depth = base;
            while let Some(branch_id) = path.pop() {
                if parents[&branch_id].is_some() {
                    depth = depth.checked_add(1).ok_or_else(|| {
                        CatalogError::Corrupt("branch lineage depth overflow".to_string())
                    })?;
                }
                if depth as usize > MAX_BRANCH_LINEAGE_DEPTH {
                    return Err(CatalogError::Capacity {
                        resource: "branch lineage depth",
                        limit: MAX_BRANCH_LINEAGE_DEPTH,
                    });
                }
                depths.insert(branch_id, depth);
            }
        }
        let mut batch = WriteBatch::new();
        for (branch_id, depth) in depths {
            put_json(
                &mut batch,
                branch_lineage_depth_key(branch_id),
                &BranchLineageDepth { branch_id, depth },
            )?;
        }
        self.db
            .write_with_options(batch, &durable_write_options())
            .await?;
        Ok(())
    }

    async fn rebuild_private_gc_blockers_unlocked(&self) -> Result<(), CatalogError> {
        let branches = self.scan_records::<BranchRecord>(BRANCH_PREFIX).await?;
        let tombstones = self
            .scan_records::<TombstoneRecord>(TOMBSTONE_PREFIX)
            .await?;
        let mut blockers = BTreeMap::new();
        for branch in branches {
            branch.validate()?;
            blockers.insert(branch.id, PrivateGcBranchBlockers::empty(branch.id));
        }
        for tombstone in tombstones
            .into_iter()
            .filter(|record| record.kind == TombstoneKind::Branch)
        {
            tombstone.validate()?;
            blockers
                .entry(tombstone.id)
                .or_insert_with(|| PrivateGcBranchBlockers::empty(tombstone.id));
        }
        for retired in self
            .scan_records::<RetiredCatalogId>(RETIRED_CATALOG_ID_PREFIX)
            .await?
            .into_iter()
            .filter(|record| record.kind == RetiredCatalogKind::Branch)
        {
            retired.validate()?;
            blockers
                .entry(retired.id)
                .or_insert_with(|| PrivateGcBranchBlockers::empty(retired.id));
        }
        for checkpoint in self
            .scan_records::<CheckpointRecord>(CHECKPOINT_PREFIX)
            .await?
        {
            checkpoint.validate()?;
            let blocker = blockers.get_mut(&checkpoint.branch_id).ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "checkpoint {} has no blocker owner branch {}",
                    checkpoint.id, checkpoint.branch_id
                ))
            })?;
            increment_blocker(&mut blocker.checkpoints, "checkpoint blocker")?;
        }
        for lease in self.scan_records::<LeaseRecord>(LEASE_PREFIX).await? {
            lease.validate()?;
            let branch_id = self.lease_branch_id_unlocked(&lease).await?;
            let blocker = blockers.get_mut(&branch_id).ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "lease {} has no blocker owner branch {branch_id}",
                    lease.id
                ))
            })?;
            increment_blocker(&mut blocker.leases, "lease blocker")?;
        }
        for operation in self
            .scan_records::<BranchCreateOperation>(BRANCH_CREATE_OPERATION_PREFIX)
            .await?
            .into_iter()
            .filter(|operation| operation.phase != BranchCreatePhase::Published)
        {
            operation.validate()?;
            let parent_id = operation.parent_id.ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "incomplete branch create {} lost its parent",
                    operation.id
                ))
            })?;
            let blocker = blockers.get_mut(&parent_id).ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "branch create {} has no blocker parent {parent_id}",
                    operation.id
                ))
            })?;
            increment_blocker(&mut blocker.incomplete_children, "child-create blocker")?;
        }
        let mut global = PrivateGcGlobalBlockers::empty();
        for run in self.scan_records::<GcRunRecord>(GC_RUN_PREFIX).await? {
            run.validate()?;
            if run.retains_roots() {
                increment_blocker(
                    &mut global.root_retaining_gc_runs,
                    "root-retaining GC blocker",
                )?;
            }
        }

        let mut batch = WriteBatch::new();
        let mut old = self
            .db
            .scan_prefix(PRIVATE_GC_BRANCH_BLOCKER_PREFIX, ..)
            .await?;
        while let Some(entry) = old.next().await? {
            batch.delete(entry.key);
        }
        for blocker in blockers.values() {
            put_json(
                &mut batch,
                private_gc_branch_blocker_key(blocker.branch_id),
                blocker,
            )?;
        }
        put_json(
            &mut batch,
            Bytes::from_static(PRIVATE_GC_GLOBAL_BLOCKER_KEY),
            &global,
        )?;
        self.db
            .write_with_options(batch, &durable_write_options())
            .await?;
        Ok(())
    }

    async fn audit_private_gc_blockers_unlocked(
        &self,
        snapshot: &CatalogSnapshot,
    ) -> Result<(), CatalogError> {
        let mut expected = BTreeMap::new();
        for branch_id in snapshot
            .branches
            .keys()
            .copied()
            .chain(
                snapshot
                    .tombstones
                    .values()
                    .filter(|record| record.kind == TombstoneKind::Branch)
                    .map(|record| record.id),
            )
            .chain(
                snapshot
                    .retired_catalog_ids
                    .values()
                    .filter(|record| record.kind == RetiredCatalogKind::Branch)
                    .map(|record| record.id),
            )
        {
            expected
                .entry(branch_id)
                .or_insert_with(|| PrivateGcBranchBlockers::empty(branch_id));
        }
        for checkpoint in snapshot.checkpoints.values() {
            let blocker = expected.get_mut(&checkpoint.branch_id).ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "checkpoint {} has no blocker owner branch {}",
                    checkpoint.id, checkpoint.branch_id
                ))
            })?;
            increment_blocker(&mut blocker.checkpoints, "checkpoint blocker")?;
        }
        for lease in snapshot.leases.values() {
            let branch_id = match lease.subject_kind {
                LeaseSubjectKind::Branch => lease.subject_id,
                LeaseSubjectKind::Checkpoint => snapshot
                    .checkpoints
                    .get(&lease.subject_id)
                    .map(|checkpoint| checkpoint.branch_id)
                    .or_else(|| {
                        snapshot
                            .tombstones
                            .get(&lease.subject_id)
                            .filter(|record| record.kind == TombstoneKind::Checkpoint)
                            .and_then(|record| record.parent_id)
                    })
                    .ok_or_else(|| {
                        CatalogError::Corrupt(format!(
                            "checkpoint lease {} lost its branch",
                            lease.id
                        ))
                    })?,
            };
            let blocker = expected.get_mut(&branch_id).ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "lease {} has no blocker owner branch {branch_id}",
                    lease.id
                ))
            })?;
            increment_blocker(&mut blocker.leases, "lease blocker")?;
        }
        for operation in snapshot
            .branch_create_operations
            .values()
            .filter(|operation| operation.phase != BranchCreatePhase::Published)
        {
            let parent_id = operation.parent_id.ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "incomplete branch create {} lost its parent",
                    operation.id
                ))
            })?;
            let blocker = expected.get_mut(&parent_id).ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "branch create {} has no blocker parent {parent_id}",
                    operation.id
                ))
            })?;
            increment_blocker(&mut blocker.incomplete_children, "child-create blocker")?;
        }

        let mut actual = BTreeMap::new();
        let mut iterator = self
            .db
            .scan_prefix(PRIVATE_GC_BRANCH_BLOCKER_PREFIX, ..)
            .await?;
        while let Some(entry) = iterator.next().await? {
            let blocker = serde_json::from_slice::<PrivateGcBranchBlockers>(&entry.value)?;
            blocker.validate()?;
            if entry.key != private_gc_branch_blocker_key(blocker.branch_id)
                || actual.insert(blocker.branch_id, blocker).is_some()
            {
                return Err(CatalogError::Corrupt(
                    "private GC branch blocker index has a wrong or duplicate key".to_string(),
                ));
            }
        }
        if actual != expected {
            return Err(CatalogError::Corrupt(
                "private GC branch blocker index disagrees with roots".to_string(),
            ));
        }
        let expected_global = PrivateGcGlobalBlockers {
            root_retaining_gc_runs: snapshot
                .gc_runs
                .values()
                .filter(|run| run.retains_roots())
                .count()
                .try_into()
                .map_err(|_| {
                    CatalogError::Corrupt("root-retaining GC count overflow".to_string())
                })?,
        };
        if self.private_gc_global_blockers_unlocked().await? != expected_global {
            return Err(CatalogError::Corrupt(
                "private GC global blocker index disagrees with GC runs".to_string(),
            ));
        }
        Ok(())
    }

    async fn apply_unlocked(&self, mutation: CatalogMutation) -> Result<u64, CatalogError> {
        let state = self.state_unlocked().await?;
        let next_generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| CatalogError::Corrupt("catalog generation overflow".to_string()))?;
        let mut batch = WriteBatch::new();

        match mutation {
            CatalogMutation::AcquireLocalGcGuard(guard) => {
                guard.validate()?;
                ensure_initial_revision(guard.revision)?;
                if let Some(existing) = self
                    .get_record::<LocalGcGuardRecord>(local_gc_guard_key(guard.id))
                    .await?
                {
                    if existing == guard {
                        return Ok(state.generation);
                    }
                    return Err(CatalogError::OperationConflict(guard.id.to_string()));
                }
                ensure_resource_id_available(self.db.as_ref(), guard.id).await?;
                if self
                    .scan_records::<LocalGcGuardRecord>(LOCAL_GC_GUARD_PREFIX)
                    .await?
                    .iter()
                    .any(|existing| existing.epoch == guard.epoch)
                {
                    return Err(CatalogError::OperationConflict(format!(
                        "private epoch {} already has a local GC guard",
                        guard.epoch
                    )));
                }
                let epoch = self
                    .get_record::<PrivateEpochRecord>(private_epoch_key(guard.epoch))
                    .await?
                    .ok_or_else(|| {
                        CatalogError::NotFound(format!("private epoch {}", guard.epoch))
                    })?;
                epoch.validate()?;
                if epoch.branch_id != guard.branch_id
                    || epoch.revision != guard.epoch_revision
                    || epoch.state != PrivateEpochState::SealedPrivate
                    || guard.created_at < epoch.updated_at
                {
                    return Err(CatalogError::OperationConflict(guard.id.to_string()));
                }
                self.validate_private_gc_owner_unlocked(guard.branch_id, &epoch.database_identity)
                    .await?;
                let branch_blockers = self
                    .private_gc_branch_blockers_unlocked(guard.branch_id)
                    .await?;
                let global_blockers = self.private_gc_global_blockers_unlocked().await?;
                if !branch_blockers.is_clear() || global_blockers.root_retaining_gc_runs != 0 {
                    return Err(CatalogError::OperationConflict(format!(
                        "local GC guard {} has an authoritative root blocker",
                        guard.id
                    )));
                }
                put_json(&mut batch, local_gc_guard_key(guard.id), &guard)?;
            }
            CatalogMutation::PublishLocalGcProgress(record) => {
                record.validate()?;
                let existing = self
                    .get_record::<LocalGcProgressRecord>(local_gc_progress_key(record.id))
                    .await?;
                if existing.as_ref() == Some(&record) {
                    return Ok(state.generation);
                }
                let guard = self
                    .get_record::<LocalGcGuardRecord>(local_gc_guard_key(record.id))
                    .await?
                    .ok_or_else(|| {
                        CatalogError::OperationConflict(format!(
                            "local GC guard {} is not active",
                            record.id
                        ))
                    })?;
                if !record.matches_guard(&guard) {
                    return Err(CatalogError::OperationConflict(record.id.to_string()));
                }
                match existing {
                    None => {
                        ensure_initial_revision(record.revision)?;
                        if record.next_candidate != 0
                            || record.deleted_objects != 0
                            || record.deleted_bytes != 0
                            || record.already_absent != 0
                            || record.completed_at.is_some()
                            || record.started_at != record.updated_at
                        {
                            return Err(CatalogError::OperationConflict(record.id.to_string()));
                        }
                    }
                    Some(previous) => {
                        let next_revision = previous.revision.checked_add(1).ok_or_else(|| {
                            CatalogError::Corrupt("local GC progress revision overflow".to_string())
                        })?;
                        ensure_expected_revision(next_revision, record.revision)?;
                        if previous.completed_at.is_some()
                            || record.branch_id != previous.branch_id
                            || record.epoch != previous.epoch
                            || record.epoch_revision != previous.epoch_revision
                            || record.candidate_count != previous.candidate_count
                            || record.candidate_digest != previous.candidate_digest
                            || record.started_at != previous.started_at
                            || record.updated_at < previous.updated_at
                            || record.next_candidate <= previous.next_candidate
                            || record.deleted_objects < previous.deleted_objects
                            || record.deleted_bytes < previous.deleted_bytes
                            || record.already_absent < previous.already_absent
                        {
                            return Err(CatalogError::OperationConflict(record.id.to_string()));
                        }
                    }
                }
                put_json(&mut batch, local_gc_progress_key(record.id), &record)?;
                if record.completed_at.is_some() {
                    batch.delete(local_gc_guard_key(record.id));
                }
            }
            CatalogMutation::RegisterPrivateEpoch(record) => {
                record.validate()?;
                ensure_initial_revision(record.revision)?;
                if record.state != PrivateEpochState::Open
                    || record.sealed_at.is_some()
                    || record.exposed_at.is_some()
                    || record.updated_at != record.created_at
                {
                    return Err(CatalogError::Invalid(
                        "new private epoch must begin open at revision one".to_string(),
                    ));
                }
                if let Some(existing) = self
                    .get_record::<PrivateEpochRecord>(private_epoch_key(record.epoch))
                    .await?
                {
                    if existing == record {
                        return Ok(state.generation);
                    }
                    return Err(CatalogError::OperationConflict(format!(
                        "private epoch {}",
                        record.epoch
                    )));
                }
                if !self
                    .get_record::<BranchRecord>(branch_key(record.branch_id))
                    .await?
                    .is_some_and(|branch| branch.state == BranchState::Ready)
                {
                    return Err(CatalogError::NotFound(format!(
                        "ready private-epoch branch {}",
                        record.branch_id
                    )));
                }
                put_json(&mut batch, private_epoch_key(record.epoch), &record)?;
            }
            CatalogMutation::SealPrivateEpoch {
                epoch,
                branch_id,
                expected_revision,
                next_epoch,
                expected_next_revision,
                sealed_at,
            } => {
                validate_timestamp(sealed_at, "private epoch sealed_at")?;
                let mut record = self
                    .get_record::<PrivateEpochRecord>(private_epoch_key(epoch))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(format!("private epoch {epoch}")))?;
                record.validate()?;
                if record.state == PrivateEpochState::SealedPrivate
                    && record.branch_id == branch_id
                    && record.sealed_at == Some(sealed_at)
                {
                    return Ok(state.generation);
                }
                ensure_expected_revision(expected_revision, record.revision)?;
                let next = self
                    .get_record::<PrivateEpochRecord>(private_epoch_key(next_epoch))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(format!("private epoch {next_epoch}")))?;
                next.validate()?;
                ensure_expected_revision(expected_next_revision, next.revision)?;
                if record.branch_id != branch_id
                    || record.state != PrivateEpochState::Open
                    || next_epoch == epoch
                    || next.branch_id != branch_id
                    || next.state != PrivateEpochState::Open
                    || next.pool_id != record.pool_id
                    || next.database_identity != record.database_identity
                    || sealed_at < record.updated_at
                    || sealed_at < next.created_at
                    || !self
                        .get_record::<BranchRecord>(branch_key(branch_id))
                        .await?
                        .is_some_and(|branch| branch.state == BranchState::Ready)
                {
                    return Err(CatalogError::OperationConflict(format!(
                        "private epoch {epoch}"
                    )));
                }
                if self.has_local_gc_guard_for_epoch(epoch).await? {
                    return Err(CatalogError::OperationConflict(format!(
                        "private epoch {epoch} has an active local GC guard"
                    )));
                }
                record.revision = record.revision.checked_add(1).ok_or_else(|| {
                    CatalogError::Corrupt("private epoch revision overflow".to_string())
                })?;
                record.state = PrivateEpochState::SealedPrivate;
                record.updated_at = sealed_at;
                record.sealed_at = Some(sealed_at);
                record.validate()?;
                put_json(&mut batch, private_epoch_key(epoch), &record)?;
            }
            CatalogMutation::ExposePrivateEpoch {
                epoch,
                branch_id,
                expected_revision,
                exposed_at,
            } => {
                validate_timestamp(exposed_at, "private epoch exposed_at")?;
                let mut record = self
                    .get_record::<PrivateEpochRecord>(private_epoch_key(epoch))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(format!("private epoch {epoch}")))?;
                record.validate()?;
                if record.state == PrivateEpochState::Exposed
                    && record.branch_id == branch_id
                    && record.exposed_at == Some(exposed_at)
                {
                    return Ok(state.generation);
                }
                ensure_expected_revision(expected_revision, record.revision)?;
                if record.branch_id != branch_id
                    || !matches!(
                        record.state,
                        PrivateEpochState::Open | PrivateEpochState::SealedPrivate
                    )
                    || exposed_at < record.updated_at
                    || !self
                        .get_record::<BranchRecord>(branch_key(branch_id))
                        .await?
                        .is_some_and(|branch| branch.state == BranchState::Ready)
                {
                    return Err(CatalogError::OperationConflict(format!(
                        "private epoch {epoch}"
                    )));
                }
                if self.has_local_gc_guard_for_epoch(epoch).await? {
                    return Err(CatalogError::OperationConflict(format!(
                        "private epoch {epoch} has an active local GC guard"
                    )));
                }
                record.revision = record.revision.checked_add(1).ok_or_else(|| {
                    CatalogError::Corrupt("private epoch revision overflow".to_string())
                })?;
                record.state = PrivateEpochState::Exposed;
                record.updated_at = exposed_at;
                record.exposed_at = Some(exposed_at);
                record.validate()?;
                put_json(&mut batch, private_epoch_key(epoch), &record)?;
            }
            CatalogMutation::StartBranchDelete { operation } => {
                operation.validate()?;
                ensure_initial_revision(operation.revision)?;
                if operation.phase != BranchDeletePhase::Draining
                    || operation.updated_at != operation.created_at
                {
                    return Err(CatalogError::Invalid(
                        "new branch deletion must begin in draining phase".to_string(),
                    ));
                }
                if let Some(existing) = self
                    .get_record::<BranchDeleteOperation>(branch_delete_operation_key(operation.id))
                    .await?
                {
                    if branch_delete_inputs_equal(&existing, &operation) {
                        return Ok(state.generation);
                    }
                    return Err(CatalogError::OperationConflict(operation.id.to_string()));
                }
                ensure_resource_id_available(self.db.as_ref(), operation.id).await?;
                let mut branch = self
                    .get_record::<BranchRecord>(branch_key(operation.branch_id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(operation.branch_id.to_string()))?;
                if branch.state != BranchState::Ready
                    || branch.name != operation.branch_name
                    || branch.root.as_ref() != Some(&operation.root)
                    || branch.parent_id != operation.parent_id
                    || branch.origin_checkpoint_id != operation.origin_checkpoint_id
                {
                    return Err(CatalogError::OperationConflict(operation.id.to_string()));
                }
                ensure_expected_revision(operation.expected_branch_revision, branch.revision)?;
                if self
                    .has_local_gc_guard_for_branch(operation.branch_id)
                    .await?
                {
                    return Err(CatalogError::OperationConflict(format!(
                        "branch {} has an active local GC guard",
                        operation.branch_id
                    )));
                }
                if operation.created_at < branch.created_at
                    || operation.created_at < branch.updated_at
                {
                    return Err(CatalogError::Invalid(
                        "branch deletion time cannot move backwards".to_string(),
                    ));
                }
                for mut epoch in self
                    .scan_records::<PrivateEpochRecord>(PRIVATE_EPOCH_PREFIX)
                    .await?
                    .into_iter()
                    .filter(|epoch| {
                        epoch.branch_id == branch.id && epoch.state != PrivateEpochState::Exposed
                    })
                {
                    epoch.validate()?;
                    if operation.created_at < epoch.updated_at {
                        return Err(CatalogError::Invalid(
                            "branch deletion cannot precede private epoch state".to_string(),
                        ));
                    }
                    epoch.revision = epoch.revision.checked_add(1).ok_or_else(|| {
                        CatalogError::Corrupt("private epoch revision overflow".to_string())
                    })?;
                    epoch.state = PrivateEpochState::Exposed;
                    epoch.updated_at = operation.created_at;
                    epoch.exposed_at = Some(operation.created_at);
                    epoch.validate()?;
                    put_json(&mut batch, private_epoch_key(epoch.epoch), &epoch)?;
                }
                branch.revision = branch
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| CatalogError::Corrupt("branch revision overflow".to_string()))?;
                branch.state = BranchState::Deleting;
                branch.updated_at = operation.created_at;
                batch.delete(branch_name_key(&branch.name));
                put_json(&mut batch, branch_key(branch.id), &branch)?;
                put_json(
                    &mut batch,
                    branch_delete_operation_key(operation.id),
                    &operation,
                )?;
            }
            CatalogMutation::FinalizeBranchDelete {
                operation_id,
                expected_revision,
                deleted_at,
            } => {
                validate_timestamp(deleted_at, "branch deleted_at")?;
                let mut operation = self
                    .get_record::<BranchDeleteOperation>(branch_delete_operation_key(operation_id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(operation_id.to_string()))?;
                if operation.phase == BranchDeletePhase::Published {
                    if expected_revision.checked_add(1) == Some(operation.revision) {
                        return Ok(state.generation);
                    }
                    return Err(CatalogError::RevisionConflict {
                        expected: expected_revision,
                        actual: operation.revision,
                    });
                }
                ensure_expected_revision(expected_revision, operation.revision)?;
                if deleted_at < operation.updated_at {
                    return Err(CatalogError::Invalid(
                        "branch deletion time cannot move backwards".to_string(),
                    ));
                }
                let branch = self
                    .get_record::<BranchRecord>(branch_key(operation.branch_id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(operation.branch_id.to_string()))?;
                let deleting_revision = operation
                    .expected_branch_revision
                    .checked_add(1)
                    .ok_or_else(|| {
                        CatalogError::Corrupt("deleted branch revision overflow".to_string())
                    })?;
                if branch.state != BranchState::Deleting
                    || branch.name != operation.branch_name
                    || branch.root.as_ref() != Some(&operation.root)
                    || branch.parent_id != operation.parent_id
                    || branch.origin_checkpoint_id != operation.origin_checkpoint_id
                    || branch.revision != deleting_revision
                {
                    return Err(CatalogError::OperationConflict(operation_id.to_string()));
                }
                let leases = self.scan_records::<LeaseRecord>(LEASE_PREFIX).await?;
                if leases.iter().any(|lease| {
                    lease.subject_kind == LeaseSubjectKind::Branch
                        && lease.subject_id == operation.branch_id
                        && lease.access_mode == LeaseAccessMode::Write
                }) {
                    return Err(CatalogError::WriterLeaseActive(operation.branch_id));
                }
                ensure_absent(
                    self.db.as_ref(),
                    tombstone_key(operation.branch_id),
                    &operation.branch_id.to_string(),
                )
                .await?;
                batch.delete(branch_key(operation.branch_id));
                put_json(
                    &mut batch,
                    tombstone_key(operation.branch_id),
                    &TombstoneRecord {
                        id: operation.branch_id,
                        kind: TombstoneKind::Branch,
                        name: operation.branch_name.clone(),
                        parent_id: operation.parent_id,
                        origin_checkpoint_id: operation.origin_checkpoint_id,
                        created_at: branch.created_at,
                        deleted_revision: Some(operation.expected_branch_revision),
                        deletion_operation_id: Some(operation.id),
                        deleted_generation: next_generation,
                        deleted_at,
                    },
                )?;
                operation.revision = operation.revision.checked_add(1).ok_or_else(|| {
                    CatalogError::Corrupt("branch delete operation revision overflow".to_string())
                })?;
                operation.phase = BranchDeletePhase::Published;
                operation.updated_at = deleted_at;
                put_json(
                    &mut batch,
                    branch_delete_operation_key(operation.id),
                    &operation,
                )?;
            }
            CatalogMutation::AcquireLease {
                expected_subject_revision,
                lease,
            } => {
                lease.validate()?;
                ensure_initial_revision(lease.revision)?;
                if let Some(existing) = self.get_record::<LeaseRecord>(lease_key(lease.id)).await? {
                    if existing.subject_kind == lease.subject_kind
                        && existing.subject_id == lease.subject_id
                        && existing.root == lease.root
                        && existing.access_mode == lease.access_mode
                        && existing.token_hash == lease.token_hash
                        && existing.revision == 1
                        && existing.expires_at - existing.updated_at
                            == lease.expires_at - lease.updated_at
                    {
                        return Ok(state.generation);
                    }
                    return Err(CatalogError::OperationConflict(lease.id.to_string()));
                }
                ensure_resource_id_available(self.db.as_ref(), lease.id).await?;
                ensure_absent(
                    self.db.as_ref(),
                    lease_tombstone_key(lease.id),
                    &lease.id.to_string(),
                )
                .await?;
                let lease_branch_id = match lease.subject_kind {
                    LeaseSubjectKind::Branch => {
                        let branch = self
                            .get_record::<BranchRecord>(branch_key(lease.subject_id))
                            .await?
                            .ok_or_else(|| CatalogError::NotFound(lease.subject_id.to_string()))?;
                        ensure_expected_revision(expected_subject_revision, branch.revision)?;
                        if branch.state != BranchState::Ready
                            || branch.root.as_ref() != Some(&lease.root)
                        {
                            return Err(CatalogError::OperationConflict(lease.id.to_string()));
                        }
                        branch.id
                    }
                    LeaseSubjectKind::Checkpoint => {
                        let checkpoint = self
                            .get_record::<CheckpointRecord>(checkpoint_key(lease.subject_id))
                            .await?
                            .ok_or_else(|| CatalogError::NotFound(lease.subject_id.to_string()))?;
                        ensure_expected_revision(expected_subject_revision, checkpoint.revision)?;
                        if checkpoint.root != lease.root
                            || lease.access_mode != LeaseAccessMode::Read
                        {
                            return Err(CatalogError::OperationConflict(lease.id.to_string()));
                        }
                        checkpoint.branch_id
                    }
                };
                if self.has_local_gc_guard_for_branch(lease_branch_id).await? {
                    return Err(CatalogError::OperationConflict(format!(
                        "branch {lease_branch_id} has an active local GC guard"
                    )));
                }
                let mut blockers = self
                    .private_gc_branch_blockers_unlocked(lease_branch_id)
                    .await?;
                if blockers.leases >= MAX_ACTIVE_LEASES_PER_BRANCH as u64 {
                    return Err(CatalogError::Capacity {
                        resource: "active lease per branch",
                        limit: MAX_ACTIVE_LEASES_PER_BRANCH,
                    });
                }
                increment_blocker(&mut blockers.leases, "lease blocker")?;
                put_json(&mut batch, lease_key(lease.id), &lease)?;
                put_json(
                    &mut batch,
                    private_gc_branch_blocker_key(lease_branch_id),
                    &blockers,
                )?;
            }
            CatalogMutation::RenewLease {
                id,
                expected_revision,
                token_hash,
                renewed_at,
                expires_at,
            } => {
                validate_timestamp(renewed_at, "lease renewed_at")?;
                validate_timestamp(expires_at, "lease expires_at")?;
                let mut lease = self
                    .get_record::<LeaseRecord>(lease_key(id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(id.to_string()))?;
                let retry_revision = expected_revision.checked_add(1).ok_or_else(|| {
                    CatalogError::Invalid("lease expected revision overflow".to_string())
                })?;
                let subject_is_mountable = match lease.subject_kind {
                    LeaseSubjectKind::Branch => self
                        .get_record::<BranchRecord>(branch_key(lease.subject_id))
                        .await?
                        .is_some_and(|branch| {
                            branch.state == BranchState::Ready
                                && branch.root.as_ref() == Some(&lease.root)
                        }),
                    LeaseSubjectKind::Checkpoint => self
                        .get_record::<CheckpointRecord>(checkpoint_key(lease.subject_id))
                        .await?
                        .is_some_and(|checkpoint| checkpoint.root == lease.root),
                };
                if !subject_is_mountable {
                    return Err(CatalogError::OperationConflict(id.to_string()));
                }
                if lease.revision == retry_revision
                    && lease.token_hash == token_hash
                    && lease.expires_at - lease.updated_at == expires_at - renewed_at
                    && renewed_at < lease.expires_at
                {
                    return Ok(state.generation);
                }
                ensure_expected_revision(expected_revision, lease.revision)?;
                if lease.token_hash != token_hash
                    || renewed_at >= lease.expires_at
                    || expires_at <= renewed_at
                    || renewed_at < lease.updated_at
                    || expires_at < lease.expires_at
                    || expires_at - renewed_at > super::lease::MAX_LEASE_DURATION
                {
                    return Err(CatalogError::OperationConflict(id.to_string()));
                }
                lease.revision = lease
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| CatalogError::Corrupt("lease revision overflow".to_string()))?;
                lease.updated_at = renewed_at;
                lease.expires_at = expires_at;
                put_json(&mut batch, lease_key(id), &lease)?;
            }
            CatalogMutation::EndLease {
                id,
                expected_revision,
                token_hash,
                ended_at,
            } => {
                validate_timestamp(ended_at, "lease ended_at")?;
                if let Some(tombstone) = self
                    .get_record::<LeaseTombstone>(lease_tombstone_key(id))
                    .await?
                {
                    if tombstone.token_hash == token_hash {
                        return Ok(state.generation);
                    }
                    return Err(CatalogError::OperationConflict(id.to_string()));
                }
                let lease = self
                    .get_record::<LeaseRecord>(lease_key(id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(id.to_string()))?;
                ensure_expected_revision(expected_revision, lease.revision)?;
                if lease.token_hash != token_hash {
                    return Err(CatalogError::OperationConflict(id.to_string()));
                }
                if ended_at < lease.issued_at {
                    return Err(CatalogError::Invalid(
                        "lease release cannot precede issuance".to_string(),
                    ));
                }
                let lease_branch_id = self.lease_branch_id_unlocked(&lease).await?;
                let mut blockers = self
                    .private_gc_branch_blockers_unlocked(lease_branch_id)
                    .await?;
                decrement_blocker(&mut blockers.leases, "lease blocker")?;
                batch.delete(lease_key(id));
                put_json(
                    &mut batch,
                    private_gc_branch_blocker_key(lease_branch_id),
                    &blockers,
                )?;
                put_json(
                    &mut batch,
                    lease_tombstone_key(id),
                    &LeaseTombstone {
                        id,
                        token_hash,
                        ended_at,
                    },
                )?;
            }
            CatalogMutation::ExpireLease {
                id,
                expected_revision,
                observed_at,
            } => {
                validate_timestamp(observed_at, "lease expiry observation")?;
                if self.db.get(lease_tombstone_key(id)).await?.is_some() {
                    return Ok(state.generation);
                }
                let lease = self
                    .get_record::<LeaseRecord>(lease_key(id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(id.to_string()))?;
                ensure_expected_revision(expected_revision, lease.revision)?;
                let retention_deadline = lease
                    .expires_at
                    .checked_add_signed(LEASE_CLOCK_SKEW)
                    .ok_or_else(|| {
                        CatalogError::Invalid("lease retention deadline overflow".to_string())
                    })?;
                if observed_at < retention_deadline {
                    return Err(CatalogError::OperationConflict(id.to_string()));
                }
                let lease_branch_id = self.lease_branch_id_unlocked(&lease).await?;
                let mut blockers = self
                    .private_gc_branch_blockers_unlocked(lease_branch_id)
                    .await?;
                decrement_blocker(&mut blockers.leases, "lease blocker")?;
                batch.delete(lease_key(id));
                put_json(
                    &mut batch,
                    private_gc_branch_blocker_key(lease_branch_id),
                    &blockers,
                )?;
                put_json(
                    &mut batch,
                    lease_tombstone_key(id),
                    &LeaseTombstone {
                        id,
                        token_hash: lease.token_hash,
                        ended_at: observed_at,
                    },
                )?;
            }
            CatalogMutation::ReserveBranchCreate { branch, operation } => {
                branch.validate()?;
                operation.validate()?;
                ensure_initial_revision(branch.revision)?;
                ensure_initial_revision(operation.revision)?;
                if branch.state != BranchState::Creating
                    || branch.root.is_some()
                    || operation.phase != BranchCreatePhase::Reserved
                    || operation.destination_root.is_some()
                    || branch.id != operation.destination_id
                    || branch.name != operation.destination_name
                    || branch.parent_id != operation.parent_id
                    || branch.origin_checkpoint_id != Some(operation.source_checkpoint_id)
                    || branch.created_at != operation.created_at
                    || branch.updated_at != operation.updated_at
                    || operation.updated_at != operation.created_at
                {
                    return Err(CatalogError::Invalid(
                        "branch reservation and create operation disagree".to_string(),
                    ));
                }
                if let Some(existing) = self
                    .get_record::<BranchCreateOperation>(branch_create_operation_key(operation.id))
                    .await?
                {
                    if existing.immutable_inputs_equal(&operation) {
                        return Ok(state.generation);
                    }
                    return Err(CatalogError::OperationConflict(operation.id.to_string()));
                }
                ensure_resource_id_available(self.db.as_ref(), operation.id).await?;
                ensure_resource_id_available(self.db.as_ref(), branch.id).await?;
                ensure_absent(
                    self.db.as_ref(),
                    branch_name_key(&branch.name),
                    &branch.name,
                )
                .await?;
                let source = self
                    .get_record::<CheckpointRecord>(checkpoint_key(operation.source_checkpoint_id))
                    .await?
                    .ok_or_else(|| {
                        CatalogError::NotFound(operation.source_checkpoint_id.to_string())
                    })?;
                if source.root != operation.source_root
                    || operation.parent_id != Some(source.branch_id)
                {
                    return Err(CatalogError::OperationConflict(format!(
                        "source checkpoint {} identity changed",
                        source.id
                    )));
                }
                if self.has_local_gc_guard_for_branch(source.branch_id).await? {
                    return Err(CatalogError::OperationConflict(format!(
                        "branch {} has an active local GC guard",
                        source.branch_id
                    )));
                }
                if let Some(parent_id) = operation.parent_id {
                    ensure_known_resource(self.db.as_ref(), parent_id, TombstoneKind::Branch)
                        .await?;
                    let mut parent_blockers =
                        self.private_gc_branch_blockers_unlocked(parent_id).await?;
                    increment_blocker(
                        &mut parent_blockers.incomplete_children,
                        "child-create blocker",
                    )?;
                    put_json(
                        &mut batch,
                        private_gc_branch_blocker_key(parent_id),
                        &parent_blockers,
                    )?;
                }
                let lineage_depth = self
                    .next_lineage_depth_unlocked(operation.parent_id)
                    .await?;
                self.ensure_live_branch_capacity_unlocked().await?;
                put_json(&mut batch, branch_key(branch.id), &branch)?;
                put_json(
                    &mut batch,
                    branch_lineage_depth_key(branch.id),
                    &BranchLineageDepth {
                        branch_id: branch.id,
                        depth: lineage_depth,
                    },
                )?;
                batch.put(branch_name_key(&branch.name), branch.id.to_string());
                put_json(
                    &mut batch,
                    private_gc_branch_blocker_key(branch.id),
                    &PrivateGcBranchBlockers::empty(branch.id),
                )?;
                put_json(
                    &mut batch,
                    branch_create_operation_key(operation.id),
                    &operation,
                )?;
                batch.put(
                    branch_create_source_key(operation.source_checkpoint_id, operation.id),
                    operation.destination_id.to_string(),
                );
            }
            CatalogMutation::RecordBranchCreateRoot {
                operation_id,
                expected_revision,
                destination_root,
                updated_at,
            } => {
                validate_root(&destination_root)?;
                validate_timestamp(updated_at, "branch root-created updated_at")?;
                let mut operation = self
                    .get_record::<BranchCreateOperation>(branch_create_operation_key(operation_id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(operation_id.to_string()))?;
                if operation.phase != BranchCreatePhase::Reserved {
                    if operation.destination_root.as_ref() == Some(&destination_root) {
                        return Ok(state.generation);
                    }
                    return Err(CatalogError::OperationConflict(operation_id.to_string()));
                }
                ensure_expected_revision(expected_revision, operation.revision)?;
                if updated_at < operation.created_at || updated_at < operation.updated_at {
                    return Err(CatalogError::Invalid(
                        "branch root-created time cannot move backwards".to_string(),
                    ));
                }
                operation.revision = operation.revision.checked_add(1).ok_or_else(|| {
                    CatalogError::Corrupt("branch operation revision overflow".to_string())
                })?;
                operation.phase = BranchCreatePhase::RootCreated;
                operation.destination_root = Some(destination_root);
                operation.updated_at = updated_at;
                put_json(
                    &mut batch,
                    branch_create_operation_key(operation.id),
                    &operation,
                )?;
                batch.delete(branch_create_source_key(
                    operation.source_checkpoint_id,
                    operation.id,
                ));
            }
            CatalogMutation::PublishBranchCreate {
                operation_id,
                expected_revision,
                updated_at,
            } => {
                validate_timestamp(updated_at, "branch publication updated_at")?;
                let mut operation = self
                    .get_record::<BranchCreateOperation>(branch_create_operation_key(operation_id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(operation_id.to_string()))?;
                let root = operation.destination_root.clone().ok_or_else(|| {
                    CatalogError::OperationConflict(format!(
                        "operation {operation_id} has no destination root"
                    ))
                })?;
                let mut branch = self
                    .get_record::<BranchRecord>(branch_key(operation.destination_id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(operation.destination_id.to_string()))?;
                if operation.phase == BranchCreatePhase::Published {
                    if branch.state == BranchState::Ready
                        && branch.parent_id == operation.parent_id
                        && branch.origin_checkpoint_id == Some(operation.source_checkpoint_id)
                    {
                        return Ok(state.generation);
                    }
                    return Err(CatalogError::OperationConflict(operation_id.to_string()));
                }
                if operation.phase != BranchCreatePhase::RootCreated
                    || branch.state != BranchState::Creating
                    || branch.root.is_some()
                    || branch.name != operation.destination_name
                    || branch.parent_id != operation.parent_id
                    || branch.origin_checkpoint_id != Some(operation.source_checkpoint_id)
                {
                    return Err(CatalogError::OperationConflict(operation_id.to_string()));
                }
                ensure_expected_revision(expected_revision, operation.revision)?;
                if updated_at < operation.updated_at || updated_at < branch.updated_at {
                    return Err(CatalogError::Invalid(
                        "branch publication time cannot move backwards".to_string(),
                    ));
                }
                operation.revision = operation.revision.checked_add(1).ok_or_else(|| {
                    CatalogError::Corrupt("branch operation revision overflow".to_string())
                })?;
                operation.phase = BranchCreatePhase::Published;
                operation.updated_at = updated_at;
                branch.revision = branch
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| CatalogError::Corrupt("branch revision overflow".to_string()))?;
                branch.state = BranchState::Ready;
                branch.root = Some(root);
                branch.updated_at = updated_at;
                let parent_id = operation.parent_id.ok_or_else(|| {
                    CatalogError::Corrupt(format!(
                        "branch create operation {} lost its parent",
                        operation.id
                    ))
                })?;
                let mut parent_blockers =
                    self.private_gc_branch_blockers_unlocked(parent_id).await?;
                decrement_blocker(
                    &mut parent_blockers.incomplete_children,
                    "child-create blocker",
                )?;
                put_json(
                    &mut batch,
                    branch_create_operation_key(operation.id),
                    &operation,
                )?;
                put_json(&mut batch, branch_key(branch.id), &branch)?;
                put_json(
                    &mut batch,
                    private_gc_branch_blocker_key(parent_id),
                    &parent_blockers,
                )?;
            }
            #[cfg(test)]
            CatalogMutation::CreateBranch(record) => {
                record.validate()?;
                if record.state != BranchState::Ready {
                    return Err(CatalogError::Invalid(
                        "only ready branches may use the general create mutation".to_string(),
                    ));
                }
                ensure_initial_revision(record.revision)?;
                ensure_resource_id_available(self.db.as_ref(), record.id).await?;
                if let Some(parent_id) = record.parent_id {
                    ensure_known_resource(self.db.as_ref(), parent_id, TombstoneKind::Branch)
                        .await?;
                }
                if let Some(checkpoint_id) = record.origin_checkpoint_id {
                    ensure_known_resource(
                        self.db.as_ref(),
                        checkpoint_id,
                        TombstoneKind::Checkpoint,
                    )
                    .await?;
                }
                ensure_absent(
                    self.db.as_ref(),
                    branch_name_key(&record.name),
                    &record.name,
                )
                .await?;
                let lineage_depth = self.next_lineage_depth_unlocked(record.parent_id).await?;
                self.ensure_live_branch_capacity_unlocked().await?;
                put_json(&mut batch, branch_key(record.id), &record)?;
                put_json(
                    &mut batch,
                    branch_lineage_depth_key(record.id),
                    &BranchLineageDepth {
                        branch_id: record.id,
                        depth: lineage_depth,
                    },
                )?;
                batch.put(branch_name_key(&record.name), record.id.to_string());
                put_json(
                    &mut batch,
                    private_gc_branch_blocker_key(record.id),
                    &PrivateGcBranchBlockers::empty(record.id),
                )?;
            }
            #[cfg(test)]
            CatalogMutation::ReplaceBranch {
                expected_revision,
                record,
            } => {
                record.validate()?;
                let old = self
                    .get_record::<BranchRecord>(branch_key(record.id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(record.id.to_string()))?;
                if old.state != BranchState::Ready
                    || record.state != BranchState::Ready
                    || old.state != record.state
                    || old.parent_id != record.parent_id
                    || old.origin_checkpoint_id != record.origin_checkpoint_id
                    || old.root != record.root
                {
                    return Err(CatalogError::Invalid(
                        "branch lifecycle and lineage require dedicated mutations".to_string(),
                    ));
                }
                validate_revision_change(expected_revision, old.revision, record.revision)?;
                if let Some(parent_id) = record.parent_id {
                    ensure_known_resource(self.db.as_ref(), parent_id, TombstoneKind::Branch)
                        .await?;
                }
                if let Some(checkpoint_id) = record.origin_checkpoint_id {
                    ensure_known_resource(
                        self.db.as_ref(),
                        checkpoint_id,
                        TombstoneKind::Checkpoint,
                    )
                    .await?;
                }
                if old.name != record.name {
                    ensure_absent(
                        self.db.as_ref(),
                        branch_name_key(&record.name),
                        &record.name,
                    )
                    .await?;
                    batch.delete(branch_name_key(&old.name));
                    batch.put(branch_name_key(&record.name), record.id.to_string());
                }
                put_json(&mut batch, branch_key(record.id), &record)?;
            }
            #[cfg(test)]
            CatalogMutation::CreateCheckpoint(record) => {
                record.validate()?;
                ensure_initial_revision(record.revision)?;
                if self.has_local_gc_guard_for_branch(record.branch_id).await? {
                    return Err(CatalogError::OperationConflict(format!(
                        "branch {} has an active local GC guard",
                        record.branch_id
                    )));
                }
                ensure_resource_id_available(self.db.as_ref(), record.id).await?;
                if !self
                    .get_record::<BranchRecord>(branch_key(record.branch_id))
                    .await?
                    .is_some_and(|branch| branch.state == BranchState::Ready)
                {
                    return Err(CatalogError::NotFound(format!(
                        "ready checkpoint branch {}",
                        record.branch_id
                    )));
                }
                ensure_absent(
                    self.db.as_ref(),
                    checkpoint_name_key(record.branch_id, &record.name),
                    &record.name,
                )
                .await?;
                let mut blockers = self
                    .private_gc_branch_blockers_unlocked(record.branch_id)
                    .await?;
                if blockers.checkpoints >= super::MAX_CHECKPOINTS_PER_BRANCH as u64 {
                    return Err(CatalogError::Capacity {
                        resource: "checkpoint per branch",
                        limit: super::MAX_CHECKPOINTS_PER_BRANCH,
                    });
                }
                increment_blocker(&mut blockers.checkpoints, "checkpoint blocker")?;
                put_json(&mut batch, checkpoint_key(record.id), &record)?;
                batch.put(
                    checkpoint_name_key(record.branch_id, &record.name),
                    record.id.to_string(),
                );
                put_json(
                    &mut batch,
                    private_gc_branch_blocker_key(record.branch_id),
                    &blockers,
                )?;
            }
            #[cfg(test)]
            CatalogMutation::ReplaceCheckpoint {
                expected_revision,
                record,
            } => {
                record.validate()?;
                let old = self
                    .get_record::<CheckpointRecord>(checkpoint_key(record.id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(record.id.to_string()))?;
                if old.branch_id != record.branch_id || old.root != record.root {
                    return Err(CatalogError::Invalid(
                        "checkpoint branch and immutable root cannot be replaced".to_string(),
                    ));
                }
                validate_revision_change(expected_revision, old.revision, record.revision)?;
                if !self
                    .get_record::<BranchRecord>(branch_key(record.branch_id))
                    .await?
                    .is_some_and(|branch| branch.state == BranchState::Ready)
                {
                    return Err(CatalogError::NotFound(format!(
                        "ready checkpoint branch {}",
                        record.branch_id
                    )));
                }
                if old.name != record.name || old.branch_id != record.branch_id {
                    ensure_absent(
                        self.db.as_ref(),
                        checkpoint_name_key(record.branch_id, &record.name),
                        &record.name,
                    )
                    .await?;
                    batch.delete(checkpoint_name_key(old.branch_id, &old.name));
                    batch.put(
                        checkpoint_name_key(record.branch_id, &record.name),
                        record.id.to_string(),
                    );
                }
                put_json(&mut batch, checkpoint_key(record.id), &record)?;
            }
            CatalogMutation::DeleteCheckpoint {
                id,
                expected_revision,
                name,
                deleted_at,
            } => {
                validate_name(&name)?;
                validate_timestamp(deleted_at, "checkpoint deleted_at")?;
                if has_any_key(
                    self.db.as_ref(),
                    &branch_create_source_checkpoint_prefix(id),
                )
                .await?
                {
                    return Err(CatalogError::OperationConflict(format!(
                        "checkpoint {id} is held by an incomplete branch create"
                    )));
                }
                let old = self
                    .get_record::<CheckpointRecord>(checkpoint_key(id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(id.to_string()))?;
                if old.name != name {
                    return Err(CatalogError::NotFound(format!("{name} ({id})")));
                }
                ensure_expected_revision(expected_revision, old.revision)?;
                if deleted_at < old.created_at {
                    return Err(CatalogError::Invalid(
                        "checkpoint deletion cannot precede creation".to_string(),
                    ));
                }
                ensure_absent(self.db.as_ref(), tombstone_key(id), &id.to_string()).await?;
                let mut blockers = self
                    .private_gc_branch_blockers_unlocked(old.branch_id)
                    .await?;
                decrement_blocker(&mut blockers.checkpoints, "checkpoint blocker")?;
                batch.delete(checkpoint_key(id));
                batch.delete(checkpoint_name_key(old.branch_id, &name));
                put_json(
                    &mut batch,
                    private_gc_branch_blocker_key(old.branch_id),
                    &blockers,
                )?;
                put_json(
                    &mut batch,
                    tombstone_key(id),
                    &TombstoneRecord {
                        id,
                        kind: TombstoneKind::Checkpoint,
                        name,
                        parent_id: Some(old.branch_id),
                        origin_checkpoint_id: None,
                        created_at: old.created_at,
                        deleted_revision: Some(old.revision),
                        deletion_operation_id: None,
                        deleted_generation: next_generation,
                        deleted_at,
                    },
                )?;
            }
        }

        put_json(
            &mut batch,
            Bytes::from_static(STATE_KEY),
            &CatalogState {
                schema_version: CATALOG_SCHEMA_VERSION,
                generation: next_generation,
            },
        )?;
        self.db
            .write_with_options(batch, &durable_write_options())
            .await?;
        Ok(next_generation)
    }
}

#[async_trait]
impl Catalog for SlateDbCatalog {
    async fn close(&self) -> Result<(), CatalogError> {
        SlateDbCatalog::close(self).await
    }

    async fn snapshot(&self) -> Result<CatalogSnapshot, CatalogError> {
        let _guard = self.lock.lock().await;
        let state = self.state_unlocked().await?;
        self.snapshot_unlocked(state).await
    }

    async fn branch(&self, id: Uuid) -> Result<Option<BranchRecord>, CatalogError> {
        let _guard = self.lock.lock().await;
        self.get_record(branch_key(id)).await
    }

    async fn branch_by_name(&self, name: &str) -> Result<Option<BranchRecord>, CatalogError> {
        validate_name(name)?;
        let _guard = self.lock.lock().await;
        match self.id_by_name(branch_name_key(name)).await? {
            Some(id) => self.get_record(branch_key(id)).await,
            None => Ok(None),
        }
    }

    async fn checkpoint(&self, id: Uuid) -> Result<Option<CheckpointRecord>, CatalogError> {
        let _guard = self.lock.lock().await;
        self.get_record(checkpoint_key(id)).await
    }

    async fn checkpoint_by_name(
        &self,
        branch_id: Uuid,
        name: &str,
    ) -> Result<Option<CheckpointRecord>, CatalogError> {
        validate_name(name)?;
        let _guard = self.lock.lock().await;
        match self
            .id_by_name(checkpoint_name_key(branch_id, name))
            .await?
        {
            Some(id) => self.get_record(checkpoint_key(id)).await,
            None => Ok(None),
        }
    }

    async fn branch_create_operation(
        &self,
        id: Uuid,
    ) -> Result<Option<BranchCreateOperation>, CatalogError> {
        let _guard = self.lock.lock().await;
        self.get_record(branch_create_operation_key(id)).await
    }

    async fn branch_delete_operation(
        &self,
        id: Uuid,
    ) -> Result<Option<BranchDeleteOperation>, CatalogError> {
        let _guard = self.lock.lock().await;
        self.get_record(branch_delete_operation_key(id)).await
    }

    async fn gc_run(&self, id: Uuid) -> Result<Option<GcRunRecord>, CatalogError> {
        let _guard = self.lock.lock().await;
        let run = self.get_record::<GcRunRecord>(gc_run_key(id)).await?;
        if let Some(run) = &run {
            run.validate()?;
        }
        Ok(run)
    }

    async fn private_epoch(&self, epoch: u64) -> Result<Option<PrivateEpochRecord>, CatalogError> {
        let _guard = self.lock.lock().await;
        let record = self
            .get_record::<PrivateEpochRecord>(private_epoch_key(epoch))
            .await?;
        if let Some(record) = &record {
            record.validate()?;
        }
        Ok(record)
    }

    async fn private_gc_owner_view(
        &self,
        branch_id: Uuid,
        database_identity: &str,
        epoch_limit: usize,
    ) -> Result<PrivateGcOwnerView, CatalogError> {
        let _guard = self.lock.lock().await;
        let branch = self
            .validate_private_gc_owner_unlocked(branch_id, database_identity)
            .await?;
        let mut guard_iterator = self.db.scan_prefix(LOCAL_GC_GUARD_PREFIX, ..).await?;
        let mut active_guard: Option<LocalGcGuardRecord> = None;
        while let Some(entry) = guard_iterator.next().await? {
            let guard = serde_json::from_slice::<LocalGcGuardRecord>(&entry.value)?;
            guard.validate()?;
            if guard.branch_id != branch_id {
                continue;
            }
            let epoch = self
                .get_record::<PrivateEpochRecord>(private_epoch_key(guard.epoch))
                .await?
                .ok_or_else(|| {
                    CatalogError::Corrupt(format!(
                        "local GC guard {} refers to missing epoch {}",
                        guard.id, guard.epoch
                    ))
                })?;
            epoch.validate()?;
            if epoch.branch_id != branch.id
                || guard.epoch_revision != epoch.revision
                || epoch.state != PrivateEpochState::SealedPrivate
            {
                return Err(CatalogError::Corrupt(format!(
                    "local GC guard {} lost its exact sealed owner",
                    guard.id
                )));
            }
            if epoch.database_identity == database_identity {
                let replace = active_guard.as_ref().is_none_or(|current| {
                    (guard.created_at, guard.id) < (current.created_at, current.id)
                });
                if replace {
                    active_guard = Some(guard);
                }
            }
        }
        if active_guard.is_some() {
            return Ok(PrivateGcOwnerView {
                active_guard,
                sealed_epochs: Vec::new(),
            });
        }

        let mut epoch_iterator = self.db.scan_prefix(PRIVATE_EPOCH_PREFIX, ..).await?;
        let mut sealed_epochs = Vec::with_capacity(epoch_limit.min(32));
        while sealed_epochs.len() < epoch_limit {
            let Some(entry) = epoch_iterator.next().await? else {
                break;
            };
            let epoch = serde_json::from_slice::<PrivateEpochRecord>(&entry.value)?;
            epoch.validate()?;
            if epoch.state == PrivateEpochState::SealedPrivate
                && epoch.branch_id == branch_id
                && epoch.database_identity == database_identity
            {
                sealed_epochs.push(epoch);
            }
        }
        Ok(PrivateGcOwnerView {
            active_guard: None,
            sealed_epochs,
        })
    }

    async fn private_gc_guard_view(
        &self,
        guard_id: Uuid,
        writer_epoch_id: u64,
    ) -> Result<PrivateGcGuardView, CatalogError> {
        let _guard = self.lock.lock().await;
        let guard = self
            .get_record::<LocalGcGuardRecord>(local_gc_guard_key(guard_id))
            .await?;
        let progress = self
            .get_record::<LocalGcProgressRecord>(local_gc_progress_key(guard_id))
            .await?;
        if let Some(record) = &guard {
            record.validate()?;
            if record.id != guard_id {
                return Err(CatalogError::Corrupt(format!(
                    "local GC guard key {guard_id} contains {}",
                    record.id
                )));
            }
        }
        if let Some(record) = &progress {
            record.validate()?;
            if record.id != guard_id {
                return Err(CatalogError::Corrupt(format!(
                    "local GC progress key {guard_id} contains {}",
                    record.id
                )));
            }
        }
        if let Some(progress) = &progress {
            match (progress.completed_at, guard.as_ref()) {
                (None, Some(guard)) if progress.matches_guard(guard) => {}
                (Some(_), None) => {}
                _ => {
                    return Err(CatalogError::Corrupt(format!(
                        "local GC progress {guard_id} disagrees with guard retirement"
                    )));
                }
            }
        }
        let guarded_epoch_id = match (&guard, &progress) {
            (Some(guard), Some(progress)) if guard.epoch != progress.epoch => {
                return Err(CatalogError::Corrupt(format!(
                    "local GC guard {guard_id} disagrees with its progress epoch"
                )));
            }
            (Some(guard), _) => Some(guard.epoch),
            (_, Some(progress)) => Some(progress.epoch),
            (None, None) => None,
        };
        let guarded_epoch = match guarded_epoch_id {
            Some(epoch) => {
                self.get_record::<PrivateEpochRecord>(private_epoch_key(epoch))
                    .await?
            }
            None => None,
        };
        let writer_epoch = match &guarded_epoch {
            Some(guarded) if guarded.epoch == writer_epoch_id => Some(guarded.clone()),
            _ => {
                self.get_record::<PrivateEpochRecord>(private_epoch_key(writer_epoch_id))
                    .await?
            }
        };
        if let Some(record) = &guarded_epoch {
            record.validate()?;
        }
        if let Some(record) = &writer_epoch {
            record.validate()?;
        }
        if let Some(guard) = &guard {
            let guarded = guarded_epoch.as_ref().ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "local GC guard {guard_id} refers to a missing epoch"
                ))
            })?;
            if guarded.epoch != guard.epoch
                || guarded.branch_id != guard.branch_id
                || guarded.revision != guard.epoch_revision
                || guarded.state != PrivateEpochState::SealedPrivate
            {
                return Err(CatalogError::Corrupt(format!(
                    "local GC guard {guard_id} lost its exact sealed epoch"
                )));
            }
            self.validate_private_gc_owner_unlocked(guard.branch_id, &guarded.database_identity)
                .await?;
        }
        Ok(PrivateGcGuardView {
            guard,
            progress,
            guarded_epoch,
            writer_epoch,
        })
    }

    async fn gc_blockers(&self, run_id: Uuid) -> Result<Vec<GcBlockerRecord>, CatalogError> {
        let _guard = self.lock.lock().await;
        let prefix = gc_blocker_run_prefix(run_id);
        let mut iterator = self.db.scan_prefix(&prefix, ..).await?;
        let mut blockers = Vec::new();
        while let Some(entry) = iterator.next().await? {
            let blocker = serde_json::from_slice::<GcBlockerRecord>(&entry.value)?;
            blocker.validate()?;
            if blocker.run_id != run_id {
                return Err(CatalogError::Corrupt(
                    "GC blocker key disagrees with its run".to_string(),
                ));
            }
            blockers.push(blocker);
        }
        blockers.sort_by_key(|blocker| blocker.kind);
        Ok(blockers)
    }

    async fn begin_gc_run(
        &self,
        expected_generation: u64,
        run: GcRunRecord,
    ) -> Result<(), CatalogError> {
        let _guard = self.lock.lock().await;
        run.validate()?;
        ensure_initial_revision(run.revision)?;
        if run.catalog_generation != expected_generation {
            return Err(CatalogError::Invalid(
                "GC run generation disagrees with its capture fence".to_string(),
            ));
        }
        let state = self.state_unlocked().await?;
        if let Some(existing) = self.get_record::<GcRunRecord>(gc_run_key(run.id)).await? {
            if existing == run {
                return Ok(());
            }
            return Err(CatalogError::OperationConflict(run.id.to_string()));
        }
        ensure_expected_revision(expected_generation, state.generation)?;
        ensure_resource_id_available(self.db.as_ref(), run.id).await?;
        let mut global = self.private_gc_global_blockers_unlocked().await?;
        increment_blocker(
            &mut global.root_retaining_gc_runs,
            "root-retaining GC blocker",
        )?;
        let mut batch = WriteBatch::new();
        put_json(&mut batch, gc_run_key(run.id), &run)?;
        put_json(
            &mut batch,
            Bytes::from_static(PRIVATE_GC_GLOBAL_BLOCKER_KEY),
            &global,
        )?;
        self.db
            .write_with_options(batch, &durable_write_options())
            .await?;
        Ok(())
    }

    async fn publish_gc_marks(
        &self,
        id: Uuid,
        expected_revision: u64,
        root_digest: String,
        mark_shards: Vec<GcMarkShard>,
        mark_stats: GcMarkStats,
        updated_at: DateTime<Utc>,
    ) -> Result<GcRunRecord, CatalogError> {
        let _guard = self.lock.lock().await;
        validate_timestamp(updated_at, "GC marking updated_at")?;
        let mut run = self
            .get_record::<GcRunRecord>(gc_run_key(id))
            .await?
            .ok_or_else(|| CatalogError::NotFound(id.to_string()))?;
        run.validate()?;
        if run.phase == GcRunPhase::Marking {
            if run.root_digest == root_digest
                && run.mark_shards == mark_shards
                && run.mark_stats.as_ref() == Some(&mark_stats)
            {
                return Ok(run);
            }
            return Err(CatalogError::OperationConflict(id.to_string()));
        }
        ensure_expected_revision(expected_revision, run.revision)?;
        if run.phase != GcRunPhase::Captured
            || run.root_digest != root_digest
            || updated_at < run.updated_at
        {
            return Err(CatalogError::OperationConflict(id.to_string()));
        }
        run.revision = run
            .revision
            .checked_add(1)
            .ok_or_else(|| CatalogError::Corrupt("GC run revision overflow".to_string()))?;
        run.phase = GcRunPhase::Marking;
        run.mark_shards = mark_shards;
        run.mark_stats = Some(mark_stats);
        run.updated_at = updated_at;
        run.validate()?;
        self.db
            .put_with_options(
                gc_run_key(run.id),
                serde_json::to_vec(&run)?,
                &slatedb::config::PutOptions::default(),
                &durable_write_options(),
            )
            .await?;
        Ok(run)
    }

    async fn publish_gc_report(
        &self,
        publication: GcReportPublication,
    ) -> Result<GcRunRecord, CatalogError> {
        let GcReportPublication {
            id,
            expected_revision,
            expected_generation,
            root_digest,
            candidate_shards,
            inventory_stats,
            reported_at,
        } = publication;
        let _guard = self.lock.lock().await;
        validate_timestamp(reported_at, "GC report timestamp")?;
        let state = self.state_unlocked().await?;
        let mut run = self
            .get_record::<GcRunRecord>(gc_run_key(id))
            .await?
            .ok_or_else(|| CatalogError::NotFound(id.to_string()))?;
        run.validate()?;
        if run.phase == GcRunPhase::Reported {
            if run.catalog_generation == expected_generation
                && run.root_digest == root_digest
                && run.quarantine_shards == candidate_shards
                && run.inventory_stats.as_ref() == Some(&inventory_stats)
                && run.quarantine_at == Some(reported_at)
            {
                return Ok(run);
            }
            return Err(CatalogError::OperationConflict(id.to_string()));
        }
        ensure_expected_revision(expected_revision, run.revision)?;
        ensure_expected_revision(expected_generation, state.generation)?;
        if run.phase != GcRunPhase::Marking
            || run.catalog_generation != expected_generation
            || run.root_digest != root_digest
            || run.segment_pool.is_empty()
            || reported_at < run.updated_at
        {
            return Err(CatalogError::OperationConflict(id.to_string()));
        }
        run.revision = run
            .revision
            .checked_add(1)
            .ok_or_else(|| CatalogError::Corrupt("GC run revision overflow".to_string()))?;
        run.phase = GcRunPhase::Reported;
        run.quarantine_shards = candidate_shards;
        run.inventory_stats = Some(inventory_stats);
        // This is the immutable first-observation timestamp. In report mode
        // it does not mean that quarantine was enabled.
        run.quarantine_at = Some(reported_at);
        run.updated_at = reported_at;
        run.validate()?;

        let mut global = self.private_gc_global_blockers_unlocked().await?;
        decrement_blocker(
            &mut global.root_retaining_gc_runs,
            "root-retaining GC blocker",
        )?;
        let mut batch = WriteBatch::new();
        put_json(&mut batch, gc_run_key(run.id), &run)?;
        put_json(
            &mut batch,
            Bytes::from_static(PRIVATE_GC_GLOBAL_BLOCKER_KEY),
            &global,
        )?;
        self.db
            .write_with_options(batch, &durable_write_options())
            .await?;
        Ok(run)
    }

    async fn publish_gc_quarantine(
        &self,
        publication: GcQuarantinePublication,
    ) -> Result<GcRunRecord, CatalogError> {
        let GcQuarantinePublication {
            id,
            expected_revision,
            expected_generation,
            root_digest,
            quarantine_shards,
            inventory_stats,
            quarantine_at,
        } = publication;
        let _guard = self.lock.lock().await;
        validate_timestamp(quarantine_at, "GC quarantine timestamp")?;
        let state = self.state_unlocked().await?;
        let mut run = self
            .get_record::<GcRunRecord>(gc_run_key(id))
            .await?
            .ok_or_else(|| CatalogError::NotFound(id.to_string()))?;
        run.validate()?;
        if run.phase == GcRunPhase::Quarantined {
            if run.catalog_generation == expected_generation
                && run.root_digest == root_digest
                && run.quarantine_shards == quarantine_shards
                && run.inventory_stats.as_ref() == Some(&inventory_stats)
                && run.quarantine_at == Some(quarantine_at)
            {
                return Ok(run);
            }
            return Err(CatalogError::OperationConflict(id.to_string()));
        }
        ensure_expected_revision(expected_revision, run.revision)?;
        ensure_expected_revision(expected_generation, state.generation)?;
        if run.phase != GcRunPhase::Marking
            || run.catalog_generation != expected_generation
            || run.root_digest != root_digest
            || run.segment_pool.is_empty()
            || quarantine_at < run.updated_at
        {
            return Err(CatalogError::OperationConflict(id.to_string()));
        }
        run.revision = run
            .revision
            .checked_add(1)
            .ok_or_else(|| CatalogError::Corrupt("GC run revision overflow".to_string()))?;
        run.phase = GcRunPhase::Quarantined;
        run.quarantine_shards = quarantine_shards;
        run.inventory_stats = Some(inventory_stats);
        run.quarantine_at = Some(quarantine_at);
        run.updated_at = quarantine_at;
        run.validate()?;
        self.db
            .put_with_options(
                gc_run_key(run.id),
                serde_json::to_vec(&run)?,
                &slatedb::config::PutOptions::default(),
                &durable_write_options(),
            )
            .await?;
        Ok(run)
    }

    async fn begin_gc_revalidation(
        &self,
        capture: GcRevalidationCapture,
    ) -> Result<GcRunRecord, CatalogError> {
        let GcRevalidationCapture {
            run_id,
            expected_revision,
            expected_generation,
            observation,
            updated_at,
        } = capture;
        let _guard = self.lock.lock().await;
        validate_timestamp(updated_at, "GC revalidation capture")?;
        let state = self.state_unlocked().await?;
        let mut run = self
            .get_record::<GcRunRecord>(gc_run_key(run_id))
            .await?
            .ok_or_else(|| CatalogError::NotFound(run_id.to_string()))?;
        run.validate()?;
        if matches!(run.phase, GcRunPhase::Revalidating | GcRunPhase::Validated) {
            let existing = run
                .revalidation
                .as_ref()
                .ok_or_else(|| CatalogError::Corrupt("missing GC revalidation".to_string()))?;
            if existing.id == observation.id
                && existing.catalog_generation == observation.catalog_generation
                && existing.grace_seconds == observation.grace_seconds
                && existing.not_before == observation.not_before
                && existing.inventory_cutoff == observation.inventory_cutoff
                && existing.roots == observation.roots
                && existing.root_digest == observation.root_digest
                && existing.captured_at == observation.captured_at
            {
                return Ok(run);
            }
            return Err(CatalogError::OperationConflict(run_id.to_string()));
        }
        ensure_expected_revision(expected_revision, run.revision)?;
        ensure_expected_revision(expected_generation, state.generation)?;
        if run.phase != GcRunPhase::Quarantined
            || observation.catalog_generation != expected_generation
            || !observation.mark_shards.is_empty()
            || observation.mark_stats.is_some()
            || !observation.candidate_shards.is_empty()
            || observation.stats.is_some()
            || observation.completed_at.is_some()
            || updated_at != observation.captured_at
            || updated_at < run.updated_at
        {
            return Err(CatalogError::OperationConflict(run_id.to_string()));
        }
        run.revision = run
            .revision
            .checked_add(1)
            .ok_or_else(|| CatalogError::Corrupt("GC run revision overflow".to_string()))?;
        run.phase = GcRunPhase::Revalidating;
        run.revalidation = Some(observation);
        run.updated_at = updated_at;
        run.validate()?;
        self.db
            .put_with_options(
                gc_run_key(run.id),
                serde_json::to_vec(&run)?,
                &slatedb::config::PutOptions::default(),
                &durable_write_options(),
            )
            .await?;
        Ok(run)
    }

    async fn publish_gc_revalidation(
        &self,
        publication: GcRevalidationPublication,
    ) -> Result<GcRunRecord, CatalogError> {
        let GcRevalidationPublication {
            run_id,
            expected_revision,
            expected_generation,
            observation_id,
            root_digest,
            mark_shards,
            mark_stats,
            candidate_shards,
            stats,
            completed_at,
        } = publication;
        let _guard = self.lock.lock().await;
        validate_timestamp(completed_at, "GC revalidation completion")?;
        let state = self.state_unlocked().await?;
        let mut run = self
            .get_record::<GcRunRecord>(gc_run_key(run_id))
            .await?
            .ok_or_else(|| CatalogError::NotFound(run_id.to_string()))?;
        run.validate()?;
        if run.phase == GcRunPhase::Validated {
            let existing = run
                .revalidation
                .as_ref()
                .ok_or_else(|| CatalogError::Corrupt("missing GC revalidation".to_string()))?;
            if existing.id == observation_id
                && existing.catalog_generation == expected_generation
                && existing.root_digest == root_digest
                && existing.mark_shards == mark_shards
                && existing.mark_stats.as_ref() == Some(&mark_stats)
                && existing.candidate_shards == candidate_shards
                && existing.stats.as_ref() == Some(&stats)
                && existing.completed_at == Some(completed_at)
            {
                return Ok(run);
            }
            return Err(CatalogError::OperationConflict(run_id.to_string()));
        }
        ensure_expected_revision(expected_revision, run.revision)?;
        ensure_expected_revision(expected_generation, state.generation)?;
        let observation = run
            .revalidation
            .as_mut()
            .ok_or_else(|| CatalogError::Corrupt("missing GC revalidation".to_string()))?;
        if run.phase != GcRunPhase::Revalidating
            || observation.id != observation_id
            || observation.catalog_generation != expected_generation
            || observation.root_digest != root_digest
            || completed_at < observation.captured_at
        {
            return Err(CatalogError::OperationConflict(run_id.to_string()));
        }
        observation.mark_shards = mark_shards;
        observation.mark_stats = Some(mark_stats);
        observation.candidate_shards = candidate_shards;
        observation.stats = Some(stats);
        observation.completed_at = Some(completed_at);
        run.revision = run
            .revision
            .checked_add(1)
            .ok_or_else(|| CatalogError::Corrupt("GC run revision overflow".to_string()))?;
        run.phase = GcRunPhase::Validated;
        run.updated_at = completed_at;
        run.validate()?;
        self.db
            .put_with_options(
                gc_run_key(run.id),
                serde_json::to_vec(&run)?,
                &slatedb::config::PutOptions::default(),
                &durable_write_options(),
            )
            .await?;
        Ok(run)
    }

    async fn publish_gc_deletion(
        &self,
        publication: GcDeletionPublication,
    ) -> Result<GcRunRecord, CatalogError> {
        let GcDeletionPublication {
            run_id,
            expected_revision,
            expected_generation,
            progress,
            updated_at,
        } = publication;
        let _guard = self.lock.lock().await;
        validate_timestamp(updated_at, "GC deletion progress")?;
        let state = self.state_unlocked().await?;
        let mut run = self
            .get_record::<GcRunRecord>(gc_run_key(run_id))
            .await?
            .ok_or_else(|| CatalogError::NotFound(run_id.to_string()))?;
        run.validate()?;
        if matches!(run.phase, GcRunPhase::Deleting | GcRunPhase::Completed)
            && run.deletion.as_ref() == Some(&progress)
            && run.updated_at == updated_at
        {
            return Ok(run);
        }
        ensure_expected_revision(expected_revision, run.revision)?;
        ensure_expected_revision(expected_generation, state.generation)?;
        let observation = run
            .revalidation
            .as_ref()
            .ok_or_else(|| CatalogError::Corrupt("missing GC revalidation".to_string()))?;
        if observation.catalog_generation != expected_generation || updated_at < run.updated_at {
            return Err(CatalogError::OperationConflict(run_id.to_string()));
        }
        match (&run.phase, &run.deletion) {
            (GcRunPhase::Validated, None)
                if progress.next_shard == 0
                    && progress.next_record == 0
                    && progress.deleted_objects == 0
                    && progress.deleted_bytes == 0
                    && progress.already_absent == 0
                    && progress.completed_at.is_none()
                    && progress.started_at == updated_at => {}
            (GcRunPhase::Deleting, Some(previous))
                if progress.batch_size == previous.batch_size
                    && progress.started_at == previous.started_at
                    && (progress.next_shard, progress.next_record)
                        >= (previous.next_shard, previous.next_record)
                    && progress.deleted_objects >= previous.deleted_objects
                    && progress.deleted_bytes >= previous.deleted_bytes
                    && progress.already_absent >= previous.already_absent => {}
            _ => return Err(CatalogError::OperationConflict(run_id.to_string())),
        }
        run.revision = run
            .revision
            .checked_add(1)
            .ok_or_else(|| CatalogError::Corrupt("GC run revision overflow".to_string()))?;
        run.phase = if progress.completed_at.is_some() {
            GcRunPhase::Completed
        } else {
            GcRunPhase::Deleting
        };
        run.deletion = Some(progress);
        run.updated_at = updated_at;
        run.validate()?;
        let mut batch = WriteBatch::new();
        put_json(&mut batch, gc_run_key(run.id), &run)?;
        if run.phase == GcRunPhase::Completed {
            let mut global = self.private_gc_global_blockers_unlocked().await?;
            decrement_blocker(
                &mut global.root_retaining_gc_runs,
                "root-retaining GC blocker",
            )?;
            put_json(
                &mut batch,
                Bytes::from_static(PRIVATE_GC_GLOBAL_BLOCKER_KEY),
                &global,
            )?;
        }
        self.db
            .write_with_options(batch, &durable_write_options())
            .await?;
        Ok(run)
    }

    async fn record_gc_blocker(
        &self,
        run_id: Uuid,
        kind: GcBlockerKind,
        detail: String,
        observed_at: DateTime<Utc>,
    ) -> Result<GcBlockerRecord, CatalogError> {
        let _guard = self.lock.lock().await;
        validate_timestamp(observed_at, "GC blocker observation")?;
        if self
            .get_record::<GcRunRecord>(gc_run_key(run_id))
            .await?
            .is_none()
        {
            return Err(CatalogError::NotFound(run_id.to_string()));
        }
        let key = gc_blocker_key(run_id, kind);
        let blocker = match self.get_record::<GcBlockerRecord>(key.clone()).await? {
            Some(mut existing) => {
                existing.validate()?;
                if observed_at < existing.last_observed_at {
                    return Err(CatalogError::OperationConflict(run_id.to_string()));
                }
                existing.occurrences = existing.occurrences.checked_add(1).ok_or_else(|| {
                    CatalogError::Corrupt("GC blocker occurrence overflow".to_string())
                })?;
                existing.detail = detail;
                existing.last_observed_at = observed_at;
                existing
            }
            None => GcBlockerRecord {
                run_id,
                kind,
                occurrences: 1,
                detail,
                first_observed_at: observed_at,
                last_observed_at: observed_at,
            },
        };
        blocker.validate()?;
        self.db
            .put_with_options(
                key,
                serde_json::to_vec(&blocker)?,
                &slatedb::config::PutOptions::default(),
                &durable_write_options(),
            )
            .await?;
        Ok(blocker)
    }

    async fn lease(&self, id: Uuid) -> Result<Option<LeaseRecord>, CatalogError> {
        let _guard = self.lock.lock().await;
        self.get_record(lease_key(id)).await
    }

    async fn tombstone(&self, id: Uuid) -> Result<Option<TombstoneRecord>, CatalogError> {
        let _guard = self.lock.lock().await;
        self.get_record(tombstone_key(id)).await
    }

    async fn cleanup_tombstones(
        &self,
        policy: TombstoneCleanupPolicy,
    ) -> Result<TombstoneCleanupReport, CatalogError> {
        validate_timestamp(policy.retain_after, "tombstone retention cutoff")?;
        if policy.scan_limit == 0
            || policy.scan_limit > MAX_TOMBSTONE_CLEANUP_SCAN
            || policy.compact_limit == 0
            || policy.compact_limit > policy.scan_limit
        {
            return Err(CatalogError::Invalid(format!(
                "tombstone cleanup limits must satisfy 1 <= compact <= scan <= {MAX_TOMBSTONE_CLEANUP_SCAN}"
            )));
        }

        let _guard = self.lock.lock().await;
        let state = self.state_unlocked().await?;
        let mut cursor = self
            .get_record::<TombstoneCleanupCursor>(Bytes::from_static(TOMBSTONE_CLEANUP_CURSOR_KEY))
            .await?
            .unwrap_or_default();
        if cursor.after.is_some_and(|id| id.is_nil()) {
            return Err(CatalogError::Corrupt(
                "tombstone cleanup cursor UUID is nil".to_string(),
            ));
        }
        let cursor_suffix = cursor.after.map(|id| id.to_string().into_bytes());
        let mut iterator = match cursor_suffix.as_deref() {
            Some(suffix) => {
                self.db
                    .scan_prefix(
                        TOMBSTONE_PREFIX,
                        (Bound::Excluded(suffix), Bound::<&[u8]>::Unbounded),
                    )
                    .await?
            }
            None => self.db.scan_prefix(TOMBSTONE_PREFIX, ..).await?,
        };
        let global = self.private_gc_global_blockers_unlocked().await?;
        let mut report = TombstoneCleanupReport::default();
        let mut batch = WriteBatch::new();
        let mut reached_end = false;

        while report.examined < policy.scan_limit as u64 {
            let Some(entry) = iterator.next().await? else {
                reached_end = true;
                break;
            };
            let tombstone = serde_json::from_slice::<TombstoneRecord>(&entry.value)?;
            tombstone.validate()?;
            if entry.key != tombstone_key(tombstone.id) {
                return Err(CatalogError::Corrupt(format!(
                    "tombstone key disagrees with {}",
                    tombstone.id
                )));
            }
            cursor.after = Some(tombstone.id);
            report.examined += 1;

            if tombstone.deleted_at > policy.retain_after {
                report.retained_by_age += 1;
                continue;
            }
            if global.root_retaining_gc_runs != 0 {
                report.retained_by_roots += 1;
                continue;
            }
            let owner_id = match tombstone.kind {
                TombstoneKind::Branch => tombstone.id,
                TombstoneKind::Checkpoint => match tombstone.parent_id {
                    Some(parent_id) => parent_id,
                    None => {
                        report.retained_by_dependency += 1;
                        continue;
                    }
                },
            };
            let blockers = self.private_gc_branch_blockers_unlocked(owner_id).await?;
            if blockers.leases != 0 {
                report.retained_by_roots += 1;
                continue;
            }
            if blockers.incomplete_children != 0 {
                report.retained_by_dependency += 1;
                continue;
            }

            let delete_operation = if tombstone.kind == TombstoneKind::Branch {
                match tombstone.deletion_operation_id {
                    Some(operation_id) => {
                        let operation = self
                            .get_record::<BranchDeleteOperation>(branch_delete_operation_key(
                                operation_id,
                            ))
                            .await?
                            .ok_or_else(|| {
                                CatalogError::Corrupt(format!(
                                    "branch tombstone {} lost delete operation {operation_id}",
                                    tombstone.id
                                ))
                            })?;
                        operation.validate()?;
                        if operation.phase != BranchDeletePhase::Published
                            || operation.branch_id != tombstone.id
                            || operation.branch_name != tombstone.name
                            || operation.parent_id != tombstone.parent_id
                            || operation.origin_checkpoint_id != tombstone.origin_checkpoint_id
                            || tombstone.deleted_revision
                                != Some(operation.expected_branch_revision)
                            || operation.updated_at != tombstone.deleted_at
                        {
                            return Err(CatalogError::Corrupt(format!(
                                "branch tombstone {} disagrees with delete operation {operation_id}",
                                tombstone.id
                            )));
                        }
                        Some(operation)
                    }
                    None => None,
                }
            } else {
                None
            };

            if report.compacted >= policy.compact_limit as u64 {
                report.eligible_backlog_lower_bound += 1;
                continue;
            }
            if self
                .get_record::<RetiredCatalogId>(retired_catalog_id_key(tombstone.id))
                .await?
                .is_some()
            {
                return Err(CatalogError::Corrupt(format!(
                    "catalog ID {} is both tombstoned and retired",
                    tombstone.id
                )));
            }
            put_json(
                &mut batch,
                retired_catalog_id_key(tombstone.id),
                &RetiredCatalogId {
                    id: tombstone.id,
                    kind: tombstone.kind.into(),
                },
            )?;
            batch.delete(tombstone_key(tombstone.id));
            if let Some(operation) = delete_operation {
                if self
                    .get_record::<RetiredCatalogId>(retired_catalog_id_key(operation.id))
                    .await?
                    .is_some()
                {
                    return Err(CatalogError::Corrupt(format!(
                        "branch delete operation {} is already retired",
                        operation.id
                    )));
                }
                put_json(
                    &mut batch,
                    retired_catalog_id_key(operation.id),
                    &RetiredCatalogId {
                        id: operation.id,
                        kind: RetiredCatalogKind::BranchDeleteOperation,
                    },
                )?;
                batch.delete(branch_delete_operation_key(operation.id));
            }
            report.compacted += 1;
        }

        if reached_end {
            report.cursor_wrapped = cursor.after.is_some();
            cursor.after = None;
        }
        put_json(
            &mut batch,
            Bytes::from_static(TOMBSTONE_CLEANUP_CURSOR_KEY),
            &cursor,
        )?;
        if report.compacted != 0 {
            put_json(
                &mut batch,
                Bytes::from_static(STATE_KEY),
                &CatalogState {
                    schema_version: CATALOG_SCHEMA_VERSION,
                    generation: state.generation.checked_add(1).ok_or_else(|| {
                        CatalogError::Corrupt("catalog generation overflow".to_string())
                    })?,
                },
            )?;
        }
        self.db
            .write_with_options(batch, &durable_write_options())
            .await?;
        super::record_cleanup_metrics(
            "tombstones",
            report.examined,
            report.compacted,
            0,
            report.retained_by_age + report.retained_by_roots + report.retained_by_dependency,
            report.eligible_backlog_lower_bound,
        );
        Ok(report)
    }

    async fn apply(&self, mutation: CatalogMutation) -> Result<u64, CatalogError> {
        let _guard = self.lock.lock().await;
        self.apply_unlocked(mutation).await
    }
}

fn durable_write_options() -> WriteOptions {
    WriteOptions {
        await_durable: true,
        ..Default::default()
    }
}

fn branch_key(id: Uuid) -> Bytes {
    joined_key(BRANCH_PREFIX, id.to_string().as_bytes())
}

fn branch_name_key(name: &str) -> Bytes {
    joined_key(BRANCH_NAME_PREFIX, name.as_bytes())
}

fn branch_lineage_depth_key(id: Uuid) -> Bytes {
    joined_key(BRANCH_LINEAGE_DEPTH_PREFIX, id.to_string().as_bytes())
}

fn checkpoint_key(id: Uuid) -> Bytes {
    joined_key(CHECKPOINT_PREFIX, id.to_string().as_bytes())
}

fn checkpoint_name_key(branch_id: Uuid, name: &str) -> Bytes {
    let mut suffix = branch_id.to_string().into_bytes();
    suffix.push(b'/');
    suffix.extend_from_slice(name.as_bytes());
    joined_key(CHECKPOINT_NAME_PREFIX, &suffix)
}

fn tombstone_key(id: Uuid) -> Bytes {
    joined_key(TOMBSTONE_PREFIX, id.to_string().as_bytes())
}

fn branch_create_operation_key(id: Uuid) -> Bytes {
    joined_key(BRANCH_CREATE_OPERATION_PREFIX, id.to_string().as_bytes())
}

fn branch_delete_operation_key(id: Uuid) -> Bytes {
    joined_key(BRANCH_DELETE_OPERATION_PREFIX, id.to_string().as_bytes())
}

fn gc_run_key(id: Uuid) -> Bytes {
    joined_key(GC_RUN_PREFIX, id.to_string().as_bytes())
}

fn private_epoch_key(epoch: u64) -> Bytes {
    joined_key(PRIVATE_EPOCH_PREFIX, format!("{epoch:016x}").as_bytes())
}

fn local_gc_guard_key(id: Uuid) -> Bytes {
    joined_key(LOCAL_GC_GUARD_PREFIX, id.to_string().as_bytes())
}

fn local_gc_progress_key(id: Uuid) -> Bytes {
    joined_key(LOCAL_GC_PROGRESS_PREFIX, id.to_string().as_bytes())
}

fn private_gc_branch_blocker_key(branch_id: Uuid) -> Bytes {
    joined_key(
        PRIVATE_GC_BRANCH_BLOCKER_PREFIX,
        branch_id.to_string().as_bytes(),
    )
}

fn retired_catalog_id_key(id: Uuid) -> Bytes {
    joined_key(RETIRED_CATALOG_ID_PREFIX, id.to_string().as_bytes())
}

fn gc_blocker_run_prefix(run_id: Uuid) -> Bytes {
    let mut suffix = run_id.to_string().into_bytes();
    suffix.push(b'/');
    joined_key(GC_BLOCKER_PREFIX, &suffix)
}

fn gc_blocker_key(run_id: Uuid, kind: GcBlockerKind) -> Bytes {
    let suffix = match kind {
        GcBlockerKind::MissingRoot => b"missing-root".as_slice(),
        GcBlockerKind::CorruptMetadata => b"corrupt-metadata".as_slice(),
        GcBlockerKind::GenerationChanged => b"generation-changed".as_slice(),
        GcBlockerKind::LeaseUncertainty => b"lease-uncertainty".as_slice(),
        GcBlockerKind::StorageUnavailable => b"storage-unavailable".as_slice(),
    };
    let mut key = gc_blocker_run_prefix(run_id).to_vec();
    key.extend_from_slice(suffix);
    Bytes::from(key)
}

fn lease_key(id: Uuid) -> Bytes {
    joined_key(LEASE_PREFIX, id.to_string().as_bytes())
}

fn lease_tombstone_key(id: Uuid) -> Bytes {
    joined_key(LEASE_TOMBSTONE_PREFIX, id.to_string().as_bytes())
}

fn branch_create_source_checkpoint_prefix(checkpoint_id: Uuid) -> Bytes {
    let mut suffix = checkpoint_id.to_string().into_bytes();
    suffix.push(b'/');
    joined_key(BRANCH_CREATE_SOURCE_PREFIX, &suffix)
}

fn branch_create_source_key(checkpoint_id: Uuid, operation_id: Uuid) -> Bytes {
    let mut key = branch_create_source_checkpoint_prefix(checkpoint_id).to_vec();
    key.extend_from_slice(operation_id.to_string().as_bytes());
    Bytes::from(key)
}

fn joined_key(prefix: &[u8], suffix: &[u8]) -> Bytes {
    let mut key = Vec::with_capacity(prefix.len() + suffix.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(suffix);
    Bytes::from(key)
}

fn put_json<T: Serialize>(
    batch: &mut WriteBatch,
    key: Bytes,
    value: &T,
) -> Result<(), CatalogError> {
    batch.put(key, serde_json::to_vec(value)?);
    Ok(())
}

fn increment_blocker(value: &mut u64, label: &str) -> Result<(), CatalogError> {
    *value = value
        .checked_add(1)
        .ok_or_else(|| CatalogError::Corrupt(format!("{label} count overflow")))?;
    Ok(())
}

fn decrement_blocker(value: &mut u64, label: &str) -> Result<(), CatalogError> {
    *value = value
        .checked_sub(1)
        .ok_or_else(|| CatalogError::Corrupt(format!("{label} count underflow")))?;
    Ok(())
}

async fn ensure_absent(db: &Db, key: Bytes, label: &str) -> Result<(), CatalogError> {
    if db.get(key).await?.is_some() {
        return Err(CatalogError::AlreadyExists(label.to_string()));
    }
    Ok(())
}

async fn ensure_resource_id_available(db: &Db, id: Uuid) -> Result<(), CatalogError> {
    for key in [
        branch_key(id),
        branch_lineage_depth_key(id),
        checkpoint_key(id),
        tombstone_key(id),
        branch_create_operation_key(id),
        branch_delete_operation_key(id),
        gc_run_key(id),
        lease_key(id),
        lease_tombstone_key(id),
        local_gc_guard_key(id),
        local_gc_progress_key(id),
        retired_catalog_id_key(id),
    ] {
        ensure_absent(db, key, &id.to_string()).await?;
    }
    Ok(())
}

fn branch_delete_inputs_equal(left: &BranchDeleteOperation, right: &BranchDeleteOperation) -> bool {
    left.id == right.id
        && left.branch_id == right.branch_id
        && left.branch_name == right.branch_name
        && left.expected_branch_revision == right.expected_branch_revision
        && left.root == right.root
        && left.parent_id == right.parent_id
        && left.origin_checkpoint_id == right.origin_checkpoint_id
        && left.created_at == right.created_at
}

async fn has_any_key(db: &Db, prefix: &[u8]) -> Result<bool, CatalogError> {
    let mut iterator = db.scan_prefix(prefix, ..).await?;
    Ok(iterator.next().await?.is_some())
}

async fn ensure_known_resource(
    db: &Db,
    id: Uuid,
    expected_kind: TombstoneKind,
) -> Result<(), CatalogError> {
    let live_key = match expected_kind {
        TombstoneKind::Branch => branch_key(id),
        TombstoneKind::Checkpoint => checkpoint_key(id),
    };
    if db.get(live_key).await?.is_some() {
        return Ok(());
    }
    let tombstone = db
        .get(tombstone_key(id))
        .await?
        .map(|bytes| serde_json::from_slice::<TombstoneRecord>(&bytes))
        .transpose()?;
    if tombstone.is_some_and(|record| record.kind == expected_kind) {
        return Ok(());
    }
    let expected_retired_kind = match expected_kind {
        TombstoneKind::Branch => RetiredCatalogKind::Branch,
        TombstoneKind::Checkpoint => RetiredCatalogKind::Checkpoint,
    };
    if db
        .get(retired_catalog_id_key(id))
        .await?
        .map(|bytes| serde_json::from_slice::<RetiredCatalogId>(&bytes))
        .transpose()?
        .is_some_and(|record| record.id == id && record.kind == expected_retired_kind)
    {
        Ok(())
    } else {
        Err(CatalogError::NotFound(id.to_string()))
    }
}

fn ensure_initial_revision(revision: u64) -> Result<(), CatalogError> {
    if revision != 1 {
        return Err(CatalogError::Invalid(
            "new catalog records must start at revision one".to_string(),
        ));
    }
    Ok(())
}

fn ensure_expected_revision(expected: u64, actual: u64) -> Result<(), CatalogError> {
    if expected != actual {
        return Err(CatalogError::RevisionConflict { expected, actual });
    }
    Ok(())
}

#[cfg(test)]
fn validate_revision_change(expected: u64, actual: u64, next: u64) -> Result<(), CatalogError> {
    ensure_expected_revision(expected, actual)?;
    let required = actual
        .checked_add(1)
        .ok_or_else(|| CatalogError::Corrupt("record revision overflow".to_string()))?;
    if next != required {
        return Err(CatalogError::Invalid(format!(
            "replacement revision must be {required}, found {next}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        BranchState, DurableRoot, GcInventoryStats, GcQuarantineShard, GcRevalidationRecord,
        GcRevalidationStats, RootCaptureLifecycle, RootCaptureLifecycleError, SlateDbRootStore,
        catalog_timestamp, lease::LEASE_CLOCK_SKEW,
    };
    use chrono::Utc;
    use slatedb::object_store::memory::InMemory;

    fn branch(name: &str) -> BranchRecord {
        let now = catalog_timestamp(Utc::now());
        BranchRecord {
            id: Uuid::new_v4(),
            revision: 1,
            name: name.to_string(),
            state: BranchState::Ready,
            root: Some(DurableRoot {
                identity: format!("root/{name}"),
                manifest_id: format!("manifest/{name}"),
            }),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn checkpoint(id: Uuid, branch_id: Uuid, name: &str) -> CheckpointRecord {
        let now = catalog_timestamp(Utc::now());
        CheckpointRecord {
            id,
            revision: 1,
            branch_id,
            name: name.to_string(),
            root: DurableRoot {
                identity: format!("checkpoint-root/{name}"),
                manifest_id: format!("checkpoint-manifest/{name}"),
            },
            created_at: now,
            updated_at: now,
        }
    }

    fn branch_lease(branch: &BranchRecord, issued_at: chrono::DateTime<Utc>) -> LeaseRecord {
        LeaseRecord {
            id: Uuid::new_v4(),
            revision: 1,
            subject_kind: LeaseSubjectKind::Branch,
            subject_id: branch.id,
            root: branch.root.clone().unwrap(),
            access_mode: LeaseAccessMode::Read,
            token_hash: "a".repeat(64),
            issued_at,
            updated_at: issued_at,
            expires_at: issued_at + chrono::Duration::seconds(10),
        }
    }

    fn private_epoch(branch_id: Uuid, epoch: u64, now: DateTime<Utc>) -> PrivateEpochRecord {
        PrivateEpochRecord {
            epoch,
            revision: 1,
            pool_id: Uuid::new_v4(),
            reservation_id: Uuid::new_v4(),
            branch_id,
            database_identity: format!("branches/{branch_id}"),
            state: PrivateEpochState::Open,
            created_at: now,
            updated_at: now,
            sealed_at: None,
            exposed_at: None,
        }
    }

    async fn delete_branch(catalog: &SlateDbCatalog, branch: &BranchRecord) {
        let now = catalog_timestamp(Utc::now());
        let operation = BranchDeleteOperation {
            id: Uuid::new_v4(),
            revision: 1,
            branch_id: branch.id,
            branch_name: branch.name.clone(),
            expected_branch_revision: branch.revision,
            root: branch.root.clone().unwrap(),
            parent_id: branch.parent_id,
            origin_checkpoint_id: branch.origin_checkpoint_id,
            phase: BranchDeletePhase::Draining,
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::StartBranchDelete {
                operation: operation.clone(),
            })
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::FinalizeBranchDelete {
                operation_id: operation.id,
                expected_revision: operation.revision,
                deleted_at: now,
            })
            .await
            .unwrap();
    }

    fn reserved_create(
        source: &CheckpointRecord,
        name: &str,
    ) -> (BranchRecord, BranchCreateOperation) {
        let now = catalog_timestamp(Utc::now());
        let destination_id = Uuid::new_v4();
        let branch = BranchRecord {
            id: destination_id,
            revision: 1,
            name: name.to_string(),
            state: BranchState::Creating,
            root: None,
            parent_id: Some(source.branch_id),
            origin_checkpoint_id: Some(source.id),
            created_at: now,
            updated_at: now,
        };
        let operation = BranchCreateOperation {
            id: Uuid::new_v4(),
            revision: 1,
            destination_id,
            destination_name: name.to_string(),
            source_checkpoint_id: source.id,
            source_root: source.root.clone(),
            parent_id: Some(source.branch_id),
            phase: BranchCreatePhase::Reserved,
            destination_root: None,
            created_at: now,
            updated_at: now,
        };
        (branch, operation)
    }

    async fn v17_capacity_fixture(
        path: &str,
        branch_count: usize,
        checkpoint_count: usize,
        lease_count: usize,
    ) -> SlateDbCatalog {
        assert!(branch_count > 0);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from(path), store).await.unwrap();
        let now = catalog_timestamp(Utc::now());
        let mut owner = branch("migration-capacity-owner");
        owner.id = Uuid::from_u128(1);
        let mut batch = WriteBatch::new();
        put_json(
            &mut batch,
            Bytes::from_static(STATE_KEY),
            &CatalogState {
                schema_version: SERVER_CATALOG_SCHEMA_VERSION,
                generation: 1,
            },
        )
        .unwrap();
        put_json(&mut batch, branch_key(owner.id), &owner).unwrap();
        batch.put(branch_name_key(&owner.name), owner.id.to_string());
        for sequence in 1..branch_count {
            let mut record = branch(&format!("migration-branch-{sequence:04}"));
            record.id = Uuid::from_u128(1 + sequence as u128);
            put_json(&mut batch, branch_key(record.id), &record).unwrap();
            batch.put(branch_name_key(&record.name), record.id.to_string());
        }
        for sequence in 0..checkpoint_count {
            let record = checkpoint(
                Uuid::from_u128(100_000 + sequence as u128),
                owner.id,
                &format!("migration-checkpoint-{sequence:03}"),
            );
            put_json(&mut batch, checkpoint_key(record.id), &record).unwrap();
            batch.put(
                checkpoint_name_key(owner.id, &record.name),
                record.id.to_string(),
            );
        }
        for sequence in 0..lease_count {
            let mut record = branch_lease(&owner, now);
            record.id = Uuid::from_u128(200_000 + sequence as u128);
            put_json(&mut batch, lease_key(record.id), &record).unwrap();
        }
        catalog
            .db
            .write_with_options(batch, &durable_write_options())
            .await
            .unwrap();
        catalog
    }

    #[tokio::test]
    async fn records_are_independent_and_deletion_is_atomic_with_tombstone() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("catalog"), store)
            .await
            .unwrap();
        let record = branch("main");
        catalog
            .apply(CatalogMutation::CreateBranch(record.clone()))
            .await
            .unwrap();
        assert_eq!(
            catalog.branch_by_name("main").await.unwrap(),
            Some(record.clone())
        );

        delete_branch(&catalog, &record).await;
        let snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(snapshot.generation, 3);
        assert!(snapshot.branches.is_empty());
        assert_eq!(snapshot.tombstones.get(&record.id).unwrap().name, "main");
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn branch_create_operation_is_exactly_idempotent_and_publishes_atomically() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("branch-create-lifecycle"), store)
            .await
            .unwrap();
        let parent = branch("parent");
        catalog
            .apply(CatalogMutation::CreateBranch(parent.clone()))
            .await
            .unwrap();
        let source = checkpoint(Uuid::new_v4(), parent.id, "source");
        catalog
            .apply(CatalogMutation::CreateCheckpoint(source.clone()))
            .await
            .unwrap();
        let (creating, operation) = reserved_create(&source, "child");
        let reservation = CatalogMutation::ReserveBranchCreate {
            branch: creating.clone(),
            operation: Box::new(operation.clone()),
        };
        assert_eq!(catalog.apply(reservation.clone()).await.unwrap(), 3);
        assert_eq!(catalog.apply(reservation).await.unwrap(), 3);
        let reserved = catalog
            .branch_create_operation(operation.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reserved.gc_roots(), vec![&source.root]);
        assert!(matches!(
            catalog
                .apply(CatalogMutation::DeleteCheckpoint {
                    id: source.id,
                    expected_revision: source.revision,
                    name: source.name.clone(),
                    deleted_at: catalog_timestamp(Utc::now()),
                })
                .await,
            Err(CatalogError::OperationConflict(_))
        ));

        let mut conflicting = operation.clone();
        conflicting.destination_name = "different".to_string();
        assert!(matches!(
            catalog
                .apply(CatalogMutation::ReserveBranchCreate {
                    branch: BranchRecord {
                        name: "different".to_string(),
                        ..creating.clone()
                    },
                    operation: Box::new(conflicting),
                })
                .await,
            Err(CatalogError::OperationConflict(_))
        ));

        let destination_root = DurableRoot {
            identity: "branches/child".to_string(),
            manifest_id: "checkpoint@7".to_string(),
        };
        let root_created_at = catalog_timestamp(Utc::now());
        let record_root = CatalogMutation::RecordBranchCreateRoot {
            operation_id: operation.id,
            expected_revision: 1,
            destination_root: destination_root.clone(),
            updated_at: root_created_at,
        };
        assert_eq!(catalog.apply(record_root.clone()).await.unwrap(), 4);
        assert_eq!(catalog.apply(record_root).await.unwrap(), 4);
        assert_eq!(
            catalog
                .branch_create_operation(operation.id)
                .await
                .unwrap()
                .unwrap()
                .gc_roots()
                .len(),
            1
        );
        assert_eq!(
            catalog
                .apply(CatalogMutation::DeleteCheckpoint {
                    id: source.id,
                    expected_revision: source.revision,
                    name: source.name.clone(),
                    deleted_at: catalog_timestamp(Utc::now()),
                })
                .await
                .unwrap(),
            5
        );
        assert!(matches!(
            catalog
                .apply(CatalogMutation::RecordBranchCreateRoot {
                    operation_id: operation.id,
                    expected_revision: 1,
                    destination_root: DurableRoot {
                        identity: "branches/other".to_string(),
                        manifest_id: "checkpoint@8".to_string(),
                    },
                    updated_at: root_created_at,
                })
                .await,
            Err(CatalogError::OperationConflict(_))
        ));

        let published_at = catalog_timestamp(Utc::now());
        let publish = CatalogMutation::PublishBranchCreate {
            operation_id: operation.id,
            expected_revision: 2,
            updated_at: published_at,
        };
        assert_eq!(catalog.apply(publish.clone()).await.unwrap(), 6);
        assert_eq!(catalog.apply(publish).await.unwrap(), 6);
        let snapshot = catalog.snapshot().await.unwrap();
        let ready = &snapshot.branches[&creating.id];
        assert_eq!(ready.state, BranchState::Ready);
        assert_eq!(ready.root.as_ref(), Some(&destination_root));
        let completed = &snapshot.branch_create_operations[&operation.id];
        assert_eq!(completed.phase, BranchCreatePhase::Published);
        assert_eq!(completed.revision, 3);
        assert_eq!(completed.destination_root.as_ref(), Some(&destination_root));
        assert!(completed.gc_roots().is_empty());

        assert_eq!(
            catalog
                .apply(CatalogMutation::PublishBranchCreate {
                    operation_id: operation.id,
                    expected_revision: 2,
                    updated_at: published_at,
                })
                .await
                .unwrap(),
            6
        );
        catalog.snapshot().await.unwrap();
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn production_branch_and_lineage_capacity_is_enforced_before_publication() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("branch-capacity"), store)
            .await
            .unwrap();
        let mut parent = branch("lineage-000");
        catalog
            .apply(CatalogMutation::CreateBranch(parent.clone()))
            .await
            .unwrap();
        let root = parent.clone();
        for depth in 1..=MAX_BRANCH_LINEAGE_DEPTH {
            let mut child = branch(&format!("lineage-{depth:03}"));
            child.parent_id = Some(parent.id);
            catalog
                .apply(CatalogMutation::CreateBranch(child.clone()))
                .await
                .unwrap();
            parent = child;
        }
        let mut too_deep = branch("lineage-too-deep");
        too_deep.parent_id = Some(parent.id);
        assert!(matches!(
            catalog
                .apply(CatalogMutation::CreateBranch(too_deep.clone()))
                .await,
            Err(CatalogError::Capacity {
                resource: "branch lineage depth",
                limit: MAX_BRANCH_LINEAGE_DEPTH,
            })
        ));
        assert!(catalog.branch(too_deep.id).await.unwrap().is_none());

        let deep_source = checkpoint(Uuid::new_v4(), parent.id, "deep-source");
        catalog
            .apply(CatalogMutation::CreateCheckpoint(deep_source.clone()))
            .await
            .unwrap();
        delete_branch(&catalog, &parent).await;
        let cleanup = catalog
            .cleanup_tombstones(TombstoneCleanupPolicy {
                retain_after: catalog_timestamp(Utc::now()) + chrono::Duration::seconds(1),
                scan_limit: 1,
                compact_limit: 1,
            })
            .await
            .unwrap();
        assert_eq!(cleanup.compacted, 1);
        let (after_compaction, operation) =
            reserved_create(&deep_source, "lineage-after-compaction");
        assert!(matches!(
            catalog
                .apply(CatalogMutation::ReserveBranchCreate {
                    branch: after_compaction,
                    operation: Box::new(operation),
                })
                .await,
            Err(CatalogError::Capacity {
                resource: "branch lineage depth",
                limit: MAX_BRANCH_LINEAGE_DEPTH,
            })
        ));

        let source = checkpoint(Uuid::new_v4(), root.id, "capacity-source");
        catalog
            .apply(CatalogMutation::CreateCheckpoint(source.clone()))
            .await
            .unwrap();
        let existing = MAX_BRANCH_LINEAGE_DEPTH;
        let mut batch = WriteBatch::new();
        for sequence in existing..(MAX_LIVE_BRANCHES - 1) {
            batch.put(
                branch_key(Uuid::from_u128(1_000_000 + sequence as u128)),
                Bytes::from_static(b"capacity-placeholder"),
            );
        }
        catalog
            .db
            .write_with_options(batch, &durable_write_options())
            .await
            .unwrap();
        catalog
            .ensure_live_branch_capacity_unlocked()
            .await
            .unwrap();
        let boundary = branch("branch-at-capacity");
        catalog
            .apply(CatalogMutation::CreateBranch(boundary))
            .await
            .unwrap();
        assert!(matches!(
            catalog.ensure_live_branch_capacity_unlocked().await,
            Err(CatalogError::Capacity {
                resource: "live branch",
                limit: MAX_LIVE_BRANCHES,
            })
        ));
        let generation = catalog.state_unlocked().await.unwrap().generation;
        let (creating, operation) = reserved_create(&source, "over-branch-capacity");
        assert!(matches!(
            catalog
                .apply(CatalogMutation::ReserveBranchCreate {
                    branch: creating.clone(),
                    operation: Box::new(operation),
                })
                .await,
            Err(CatalogError::Capacity {
                resource: "live branch",
                limit: MAX_LIVE_BRANCHES,
            })
        ));
        assert_eq!(
            catalog.state_unlocked().await.unwrap().generation,
            generation
        );
        assert!(catalog.branch(creating.id).await.unwrap().is_none());
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_and_lease_capacity_reuse_atomic_branch_counters() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("root-capacity"), store)
            .await
            .unwrap();
        let owner = branch("capacity-owner");
        catalog
            .apply(CatalogMutation::CreateBranch(owner.clone()))
            .await
            .unwrap();

        let mut blockers = PrivateGcBranchBlockers::empty(owner.id);
        blockers.checkpoints = crate::catalog::MAX_CHECKPOINTS_PER_BRANCH as u64;
        catalog
            .db
            .put_with_options(
                private_gc_branch_blocker_key(owner.id),
                serde_json::to_vec(&blockers).unwrap(),
                &slatedb::config::PutOptions::default(),
                &durable_write_options(),
            )
            .await
            .unwrap();
        let checkpoint = checkpoint(Uuid::new_v4(), owner.id, "over-capacity");
        assert!(matches!(
            catalog
                .apply(CatalogMutation::CreateCheckpoint(checkpoint.clone()))
                .await,
            Err(CatalogError::Capacity {
                resource: "checkpoint per branch",
                limit: crate::catalog::MAX_CHECKPOINTS_PER_BRANCH,
            })
        ));
        assert!(catalog.checkpoint(checkpoint.id).await.unwrap().is_none());

        blockers.checkpoints = 0;
        blockers.leases = MAX_ACTIVE_LEASES_PER_BRANCH as u64;
        catalog
            .db
            .put_with_options(
                private_gc_branch_blocker_key(owner.id),
                serde_json::to_vec(&blockers).unwrap(),
                &slatedb::config::PutOptions::default(),
                &durable_write_options(),
            )
            .await
            .unwrap();
        let lease = branch_lease(&owner, catalog_timestamp(Utc::now()));
        assert!(matches!(
            catalog
                .apply(CatalogMutation::AcquireLease {
                    expected_subject_revision: owner.revision,
                    lease: lease.clone(),
                })
                .await,
            Err(CatalogError::Capacity {
                resource: "active lease per branch",
                limit: MAX_ACTIVE_LEASES_PER_BRANCH,
            })
        ));
        assert!(catalog.lease(lease.id).await.unwrap().is_none());
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn leases_survive_subject_deletion_expire_once_and_cannot_resurrect() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("lease-lifecycle"), store)
            .await
            .unwrap();
        let branch = branch("leased");
        catalog
            .apply(CatalogMutation::CreateBranch(branch.clone()))
            .await
            .unwrap();
        let issued_at = catalog_timestamp(Utc::now());
        let lease = branch_lease(&branch, issued_at);
        assert_eq!(
            catalog
                .apply(CatalogMutation::AcquireLease {
                    expected_subject_revision: branch.revision,
                    lease: lease.clone(),
                })
                .await
                .unwrap(),
            2
        );
        for (renewed_at, expires_at) in [
            (
                issued_at - chrono::Duration::microseconds(1),
                lease.expires_at + chrono::Duration::seconds(1),
            ),
            (issued_at, lease.expires_at - chrono::Duration::seconds(1)),
        ] {
            assert!(matches!(
                catalog
                    .apply(CatalogMutation::RenewLease {
                        id: lease.id,
                        expected_revision: lease.revision,
                        token_hash: lease.token_hash.clone(),
                        renewed_at,
                        expires_at,
                    })
                    .await,
                Err(CatalogError::OperationConflict(_))
            ));
        }
        delete_branch(&catalog, &branch).await;
        let snapshot = catalog.snapshot().await.unwrap();
        assert!(!snapshot.branches.contains_key(&branch.id));
        assert!(snapshot.gc_roots().contains(&&lease.root));
        assert!(matches!(
            catalog
                .apply(CatalogMutation::RenewLease {
                    id: lease.id,
                    expected_revision: lease.revision,
                    token_hash: lease.token_hash.clone(),
                    renewed_at: issued_at,
                    expires_at: lease.expires_at + chrono::Duration::seconds(10),
                })
                .await,
            Err(CatalogError::OperationConflict(_))
        ));
        assert!(matches!(
            catalog
                .apply(CatalogMutation::ExpireLease {
                    id: lease.id,
                    expected_revision: lease.revision,
                    observed_at: lease.expires_at + LEASE_CLOCK_SKEW
                        - chrono::Duration::microseconds(1),
                })
                .await,
            Err(CatalogError::OperationConflict(_))
        ));
        let expire = CatalogMutation::ExpireLease {
            id: lease.id,
            expected_revision: lease.revision,
            observed_at: lease.expires_at + LEASE_CLOCK_SKEW,
        };
        assert_eq!(catalog.apply(expire.clone()).await.unwrap(), 5);
        assert_eq!(catalog.apply(expire).await.unwrap(), 5);
        let snapshot = catalog.snapshot().await.unwrap();
        assert!(!snapshot.leases.contains_key(&lease.id));
        assert!(snapshot.lease_tombstones.contains_key(&lease.id));
        assert!(!snapshot.gc_roots().contains(&&lease.root));
        assert!(matches!(
            catalog
                .apply(CatalogMutation::AcquireLease {
                    expected_subject_revision: branch.revision,
                    lease,
                })
                .await,
            Err(CatalogError::AlreadyExists(_) | CatalogError::NotFound(_))
        ));
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn corrupt_durable_lease_blocks_snapshot_and_gc_admission() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("corrupt-lease"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let branch = branch("corrupt-lease-owner");
        catalog
            .apply(CatalogMutation::CreateBranch(branch.clone()))
            .await
            .unwrap();
        let issued_at = catalog_timestamp(Utc::now());
        let lease = branch_lease(&branch, issued_at);
        catalog
            .apply(CatalogMutation::AcquireLease {
                expected_subject_revision: branch.revision,
                lease: lease.clone(),
            })
            .await
            .unwrap();
        assert!(
            catalog
                .snapshot()
                .await
                .unwrap()
                .gc_roots()
                .contains(&&lease.root)
        );

        let mut corrupt = lease.clone();
        corrupt.token_hash = "not-a-sha256".to_string();
        let mut batch = WriteBatch::new();
        put_json(&mut batch, lease_key(lease.id), &corrupt).unwrap();
        catalog
            .db
            .write_with_options(batch, &durable_write_options())
            .await
            .unwrap();

        assert!(matches!(
            catalog.snapshot().await,
            Err(CatalogError::Invalid(_))
        ));
        let run_id = Uuid::new_v4();
        let lifecycle = RootCaptureLifecycle::new(
            catalog.clone(),
            SlateDbRootStore::new(store, Path::from("corrupt-lease-branches")),
        );
        assert!(matches!(
            lifecycle.begin(run_id).await,
            Err(RootCaptureLifecycleError::Catalog(CatalogError::Invalid(_)))
        ));
        assert!(catalog.gc_run(run_id).await.unwrap().is_none());
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_lease_acquisition_serializes_with_logical_deletion() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("checkpoint-lease-delete-race"), store)
                .await
                .unwrap(),
        );
        let owner = branch("owner");
        catalog
            .apply(CatalogMutation::CreateBranch(owner.clone()))
            .await
            .unwrap();
        let checkpoint = checkpoint(Uuid::new_v4(), owner.id, "point");
        catalog
            .apply(CatalogMutation::CreateCheckpoint(checkpoint.clone()))
            .await
            .unwrap();
        let now = catalog_timestamp(Utc::now());
        let lease = LeaseRecord {
            id: Uuid::new_v4(),
            revision: 1,
            subject_kind: LeaseSubjectKind::Checkpoint,
            subject_id: checkpoint.id,
            root: checkpoint.root.clone(),
            access_mode: LeaseAccessMode::Read,
            token_hash: "b".repeat(64),
            issued_at: now,
            updated_at: now,
            expires_at: now + chrono::Duration::minutes(1),
        };
        let acquire_catalog = Arc::clone(&catalog);
        let delete_catalog = Arc::clone(&catalog);
        let acquire_lease = lease.clone();
        let delete_checkpoint = checkpoint.clone();
        let (acquired, deleted) = tokio::join!(
            async move {
                acquire_catalog
                    .apply(CatalogMutation::AcquireLease {
                        expected_subject_revision: delete_checkpoint.revision,
                        lease: acquire_lease,
                    })
                    .await
            },
            async move {
                delete_catalog
                    .apply(CatalogMutation::DeleteCheckpoint {
                        id: checkpoint.id,
                        expected_revision: checkpoint.revision,
                        name: checkpoint.name,
                        deleted_at: now,
                    })
                    .await
            }
        );
        deleted.unwrap();
        let acquired_ok = acquired.is_ok();
        assert!(acquired_ok || matches!(&acquired, Err(CatalogError::NotFound(_))));
        let snapshot = catalog.snapshot().await.unwrap();
        if acquired_ok {
            assert!(snapshot.leases.contains_key(&lease.id));
            assert!(snapshot.gc_roots().contains(&&lease.root));
        } else {
            assert!(!snapshot.leases.contains_key(&lease.id));
        }
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_fails_closed_when_source_hold_index_is_missing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("source-hold-audit"), store)
            .await
            .unwrap();
        let parent = branch("parent");
        catalog
            .apply(CatalogMutation::CreateBranch(parent.clone()))
            .await
            .unwrap();
        let source = checkpoint(Uuid::new_v4(), parent.id, "source");
        catalog
            .apply(CatalogMutation::CreateCheckpoint(source.clone()))
            .await
            .unwrap();
        let (creating, operation) = reserved_create(&source, "child");
        catalog
            .apply(CatalogMutation::ReserveBranchCreate {
                branch: creating,
                operation: Box::new(operation.clone()),
            })
            .await
            .unwrap();
        catalog.snapshot().await.unwrap();

        let mut corrupt = WriteBatch::new();
        corrupt.delete(branch_create_source_key(source.id, operation.id));
        catalog
            .db
            .write_with_options(corrupt, &durable_write_options())
            .await
            .unwrap();
        assert!(matches!(
            catalog.snapshot().await,
            Err(CatalogError::Corrupt(message)) if message.contains("source-hold index")
        ));
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_fails_closed_when_private_gc_blocker_indexes_disagree() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("private-gc-blocker-audit"), store)
            .await
            .unwrap();
        let owner = branch("blocker-owner");
        catalog
            .apply(CatalogMutation::CreateBranch(owner.clone()))
            .await
            .unwrap();
        catalog.snapshot().await.unwrap();

        let mut missing_branch = WriteBatch::new();
        missing_branch.delete(private_gc_branch_blocker_key(owner.id));
        catalog
            .db
            .write_with_options(missing_branch, &durable_write_options())
            .await
            .unwrap();
        assert!(matches!(
            catalog.snapshot().await,
            Err(CatalogError::Corrupt(message)) if message.contains("branch blocker index")
        ));

        let mut wrong_global = WriteBatch::new();
        put_json(
            &mut wrong_global,
            private_gc_branch_blocker_key(owner.id),
            &PrivateGcBranchBlockers::empty(owner.id),
        )
        .unwrap();
        put_json(
            &mut wrong_global,
            Bytes::from_static(PRIVATE_GC_GLOBAL_BLOCKER_KEY),
            &PrivateGcGlobalBlockers {
                root_retaining_gc_runs: 1,
            },
        )
        .unwrap();
        catalog
            .db
            .write_with_options(wrong_global, &durable_write_options())
            .await
            .unwrap();
        assert!(matches!(
            catalog.snapshot().await,
            Err(CatalogError::Corrupt(message)) if message.contains("global blocker index")
        ));
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn tombstone_cleanup_is_bounded_dependency_safe_and_preserves_uuid_reservations() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("tombstone-cleanup"), store)
            .await
            .unwrap();
        let owner = branch("cleanup-owner");
        catalog
            .apply(CatalogMutation::CreateBranch(owner.clone()))
            .await
            .unwrap();
        let leased_checkpoint = checkpoint(Uuid::new_v4(), owner.id, "leased-deleted");
        catalog
            .apply(CatalogMutation::CreateCheckpoint(leased_checkpoint.clone()))
            .await
            .unwrap();
        let now = catalog_timestamp(Utc::now());
        let lease = LeaseRecord {
            id: Uuid::new_v4(),
            revision: 1,
            subject_kind: LeaseSubjectKind::Checkpoint,
            subject_id: leased_checkpoint.id,
            root: leased_checkpoint.root.clone(),
            access_mode: LeaseAccessMode::Read,
            token_hash: "b".repeat(64),
            issued_at: now,
            updated_at: now,
            expires_at: now + chrono::Duration::seconds(10),
        };
        catalog
            .apply(CatalogMutation::AcquireLease {
                expected_subject_revision: leased_checkpoint.revision,
                lease: lease.clone(),
            })
            .await
            .unwrap();
        let deleted_at = now + chrono::Duration::microseconds(1);
        catalog
            .apply(CatalogMutation::DeleteCheckpoint {
                id: leased_checkpoint.id,
                expected_revision: leased_checkpoint.revision,
                name: leased_checkpoint.name.clone(),
                deleted_at,
            })
            .await
            .unwrap();
        let policy = |retain_after| TombstoneCleanupPolicy {
            retain_after,
            scan_limit: 2,
            compact_limit: 1,
        };
        let young = catalog
            .cleanup_tombstones(policy(deleted_at - chrono::Duration::microseconds(1)))
            .await
            .unwrap();
        assert_eq!(young.retained_by_age, 1);
        let leased = catalog
            .cleanup_tombstones(policy(deleted_at))
            .await
            .unwrap();
        assert_eq!(leased.retained_by_roots, 1);
        catalog
            .apply(CatalogMutation::EndLease {
                id: lease.id,
                expected_revision: lease.revision,
                token_hash: lease.token_hash,
                ended_at: deleted_at + chrono::Duration::microseconds(1),
            })
            .await
            .unwrap();
        let generation_before = catalog.snapshot().await.unwrap().generation;
        let compacted = catalog
            .cleanup_tombstones(policy(deleted_at + chrono::Duration::microseconds(1)))
            .await
            .unwrap();
        assert_eq!(compacted.compacted, 1);
        let snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(snapshot.generation, generation_before + 1);
        assert!(!snapshot.tombstones.contains_key(&leased_checkpoint.id));
        assert_eq!(
            snapshot.retired_catalog_ids[&leased_checkpoint.id].kind,
            RetiredCatalogKind::Checkpoint
        );
        assert!(matches!(
            ensure_resource_id_available(catalog.db.as_ref(), leased_checkpoint.id).await,
            Err(CatalogError::AlreadyExists(_))
        ));

        let mut deleted_ids = Vec::new();
        for index in 0..3 {
            let record = checkpoint(Uuid::new_v4(), owner.id, &format!("bounded-{index}"));
            catalog
                .apply(CatalogMutation::CreateCheckpoint(record.clone()))
                .await
                .unwrap();
            catalog
                .apply(CatalogMutation::DeleteCheckpoint {
                    id: record.id,
                    expected_revision: record.revision,
                    name: record.name,
                    deleted_at: record.created_at,
                })
                .await
                .unwrap();
            deleted_ids.push(record.id);
        }
        let bounded = TombstoneCleanupPolicy {
            retain_after: catalog_timestamp(Utc::now()),
            scan_limit: 1,
            compact_limit: 1,
        };
        for _ in 0..8 {
            let report = catalog.cleanup_tombstones(bounded).await.unwrap();
            assert!(report.examined <= 1);
            assert!(report.compacted <= 1);
        }
        let snapshot = catalog.snapshot().await.unwrap();
        assert!(
            deleted_ids
                .iter()
                .all(|id| snapshot.retired_catalog_ids.contains_key(id))
        );

        let source = checkpoint(Uuid::new_v4(), owner.id, "incomplete-source-history");
        catalog
            .apply(CatalogMutation::CreateCheckpoint(source.clone()))
            .await
            .unwrap();
        let (destination, operation) = reserved_create(&source, "cleanup-child");
        catalog
            .apply(CatalogMutation::ReserveBranchCreate {
                branch: destination,
                operation: Box::new(operation.clone()),
            })
            .await
            .unwrap();
        let root_created_at = catalog_timestamp(Utc::now()).max(operation.updated_at);
        catalog
            .apply(CatalogMutation::RecordBranchCreateRoot {
                operation_id: operation.id,
                expected_revision: operation.revision,
                destination_root: DurableRoot {
                    identity: "branches/cleanup-child".to_string(),
                    manifest_id: "manifest/cleanup-child".to_string(),
                },
                updated_at: root_created_at,
            })
            .await
            .unwrap();
        let source_deleted_at = root_created_at + chrono::Duration::microseconds(1);
        catalog
            .apply(CatalogMutation::DeleteCheckpoint {
                id: source.id,
                expected_revision: source.revision,
                name: source.name,
                deleted_at: source_deleted_at,
            })
            .await
            .unwrap();
        let broad = TombstoneCleanupPolicy {
            retain_after: source_deleted_at,
            scan_limit: 16,
            compact_limit: 16,
        };
        let retained = catalog.cleanup_tombstones(broad).await.unwrap();
        assert_eq!(retained.retained_by_dependency, 1);
        let published_at = source_deleted_at + chrono::Duration::microseconds(1);
        catalog
            .apply(CatalogMutation::PublishBranchCreate {
                operation_id: operation.id,
                expected_revision: operation.revision + 1,
                updated_at: published_at,
            })
            .await
            .unwrap();
        for _ in 0..3 {
            catalog
                .cleanup_tombstones(TombstoneCleanupPolicy {
                    retain_after: published_at,
                    ..broad
                })
                .await
                .unwrap();
        }
        assert!(
            catalog
                .snapshot()
                .await
                .unwrap()
                .retired_catalog_ids
                .contains_key(&source.id)
        );

        let doomed = branch("cleanup-deleted-branch");
        catalog
            .apply(CatalogMutation::CreateBranch(doomed.clone()))
            .await
            .unwrap();
        delete_branch(&catalog, &doomed).await;
        let delete_operation_id = catalog
            .snapshot()
            .await
            .unwrap()
            .branch_delete_operations
            .values()
            .find(|operation| operation.branch_id == doomed.id)
            .unwrap()
            .id;
        let broad = TombstoneCleanupPolicy {
            retain_after: catalog_timestamp(Utc::now()),
            scan_limit: 16,
            compact_limit: 16,
        };
        for _ in 0..3 {
            catalog.cleanup_tombstones(broad).await.unwrap();
        }
        let snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(
            snapshot.retired_catalog_ids[&doomed.id].kind,
            RetiredCatalogKind::Branch
        );
        assert_eq!(
            snapshot.retired_catalog_ids[&delete_operation_id].kind,
            RetiredCatalogKind::BranchDeleteOperation
        );
        assert!(
            !snapshot
                .branch_delete_operations
                .contains_key(&delete_operation_id)
        );
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn tombstone_cleanup_waits_for_root_retaining_global_gc_runs() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("tombstone-cleanup-gc-fence"), store)
            .await
            .unwrap();
        let owner = branch("cleanup-gc-owner");
        catalog
            .apply(CatalogMutation::CreateBranch(owner.clone()))
            .await
            .unwrap();
        let deleted = checkpoint(Uuid::new_v4(), owner.id, "cleanup-gc-deleted");
        catalog
            .apply(CatalogMutation::CreateCheckpoint(deleted.clone()))
            .await
            .unwrap();
        let deleted_at = catalog_timestamp(Utc::now()).max(deleted.created_at);
        catalog
            .apply(CatalogMutation::DeleteCheckpoint {
                id: deleted.id,
                expected_revision: deleted.revision,
                name: deleted.name,
                deleted_at,
            })
            .await
            .unwrap();
        let snapshot = catalog.snapshot().await.unwrap();
        let roots = snapshot.gc_root_pins();
        let run = GcRunRecord {
            id: Uuid::new_v4(),
            revision: 1,
            catalog_generation: snapshot.generation,
            inventory_cutoff: deleted_at,
            root_digest: crate::catalog::gc_root_digest(&roots).unwrap(),
            roots,
            segment_pool: ".zerofs/segment-pool".to_string(),
            mark_shards: Vec::new(),
            mark_stats: None,
            quarantine_shards: Vec::new(),
            inventory_stats: None,
            phase: GcRunPhase::Captured,
            quarantine_at: None,
            revalidation: None,
            deletion: None,
            created_at: deleted_at,
            updated_at: deleted_at,
        };
        catalog
            .begin_gc_run(snapshot.generation, run)
            .await
            .unwrap();
        let report = catalog
            .cleanup_tombstones(TombstoneCleanupPolicy {
                retain_after: deleted_at,
                scan_limit: 1,
                compact_limit: 1,
            })
            .await
            .unwrap();
        assert_eq!(report.retained_by_roots, 1);
        assert!(catalog.tombstone(deleted.id).await.unwrap().is_some());
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn source_checkpoint_deletion_and_create_reservation_serialize_exactly() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("source-delete-race"), store)
                .await
                .unwrap(),
        );
        let parent = branch("parent");
        catalog
            .apply(CatalogMutation::CreateBranch(parent.clone()))
            .await
            .unwrap();
        let source = checkpoint(Uuid::new_v4(), parent.id, "source");
        catalog
            .apply(CatalogMutation::CreateCheckpoint(source.clone()))
            .await
            .unwrap();
        let (creating, operation) = reserved_create(&source, "child");

        let (reserve, delete) = tokio::join!(
            catalog.apply(CatalogMutation::ReserveBranchCreate {
                branch: creating.clone(),
                operation: Box::new(operation.clone()),
            }),
            catalog.apply(CatalogMutation::DeleteCheckpoint {
                id: source.id,
                expected_revision: source.revision,
                name: source.name,
                deleted_at: catalog_timestamp(Utc::now()),
            })
        );
        let snapshot = catalog.snapshot().await.unwrap();
        match (reserve, delete) {
            (Ok(_), Err(CatalogError::OperationConflict(_))) => {
                assert!(snapshot.checkpoints.contains_key(&source.id));
                assert!(snapshot.branches.contains_key(&creating.id));
                assert!(
                    snapshot
                        .branch_create_operations
                        .contains_key(&operation.id)
                );
            }
            (Err(CatalogError::NotFound(_)), Ok(_)) => {
                assert!(snapshot.tombstones.contains_key(&source.id));
                assert!(!snapshot.branches.contains_key(&creating.id));
                assert!(
                    !snapshot
                        .branch_create_operations
                        .contains_key(&operation.id)
                );
            }
            outcomes => panic!("unexpected reservation/deletion outcomes: {outcomes:?}"),
        }
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn resource_ids_are_global_and_tombstones_prevent_reuse() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("global-ids"), store)
            .await
            .unwrap();
        let first = branch("first");
        catalog
            .apply(CatalogMutation::CreateBranch(first.clone()))
            .await
            .unwrap();

        let collision = catalog
            .apply(CatalogMutation::CreateCheckpoint(checkpoint(
                first.id,
                first.id,
                "collision",
            )))
            .await
            .unwrap_err();
        assert!(matches!(collision, CatalogError::AlreadyExists(_)));

        delete_branch(&catalog, &first).await;
        let reused = catalog
            .apply(CatalogMutation::CreateBranch(BranchRecord {
                id: first.id,
                ..branch("replacement")
            }))
            .await
            .unwrap_err();
        assert!(matches!(reused, CatalogError::AlreadyExists(_)));
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn revisions_conflict_per_record_not_global_generation() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("revisions"), store)
            .await
            .unwrap();
        let first = branch("first");
        let second = branch("second");
        assert_eq!(
            catalog
                .apply(CatalogMutation::CreateBranch(first.clone()))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            catalog
                .apply(CatalogMutation::CreateBranch(second))
                .await
                .unwrap(),
            2
        );

        let mut replacement = first.clone();
        replacement.revision = 2;
        replacement.updated_at = catalog_timestamp(Utc::now());
        let stale = catalog
            .apply(CatalogMutation::ReplaceBranch {
                expected_revision: 9,
                record: replacement,
            })
            .await
            .unwrap_err();
        assert!(matches!(
            stale,
            CatalogError::RevisionConflict {
                expected: 9,
                actual: 1
            }
        ));
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn replacements_preserve_referential_integrity() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("replacement-references"), store)
            .await
            .unwrap();
        let owner = branch("owner");
        catalog
            .apply(CatalogMutation::CreateBranch(owner.clone()))
            .await
            .unwrap();

        let mut bad_branch = owner.clone();
        bad_branch.revision = 2;
        bad_branch.parent_id = Some(Uuid::new_v4());
        assert!(matches!(
            catalog
                .apply(CatalogMutation::ReplaceBranch {
                    expected_revision: 1,
                    record: bad_branch,
                })
                .await,
            Err(CatalogError::Invalid(_))
        ));
        let mut changed_root = owner.clone();
        changed_root.revision = 2;
        changed_root.root = Some(DurableRoot {
            identity: "root/forged-forward-reference".to_string(),
            manifest_id: "manifest/forged-forward-reference".to_string(),
        });
        assert!(matches!(
            catalog
                .apply(CatalogMutation::ReplaceBranch {
                    expected_revision: 1,
                    record: changed_root,
                })
                .await,
            Err(CatalogError::Invalid(_))
        ));

        let original_checkpoint = checkpoint(Uuid::new_v4(), owner.id, "stable");
        catalog
            .apply(CatalogMutation::CreateCheckpoint(
                original_checkpoint.clone(),
            ))
            .await
            .unwrap();
        let mut bad_checkpoint = original_checkpoint;
        bad_checkpoint.revision = 2;
        bad_checkpoint.branch_id = Uuid::new_v4();
        assert!(matches!(
            catalog
                .apply(CatalogMutation::ReplaceCheckpoint {
                    expected_revision: 1,
                    record: bad_checkpoint,
                })
                .await,
            Err(CatalogError::Invalid(_))
        ));
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn private_epoch_lifecycle_is_monotonic_exact_and_branch_bound() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("private-epoch-lifecycle");
        let catalog = SlateDbCatalog::open(path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        let owner = branch("private-owner");
        catalog
            .apply(CatalogMutation::CreateBranch(owner.clone()))
            .await
            .unwrap();
        let opened_at = owner.updated_at + chrono::Duration::microseconds(1);
        let record = private_epoch(owner.id, 41, opened_at);
        let registered_generation = catalog
            .apply(CatalogMutation::RegisterPrivateEpoch(record.clone()))
            .await
            .unwrap();
        assert_eq!(
            catalog
                .apply(CatalogMutation::RegisterPrivateEpoch(record.clone()))
                .await
                .unwrap(),
            registered_generation,
            "exact registration retry is generation-neutral"
        );
        let mut conflicting = record.clone();
        conflicting.reservation_id = Uuid::new_v4();
        assert!(matches!(
            catalog
                .apply(CatalogMutation::RegisterPrivateEpoch(conflicting))
                .await,
            Err(CatalogError::OperationConflict(_))
        ));

        let sealed_at = opened_at + chrono::Duration::microseconds(1);
        let mut second = private_epoch(owner.id, 42, sealed_at);
        second.pool_id = record.pool_id;
        catalog
            .apply(CatalogMutation::RegisterPrivateEpoch(second.clone()))
            .await
            .unwrap();
        assert!(matches!(
            catalog
                .apply(CatalogMutation::SealPrivateEpoch {
                    epoch: record.epoch,
                    branch_id: owner.id,
                    expected_revision: record.revision,
                    next_epoch: second.epoch,
                    expected_next_revision: second.revision + 1,
                    sealed_at,
                })
                .await,
            Err(CatalogError::RevisionConflict {
                expected: 2,
                actual: 1
            })
        ));
        assert_eq!(
            catalog
                .private_epoch(record.epoch)
                .await
                .unwrap()
                .unwrap()
                .state,
            PrivateEpochState::Open,
            "a stale successor revision must atomically reject the old-epoch seal"
        );
        catalog
            .apply(CatalogMutation::SealPrivateEpoch {
                epoch: record.epoch,
                branch_id: owner.id,
                expected_revision: 1,
                next_epoch: second.epoch,
                expected_next_revision: second.revision,
                sealed_at,
            })
            .await
            .unwrap();
        let sealed = catalog.private_epoch(record.epoch).await.unwrap().unwrap();
        assert_eq!(sealed.state, PrivateEpochState::SealedPrivate);
        assert_eq!(sealed.revision, 2);
        assert!(matches!(
            catalog
                .apply(CatalogMutation::SealPrivateEpoch {
                    epoch: record.epoch,
                    branch_id: Uuid::new_v4(),
                    expected_revision: 2,
                    next_epoch: second.epoch,
                    expected_next_revision: second.revision,
                    sealed_at,
                })
                .await,
            Err(CatalogError::OperationConflict(_))
        ));

        let exposed_at = sealed_at + chrono::Duration::microseconds(1);
        catalog
            .apply(CatalogMutation::ExposePrivateEpoch {
                epoch: record.epoch,
                branch_id: owner.id,
                expected_revision: 2,
                exposed_at,
            })
            .await
            .unwrap();
        let exposed = catalog.private_epoch(record.epoch).await.unwrap().unwrap();
        assert_eq!(exposed.state, PrivateEpochState::Exposed);
        assert_eq!(exposed.revision, 3);
        assert_eq!(exposed.sealed_at, Some(sealed_at));
        assert!(matches!(
            catalog
                .apply(CatalogMutation::SealPrivateEpoch {
                    epoch: record.epoch,
                    branch_id: owner.id,
                    expected_revision: 3,
                    next_epoch: second.epoch,
                    expected_next_revision: second.revision,
                    sealed_at: exposed_at + chrono::Duration::microseconds(1),
                })
                .await,
            Err(CatalogError::OperationConflict(_))
        ));

        let delete_at = catalog_timestamp(Utc::now());
        let operation = BranchDeleteOperation {
            id: Uuid::new_v4(),
            revision: 1,
            branch_id: owner.id,
            branch_name: owner.name.clone(),
            expected_branch_revision: owner.revision,
            root: owner.root.clone().unwrap(),
            parent_id: owner.parent_id,
            origin_checkpoint_id: owner.origin_checkpoint_id,
            phase: BranchDeletePhase::Draining,
            created_at: delete_at,
            updated_at: delete_at,
        };
        catalog
            .apply(CatalogMutation::StartBranchDelete {
                operation: operation.clone(),
            })
            .await
            .unwrap();
        let deleting_snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(
            deleting_snapshot.branches[&owner.id].state,
            BranchState::Deleting
        );
        assert_eq!(
            deleting_snapshot.private_epochs[&second.epoch].state,
            PrivateEpochState::Exposed
        );
        catalog.close().await.unwrap();

        let catalog = SlateDbCatalog::open(path, store).await.unwrap();
        let reopened_snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(
            reopened_snapshot.private_epochs[&second.epoch].state,
            PrivateEpochState::Exposed
        );
        catalog
            .apply(CatalogMutation::FinalizeBranchDelete {
                operation_id: operation.id,
                expected_revision: operation.revision,
                deleted_at: delete_at,
            })
            .await
            .unwrap();
        let deleted_epoch = catalog.private_epoch(second.epoch).await.unwrap().unwrap();
        assert_eq!(deleted_epoch.state, PrivateEpochState::Exposed);
        assert!(deleted_epoch.exposed_at.is_some());
        catalog.snapshot().await.unwrap();
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn targeted_private_gc_views_require_the_exact_valid_ready_owner() {
        for corruption in ["missing", "non-ready", "malformed", "wrong-root"] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let catalog =
                SlateDbCatalog::open(Path::from(format!("private-gc-owner-{corruption}")), store)
                    .await
                    .unwrap();
            let owner = branch(&format!("owner-{corruption}"));
            let database_identity = owner.root.as_ref().unwrap().identity.clone();
            catalog
                .apply(CatalogMutation::CreateBranch(owner.clone()))
                .await
                .unwrap();
            let opened_at = owner.updated_at + chrono::Duration::microseconds(1);
            let mut guarded_epoch = private_epoch(owner.id, 301, opened_at);
            guarded_epoch.database_identity = database_identity.clone();
            let mut writer_epoch = private_epoch(owner.id, 302, opened_at);
            writer_epoch.database_identity = database_identity.clone();
            writer_epoch.pool_id = guarded_epoch.pool_id;
            catalog
                .apply(CatalogMutation::RegisterPrivateEpoch(guarded_epoch.clone()))
                .await
                .unwrap();
            catalog
                .apply(CatalogMutation::RegisterPrivateEpoch(writer_epoch.clone()))
                .await
                .unwrap();
            let sealed_at = opened_at + chrono::Duration::microseconds(1);
            catalog
                .apply(CatalogMutation::SealPrivateEpoch {
                    epoch: guarded_epoch.epoch,
                    branch_id: owner.id,
                    expected_revision: guarded_epoch.revision,
                    next_epoch: writer_epoch.epoch,
                    expected_next_revision: writer_epoch.revision,
                    sealed_at,
                })
                .await
                .unwrap();
            let guard = LocalGcGuardRecord {
                id: Uuid::new_v4(),
                revision: 1,
                branch_id: owner.id,
                epoch: guarded_epoch.epoch,
                epoch_revision: 2,
                candidate_count: 1,
                candidate_digest: "a".repeat(64),
                created_at: sealed_at + chrono::Duration::microseconds(1),
            };
            catalog
                .apply(CatalogMutation::AcquireLocalGcGuard(guard.clone()))
                .await
                .unwrap();
            assert_eq!(
                catalog
                    .private_gc_owner_view(owner.id, &database_identity, 1)
                    .await
                    .unwrap()
                    .active_guard,
                Some(guard.clone())
            );
            assert_eq!(
                catalog
                    .private_gc_guard_view(guard.id, writer_epoch.epoch)
                    .await
                    .unwrap()
                    .guard,
                Some(guard.clone())
            );

            let mut corrupt = WriteBatch::new();
            match corruption {
                "missing" => corrupt.delete(branch_key(owner.id)),
                "non-ready" => {
                    let mut changed = owner.clone();
                    changed.state = BranchState::Deleting;
                    put_json(&mut corrupt, branch_key(owner.id), &changed).unwrap();
                }
                "malformed" => {
                    corrupt.put(branch_key(owner.id), b"{}".as_slice());
                }
                "wrong-root" => {
                    let mut changed = owner.clone();
                    changed.root.as_mut().unwrap().identity = "root/other-database".to_string();
                    put_json(&mut corrupt, branch_key(owner.id), &changed).unwrap();
                }
                _ => unreachable!(),
            }
            catalog
                .db
                .write_with_options(corrupt, &durable_write_options())
                .await
                .unwrap();
            assert!(
                catalog
                    .private_gc_owner_view(owner.id, &database_identity, 1)
                    .await
                    .is_err(),
                "owner view accepted {corruption} owner"
            );
            assert!(
                catalog
                    .private_gc_guard_view(guard.id, writer_epoch.epoch)
                    .await
                    .is_err(),
                "deletion view accepted {corruption} owner"
            );
            catalog.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn local_gc_guard_is_durable_non_expiring_and_fences_root_exposure() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("local-gc-guard");
        let catalog = SlateDbCatalog::open(path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        let owner = branch("guard-owner");
        catalog
            .apply(CatalogMutation::CreateBranch(owner.clone()))
            .await
            .unwrap();
        let opened_at = owner.updated_at + chrono::Duration::microseconds(1);
        let database_identity = owner.root.as_ref().unwrap().identity.clone();
        let mut epoch = private_epoch(owner.id, 91, opened_at);
        epoch.database_identity = database_identity.clone();
        let mut next_epoch = private_epoch(owner.id, 92, opened_at);
        next_epoch.database_identity = database_identity;
        next_epoch.pool_id = epoch.pool_id;
        catalog
            .apply(CatalogMutation::RegisterPrivateEpoch(epoch.clone()))
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::RegisterPrivateEpoch(next_epoch.clone()))
            .await
            .unwrap();
        let sealed_at = opened_at + chrono::Duration::microseconds(1);
        catalog
            .apply(CatalogMutation::SealPrivateEpoch {
                epoch: epoch.epoch,
                branch_id: owner.id,
                expected_revision: epoch.revision,
                next_epoch: next_epoch.epoch,
                expected_next_revision: next_epoch.revision,
                sealed_at,
            })
            .await
            .unwrap();

        let lease = branch_lease(&owner, sealed_at);
        catalog
            .apply(CatalogMutation::AcquireLease {
                expected_subject_revision: owner.revision,
                lease: lease.clone(),
            })
            .await
            .unwrap();
        let guard = LocalGcGuardRecord {
            id: Uuid::new_v4(),
            revision: 1,
            branch_id: owner.id,
            epoch: epoch.epoch,
            epoch_revision: 2,
            candidate_count: 2,
            candidate_digest: "a".repeat(64),
            created_at: sealed_at + chrono::Duration::microseconds(1),
        };
        assert!(matches!(
            catalog
                .apply(CatalogMutation::AcquireLocalGcGuard(guard.clone()))
                .await,
            Err(CatalogError::OperationConflict(_))
        ));
        catalog
            .apply(CatalogMutation::EndLease {
                id: lease.id,
                expected_revision: lease.revision,
                token_hash: lease.token_hash.clone(),
                ended_at: guard.created_at,
            })
            .await
            .unwrap();

        let unrelated = branch("unrelated-guard-blockers");
        catalog
            .apply(CatalogMutation::CreateBranch(unrelated.clone()))
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::CreateCheckpoint(checkpoint(
                Uuid::new_v4(),
                unrelated.id,
                "unrelated-checkpoint",
            )))
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::AcquireLease {
                expected_subject_revision: unrelated.revision,
                lease: branch_lease(&unrelated, guard.created_at),
            })
            .await
            .unwrap();

        let guarded_generation = catalog
            .apply(CatalogMutation::AcquireLocalGcGuard(guard.clone()))
            .await
            .unwrap();
        assert_eq!(
            catalog
                .apply(CatalogMutation::AcquireLocalGcGuard(guard.clone()))
                .await
                .unwrap(),
            guarded_generation,
            "exact guard retry is generation-neutral"
        );
        assert!(matches!(
            catalog
                .apply(CatalogMutation::ExposePrivateEpoch {
                    epoch: epoch.epoch,
                    branch_id: owner.id,
                    expected_revision: 2,
                    exposed_at: guard.created_at + chrono::Duration::microseconds(1),
                })
                .await,
            Err(CatalogError::OperationConflict(_))
        ));
        assert!(matches!(
            catalog
                .apply(CatalogMutation::AcquireLease {
                    expected_subject_revision: owner.revision,
                    lease: branch_lease(
                        &owner,
                        guard.created_at + chrono::Duration::microseconds(1)
                    ),
                })
                .await,
            Err(CatalogError::OperationConflict(_))
        ));
        assert!(matches!(
            catalog
                .apply(CatalogMutation::CreateCheckpoint(checkpoint(
                    Uuid::new_v4(),
                    owner.id,
                    "guarded-checkpoint",
                )))
                .await,
            Err(CatalogError::OperationConflict(_))
        ));
        let delete_at = guard.created_at + chrono::Duration::microseconds(2);
        assert!(matches!(
            catalog
                .apply(CatalogMutation::StartBranchDelete {
                    operation: BranchDeleteOperation {
                        id: Uuid::new_v4(),
                        revision: 1,
                        branch_id: owner.id,
                        branch_name: owner.name.clone(),
                        expected_branch_revision: owner.revision,
                        root: owner.root.clone().unwrap(),
                        parent_id: owner.parent_id,
                        origin_checkpoint_id: owner.origin_checkpoint_id,
                        phase: BranchDeletePhase::Draining,
                        created_at: delete_at,
                        updated_at: delete_at,
                    },
                })
                .await,
            Err(CatalogError::OperationConflict(_))
        ));
        catalog.close().await.unwrap();

        let catalog = SlateDbCatalog::open(path, store).await.unwrap();
        let snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(snapshot.local_gc_guards[&guard.id], guard);
        assert_eq!(
            snapshot.private_epochs[&epoch.epoch].state,
            PrivateEpochState::SealedPrivate
        );
        let started_at = guard.created_at + chrono::Duration::microseconds(1);
        let initial = LocalGcProgressRecord {
            id: guard.id,
            revision: 1,
            branch_id: guard.branch_id,
            epoch: guard.epoch,
            epoch_revision: guard.epoch_revision,
            candidate_count: guard.candidate_count,
            candidate_digest: guard.candidate_digest.clone(),
            next_candidate: 0,
            deleted_objects: 0,
            deleted_bytes: 0,
            already_absent: 0,
            started_at,
            updated_at: started_at,
            completed_at: None,
        };
        let initial_generation = catalog
            .apply(CatalogMutation::PublishLocalGcProgress(initial.clone()))
            .await
            .unwrap();
        assert_eq!(
            catalog
                .apply(CatalogMutation::PublishLocalGcProgress(initial.clone()))
                .await
                .unwrap(),
            initial_generation,
            "exact local progress retry is generation-neutral"
        );
        let mut invalid = initial.clone();
        invalid.revision = 2;
        invalid.next_candidate = 1;
        assert!(matches!(
            catalog
                .apply(CatalogMutation::PublishLocalGcProgress(invalid))
                .await,
            Err(CatalogError::Invalid(_))
        ));
        let mut advanced = initial;
        advanced.revision = 2;
        advanced.next_candidate = 1;
        advanced.deleted_objects = 1;
        advanced.deleted_bytes = 4096;
        advanced.updated_at += chrono::Duration::microseconds(1);
        catalog
            .apply(CatalogMutation::PublishLocalGcProgress(advanced.clone()))
            .await
            .unwrap();
        let mut completed = advanced;
        completed.revision = 3;
        completed.next_candidate = 2;
        completed.already_absent = 1;
        completed.updated_at += chrono::Duration::microseconds(1);
        completed.completed_at = Some(completed.updated_at);
        let completed_generation = catalog
            .apply(CatalogMutation::PublishLocalGcProgress(completed.clone()))
            .await
            .unwrap();
        assert_eq!(
            catalog
                .apply(CatalogMutation::PublishLocalGcProgress(completed.clone()))
                .await
                .unwrap(),
            completed_generation,
            "lost completion response reconciles after guard retirement"
        );
        let completed_snapshot = catalog.snapshot().await.unwrap();
        assert!(!completed_snapshot.local_gc_guards.contains_key(&guard.id));
        assert_eq!(completed_snapshot.local_gc_progress[&guard.id], completed);
        catalog
            .apply(CatalogMutation::ExposePrivateEpoch {
                epoch: epoch.epoch,
                branch_id: owner.id,
                expected_revision: 2,
                exposed_at: completed.updated_at + chrono::Duration::microseconds(1),
            })
            .await
            .unwrap();
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn local_gc_guard_blocks_root_created_clone_after_source_checkpoint_deletion() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("guard-source-clone");
        let catalog = SlateDbCatalog::open(path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        let source_branch = branch("clone-source-owner");
        catalog
            .apply(CatalogMutation::CreateBranch(source_branch.clone()))
            .await
            .unwrap();
        let source = checkpoint(Uuid::new_v4(), source_branch.id, "clone-source");
        catalog
            .apply(CatalogMutation::CreateCheckpoint(source.clone()))
            .await
            .unwrap();
        let (destination, operation) = reserved_create(&source, "incomplete-destination");
        let wrong_parent = Uuid::new_v4();
        let mut misbound_destination = destination.clone();
        misbound_destination.parent_id = Some(wrong_parent);
        let mut misbound_operation = operation.clone();
        misbound_operation.parent_id = Some(wrong_parent);
        assert!(matches!(
            catalog
                .apply(CatalogMutation::ReserveBranchCreate {
                    branch: misbound_destination,
                    operation: Box::new(misbound_operation),
                })
                .await,
            Err(CatalogError::OperationConflict(_))
        ));
        catalog
            .apply(CatalogMutation::ReserveBranchCreate {
                branch: destination,
                operation: Box::new(operation.clone()),
            })
            .await
            .unwrap();
        let root_created_at = operation.updated_at + chrono::Duration::microseconds(1);
        catalog
            .apply(CatalogMutation::RecordBranchCreateRoot {
                operation_id: operation.id,
                expected_revision: operation.revision,
                destination_root: DurableRoot {
                    identity: "branches/incomplete-destination".to_string(),
                    manifest_id: "manifest/incomplete-destination".to_string(),
                },
                updated_at: root_created_at,
            })
            .await
            .unwrap();
        let deleted_at = root_created_at + chrono::Duration::microseconds(1);
        catalog
            .apply(CatalogMutation::DeleteCheckpoint {
                id: source.id,
                expected_revision: source.revision,
                name: source.name.clone(),
                deleted_at,
            })
            .await
            .unwrap();
        catalog.close().await.unwrap();

        let catalog = SlateDbCatalog::open(path, store).await.unwrap();
        let reopened = catalog.snapshot().await.unwrap();
        assert_eq!(
            reopened.tombstones[&source.id].parent_id,
            operation.parent_id
        );
        assert_eq!(
            reopened.branch_create_operations[&operation.id].phase,
            BranchCreatePhase::RootCreated
        );

        let epoch = private_epoch(source_branch.id, 92, deleted_at);
        let mut next_epoch = private_epoch(source_branch.id, 93, deleted_at);
        next_epoch.pool_id = epoch.pool_id;
        catalog
            .apply(CatalogMutation::RegisterPrivateEpoch(epoch.clone()))
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::RegisterPrivateEpoch(next_epoch.clone()))
            .await
            .unwrap();
        let sealed_at = deleted_at + chrono::Duration::microseconds(1);
        catalog
            .apply(CatalogMutation::SealPrivateEpoch {
                epoch: epoch.epoch,
                branch_id: source_branch.id,
                expected_revision: epoch.revision,
                next_epoch: next_epoch.epoch,
                expected_next_revision: next_epoch.revision,
                sealed_at,
            })
            .await
            .unwrap();
        assert!(matches!(
            catalog
                .apply(CatalogMutation::AcquireLocalGcGuard(LocalGcGuardRecord {
                    id: Uuid::new_v4(),
                    revision: 1,
                    branch_id: source_branch.id,
                    epoch: epoch.epoch,
                    epoch_revision: 2,
                    candidate_count: 1,
                    candidate_digest: "b".repeat(64),
                    created_at: sealed_at + chrono::Duration::microseconds(1),
                }))
                .await,
            Err(CatalogError::OperationConflict(_))
        ));
        catalog.snapshot().await.unwrap();
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn migrates_v1_records_before_reading_them() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("migration-v1-v2");
        let db = slatedb::DbBuilder::new(path.clone(), Arc::clone(&store))
            .build()
            .await
            .unwrap();
        let branch_id = Uuid::new_v4();
        let tombstone_id = Uuid::new_v4();
        let now = catalog_timestamp(Utc::now());
        let mut batch = WriteBatch::new();
        put_json(
            &mut batch,
            Bytes::from_static(STATE_KEY),
            &CatalogState {
                schema_version: LEGACY_SCHEMA_VERSION,
                generation: 2,
            },
        )
        .unwrap();
        batch.put(
            branch_key(branch_id),
            serde_json::to_vec(&serde_json::json!({
                "id": branch_id,
                "name": "legacy",
                "state": "ready",
                "root": {"identity": "root/legacy", "manifest_id": "manifest/legacy"},
                "parent_id": null,
                "origin_checkpoint_id": null,
                "created_at": now,
                "updated_at": now
            }))
            .unwrap(),
        );
        batch.put(
            tombstone_key(tombstone_id),
            serde_json::to_vec(&serde_json::json!({
                "id": tombstone_id,
                "kind": "branch",
                "name": "deleted-legacy",
                "deleted_generation": 2,
                "deleted_at": now
            }))
            .unwrap(),
        );
        db.write_with_options(batch, &durable_write_options())
            .await
            .unwrap();
        db.close().await.unwrap();

        let catalog = SlateDbCatalog::open(path, store).await.unwrap();
        let snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(snapshot.schema_version, CATALOG_SCHEMA_VERSION);
        assert_eq!(snapshot.branches[&branch_id].revision, 1);
        assert_eq!(snapshot.tombstones[&tombstone_id].created_at, now);
        assert_eq!(snapshot.tombstones[&tombstone_id].parent_id, None);
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn migrates_v2_through_v17_catalogs_to_production_limits_schema() {
        for prior_version in [
            PREVIOUS_SCHEMA_VERSION,
            OPERATION_SCHEMA_VERSION,
            LEASE_SCHEMA_VERSION,
            DELETION_SCHEMA_VERSION,
            BRANCH_DELETION_SCHEMA_VERSION,
            GC_CAPTURE_SCHEMA_VERSION,
            GC_MARK_SCHEMA_VERSION,
            GC_QUARANTINE_SCHEMA_VERSION,
            GC_REVALIDATION_SCHEMA_VERSION,
            GC_DELETION_SCHEMA_VERSION,
            PRIVATE_EPOCH_SCHEMA_VERSION,
            LOCAL_GC_GUARD_SCHEMA_VERSION,
            TARGETED_PRIVATE_GC_VIEW_SCHEMA_VERSION,
            PRIVATE_GC_BLOCKER_SCHEMA_VERSION,
            TOMBSTONE_CLEANUP_SCHEMA_VERSION,
            SERVER_CATALOG_SCHEMA_VERSION,
        ] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = Path::from(format!("migration-v{prior_version}-v18"));
            let db = slatedb::DbBuilder::new(path.clone(), Arc::clone(&store))
                .build()
                .await
                .unwrap();
            let existing = branch("existing");
            let existing_checkpoint =
                checkpoint(Uuid::new_v4(), existing.id, "existing-checkpoint");
            let mut batch = WriteBatch::new();
            put_json(
                &mut batch,
                Bytes::from_static(STATE_KEY),
                &CatalogState {
                    schema_version: prior_version,
                    generation: 1,
                },
            )
            .unwrap();
            put_json(&mut batch, branch_key(existing.id), &existing).unwrap();
            put_json(
                &mut batch,
                checkpoint_key(existing_checkpoint.id),
                &existing_checkpoint,
            )
            .unwrap();
            db.write_with_options(batch, &durable_write_options())
                .await
                .unwrap();
            db.close().await.unwrap();

            let catalog = SlateDbCatalog::open(path, store).await.unwrap();
            let snapshot = catalog.snapshot().await.unwrap();
            assert_eq!(snapshot.schema_version, CATALOG_SCHEMA_VERSION);
            assert_eq!(
                catalog
                    .get_record::<BranchLineageDepth>(branch_lineage_depth_key(existing.id))
                    .await
                    .unwrap(),
                Some(BranchLineageDepth {
                    branch_id: existing.id,
                    depth: 0,
                })
            );
            assert_eq!(snapshot.branches[&existing.id], existing);
            assert_eq!(
                snapshot.checkpoints[&existing_checkpoint.id],
                existing_checkpoint
            );
            assert!(snapshot.branch_create_operations.is_empty());
            assert!(snapshot.leases.is_empty());
            assert!(snapshot.lease_tombstones.is_empty());
            assert!(snapshot.private_epochs.is_empty());
            assert!(snapshot.local_gc_guards.is_empty());
            assert!(snapshot.local_gc_progress.is_empty());
            let mut expected_blockers = PrivateGcBranchBlockers::empty(existing.id);
            expected_blockers.checkpoints = 1;
            assert_eq!(
                catalog
                    .private_gc_branch_blockers_unlocked(existing.id)
                    .await
                    .unwrap(),
                expected_blockers
            );
            assert_eq!(
                catalog.private_gc_global_blockers_unlocked().await.unwrap(),
                PrivateGcGlobalBlockers::empty()
            );
            catalog.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn v18_migration_accepts_exact_caps_and_rejects_cap_plus_one_on_every_retry() {
        let exact = v17_capacity_fixture(
            "migration-v17-exact-production-capacity",
            MAX_LIVE_BRANCHES,
            crate::catalog::MAX_CHECKPOINTS_PER_BRANCH,
            MAX_ACTIVE_LEASES_PER_BRANCH,
        )
        .await;
        exact.migrate_unlocked().await.unwrap();
        assert_eq!(
            exact.state_unlocked().await.unwrap().schema_version,
            CATALOG_SCHEMA_VERSION
        );
        exact.close().await.unwrap();

        for (path, branches, checkpoints, leases, resource) in [
            (
                "migration-v17-over-branch-capacity",
                MAX_LIVE_BRANCHES + 1,
                0,
                0,
                "live branch",
            ),
            (
                "migration-v17-over-checkpoint-capacity",
                1,
                crate::catalog::MAX_CHECKPOINTS_PER_BRANCH + 1,
                0,
                "checkpoint per branch",
            ),
            (
                "migration-v17-over-lease-capacity",
                1,
                0,
                MAX_ACTIVE_LEASES_PER_BRANCH + 1,
                "active lease per branch",
            ),
        ] {
            let catalog = v17_capacity_fixture(path, branches, checkpoints, leases).await;
            for _ in 0..2 {
                assert!(matches!(
                    catalog.migrate_unlocked().await,
                    Err(CatalogError::Invalid(message))
                        if message.contains("cannot apply") && message.contains(resource)
                ));
                let state = serde_json::from_slice::<CatalogState>(
                    &catalog.db.get(STATE_KEY).await.unwrap().unwrap(),
                )
                .unwrap();
                assert_eq!(state.schema_version, SERVER_CATALOG_SCHEMA_VERSION);
            }
            catalog.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn quarantine_requires_capture_generation_and_blockers_are_durable_and_bounded() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("gc-quarantine-fence"), store)
            .await
            .unwrap();
        let now = catalog_timestamp(Utc::now());
        let root_digest = crate::catalog::gc_root_digest(&[]).unwrap();
        let run = GcRunRecord {
            id: Uuid::new_v4(),
            revision: 1,
            catalog_generation: 0,
            inventory_cutoff: now,
            roots: Vec::new(),
            root_digest: root_digest.clone(),
            segment_pool: ".zerofs/segment-pool".to_string(),
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
        catalog.begin_gc_run(0, run.clone()).await.unwrap();
        let mark_shards = (0u8..=u8::MAX)
            .map(|shard| GcMarkShard {
                shard,
                location: format!("marks/{shard:02x}"),
                checksum: "00".repeat(32),
                segment_count: 0,
            })
            .collect::<Vec<_>>();
        catalog
            .publish_gc_marks(
                run.id,
                1,
                root_digest.clone(),
                mark_shards,
                GcMarkStats {
                    roots_enumerated: 0,
                    references_enumerated: 0,
                    intermediate_runs: 0,
                    unique_segments: 0,
                },
                now,
            )
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::CreateBranch(branch("generation-change")))
            .await
            .unwrap();
        let quarantine_shards = (0u8..=u8::MAX)
            .map(|shard| GcQuarantineShard {
                shard,
                location: format!("quarantine/{shard:02x}"),
                checksum: "00".repeat(32),
                candidate_count: 0,
                candidate_bytes: 0,
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            catalog
                .publish_gc_quarantine(GcQuarantinePublication {
                    id: run.id,
                    expected_revision: 2,
                    expected_generation: 0,
                    root_digest,
                    quarantine_shards,
                    inventory_stats: GcInventoryStats {
                        objects_seen: 0,
                        objects_newer_than_cutoff: 0,
                        reachable_objects: 0,
                        candidate_objects: 0,
                        candidate_bytes: 0,
                        intermediate_runs: 0,
                    },
                    quarantine_at: now,
                })
                .await,
            Err(CatalogError::RevisionConflict {
                expected: 0,
                actual: 1
            })
        ));
        for kind in [
            GcBlockerKind::MissingRoot,
            GcBlockerKind::CorruptMetadata,
            GcBlockerKind::GenerationChanged,
            GcBlockerKind::LeaseUncertainty,
            GcBlockerKind::StorageUnavailable,
        ] {
            catalog
                .record_gc_blocker(run.id, kind, format!("blocked by {kind:?}"), now)
                .await
                .unwrap();
        }
        let blockers = catalog.gc_blockers(run.id).await.unwrap();
        assert_eq!(blockers.len(), 5);
        assert!(blockers.iter().all(|blocker| blocker.occurrences == 1));
        assert_eq!(catalog.gc_run(run.id).await.unwrap().unwrap().revision, 2);
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn revalidation_capture_and_publication_are_generation_fenced() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("gc-revalidation-fence"), store)
            .await
            .unwrap();
        let now = catalog_timestamp(Utc::now());
        let digest = crate::catalog::gc_root_digest(&[]).unwrap();
        let run_id = Uuid::new_v4();
        catalog
            .begin_gc_run(
                0,
                GcRunRecord {
                    id: run_id,
                    revision: 1,
                    catalog_generation: 0,
                    inventory_cutoff: now,
                    roots: Vec::new(),
                    root_digest: digest.clone(),
                    segment_pool: ".zerofs/segment-pool".to_string(),
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
        let marks = (0u8..=u8::MAX)
            .map(|shard| GcMarkShard {
                shard,
                location: format!("marks/{shard:02x}"),
                checksum: "00".repeat(32),
                segment_count: 0,
            })
            .collect::<Vec<_>>();
        let mark_stats = GcMarkStats {
            roots_enumerated: 0,
            references_enumerated: 0,
            intermediate_runs: 0,
            unique_segments: 0,
        };
        catalog
            .publish_gc_marks(run_id, 1, digest.clone(), marks, mark_stats.clone(), now)
            .await
            .unwrap();
        let candidates = (0u8..=u8::MAX)
            .map(|shard| GcQuarantineShard {
                shard,
                location: format!("candidates/{shard:02x}"),
                checksum: "00".repeat(32),
                candidate_count: 0,
                candidate_bytes: 0,
            })
            .collect::<Vec<_>>();
        catalog
            .publish_gc_quarantine(GcQuarantinePublication {
                id: run_id,
                expected_revision: 2,
                expected_generation: 0,
                root_digest: digest.clone(),
                quarantine_shards: candidates,
                inventory_stats: GcInventoryStats {
                    objects_seen: 0,
                    objects_newer_than_cutoff: 0,
                    reachable_objects: 0,
                    candidate_objects: 0,
                    candidate_bytes: 0,
                    intermediate_runs: 0,
                },
                quarantine_at: now,
            })
            .await
            .unwrap();
        let captured_at = now
            + chrono::Duration::seconds(super::super::gc::MIN_REVALIDATION_GRACE_SECONDS as i64);
        let observation_id = Uuid::new_v4();
        catalog
            .begin_gc_revalidation(GcRevalidationCapture {
                run_id,
                expected_revision: 3,
                expected_generation: 0,
                observation: GcRevalidationRecord {
                    id: observation_id,
                    catalog_generation: 0,
                    grace_seconds: super::super::gc::MIN_REVALIDATION_GRACE_SECONDS,
                    not_before: captured_at,
                    inventory_cutoff: captured_at,
                    roots: Vec::new(),
                    root_digest: digest.clone(),
                    mark_shards: Vec::new(),
                    mark_stats: None,
                    candidate_shards: Vec::new(),
                    stats: None,
                    captured_at,
                    completed_at: None,
                },
                updated_at: captured_at,
            })
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::CreateBranch(branch("revalidation-race")))
            .await
            .unwrap();
        assert!(matches!(
            catalog
                .publish_gc_revalidation(GcRevalidationPublication {
                    run_id,
                    expected_revision: 4,
                    expected_generation: 0,
                    observation_id,
                    root_digest: digest,
                    mark_shards: (0u8..=u8::MAX)
                        .map(|shard| GcMarkShard {
                            shard,
                            location: format!("second-marks/{shard:02x}"),
                            checksum: "00".repeat(32),
                            segment_count: 0,
                        })
                        .collect(),
                    mark_stats,
                    candidate_shards: (0u8..=u8::MAX)
                        .map(|shard| GcQuarantineShard {
                            shard,
                            location: format!("second-candidates/{shard:02x}"),
                            checksum: "00".repeat(32),
                            candidate_count: 0,
                            candidate_bytes: 0,
                        })
                        .collect(),
                    stats: GcRevalidationStats {
                        first_observation_candidates: 0,
                        became_reachable: 0,
                        already_absent: 0,
                        retained_candidates: 0,
                        retained_bytes: 0,
                    },
                    completed_at: captured_at,
                })
                .await,
            Err(CatalogError::RevisionConflict {
                expected: 0,
                actual: 1
            })
        ));
        let retained = catalog.gc_run(run_id).await.unwrap().unwrap();
        assert_eq!(retained.phase, GcRunPhase::Revalidating);
        assert_eq!(retained.revision, 4);
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_v1_migration_never_flips_the_schema_marker() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("migration-v1-invalid-v2");
        let db = Arc::new(slatedb::DbBuilder::new(path, store).build().await.unwrap());
        let branch_id = Uuid::new_v4();
        let now = catalog_timestamp(Utc::now());
        let mut batch = WriteBatch::new();
        put_json(
            &mut batch,
            Bytes::from_static(STATE_KEY),
            &CatalogState {
                schema_version: LEGACY_SCHEMA_VERSION,
                generation: 1,
            },
        )
        .unwrap();
        batch.put(
            branch_key(branch_id),
            serde_json::to_vec(&serde_json::json!({
                "id": branch_id,
                "name": "legacy-oversized",
                "state": "ready",
                "root": {
                    "identity": "x".repeat(crate::catalog::MAX_ROOT_IDENTIFIER_BYTES + 1),
                    "manifest_id": "manifest/legacy"
                },
                "parent_id": null,
                "origin_checkpoint_id": null,
                "created_at": now,
                "updated_at": now
            }))
            .unwrap(),
        );
        db.write_with_options(batch, &durable_write_options())
            .await
            .unwrap();
        let catalog = SlateDbCatalog {
            db,
            lock: Mutex::new(()),
        };

        for _ in 0..2 {
            assert!(matches!(
                catalog.migrate_unlocked().await,
                Err(CatalogError::Invalid(message)) if message.contains("cannot migrate")
            ));
            let state = serde_json::from_slice::<CatalogState>(
                &catalog.db.get(STATE_KEY).await.unwrap().unwrap(),
            )
            .unwrap();
            assert_eq!(state.schema_version, LEGACY_SCHEMA_VERSION);
        }
        catalog.close().await.unwrap();
    }
}
