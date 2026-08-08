use super::lease::LEASE_CLOCK_SKEW;
use super::{
    BranchCreateOperation, BranchCreatePhase, BranchRecord, BranchState, CATALOG_SCHEMA_VERSION,
    Catalog, CatalogError, CatalogMutation, CatalogSnapshot, CheckpointRecord, LeaseAccessMode,
    LeaseRecord, LeaseSubjectKind, LeaseTombstone, TombstoneKind, TombstoneRecord, validate_name,
    validate_root, validate_timestamp,
};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::ObjectStore;
use serde::{Serialize, de::DeserializeOwned};
use slatedb::config::WriteOptions;
use slatedb::object_store::path::Path;
use slatedb::{Db, WriteBatch};
use std::collections::BTreeSet;
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
const BRANCH_CREATE_SOURCE_PREFIX: &[u8] = b"catalog/branch-create-source/";
const LEASE_PREFIX: &[u8] = b"catalog/lease/";
const LEASE_TOMBSTONE_PREFIX: &[u8] = b"catalog/lease-tombstone/";
const LEGACY_SCHEMA_VERSION: u32 = 1;
const PREVIOUS_SCHEMA_VERSION: u32 = 2;
const OPERATION_SCHEMA_VERSION: u32 = 3;
const LEASE_SCHEMA_VERSION: u32 = 4;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CatalogState {
    schema_version: u32,
    generation: u64,
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
            catalog
                .db
                .put_with_options(
                    STATE_KEY,
                    serde_json::to_vec(&state)?,
                    &slatedb::config::PutOptions::default(),
                    &durable_write_options(),
                )
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
        let leases = self.scan_records::<LeaseRecord>(LEASE_PREFIX).await?;
        let lease_tombstones = self
            .scan_records::<LeaseTombstone>(LEASE_TOMBSTONE_PREFIX)
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
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    async fn get_record<T: DeserializeOwned>(&self, key: Bytes) -> Result<Option<T>, CatalogError> {
        self.db
            .get(key)
            .await?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
            .transpose()
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

    async fn apply_unlocked(&self, mutation: CatalogMutation) -> Result<u64, CatalogError> {
        let state = self.state_unlocked().await?;
        let next_generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| CatalogError::Corrupt("catalog generation overflow".to_string()))?;
        let mut batch = WriteBatch::new();

        match mutation {
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
                match lease.subject_kind {
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
                    }
                }
                put_json(&mut batch, lease_key(lease.id), &lease)?;
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
                batch.delete(lease_key(id));
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
                batch.delete(lease_key(id));
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
                if source.root != operation.source_root {
                    return Err(CatalogError::OperationConflict(format!(
                        "source checkpoint {} root changed",
                        source.id
                    )));
                }
                if let Some(parent_id) = operation.parent_id {
                    ensure_known_resource(self.db.as_ref(), parent_id, TombstoneKind::Branch)
                        .await?;
                }
                put_json(&mut batch, branch_key(branch.id), &branch)?;
                batch.put(branch_name_key(&branch.name), branch.id.to_string());
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
                put_json(
                    &mut batch,
                    branch_create_operation_key(operation.id),
                    &operation,
                )?;
                put_json(&mut batch, branch_key(branch.id), &branch)?;
            }
            CatalogMutation::CreateBranch(record) => {
                record.validate()?;
                if record.state == BranchState::Creating {
                    return Err(CatalogError::Invalid(
                        "creating branches must be reserved with an operation".to_string(),
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
                put_json(&mut batch, branch_key(record.id), &record)?;
                batch.put(branch_name_key(&record.name), record.id.to_string());
            }
            CatalogMutation::ReplaceBranch {
                expected_revision,
                record,
            } => {
                record.validate()?;
                let old = self
                    .get_record::<BranchRecord>(branch_key(record.id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(record.id.to_string()))?;
                if old.state == BranchState::Creating
                    || record.state == BranchState::Creating
                    || old.state != record.state
                    || old.parent_id != record.parent_id
                    || old.origin_checkpoint_id != record.origin_checkpoint_id
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
            CatalogMutation::DeleteBranch {
                id,
                expected_revision,
                name,
                deleted_at,
            } => {
                validate_name(&name)?;
                validate_timestamp(deleted_at, "branch deleted_at")?;
                let old = self
                    .get_record::<BranchRecord>(branch_key(id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(id.to_string()))?;
                if old.state == BranchState::Creating {
                    return Err(CatalogError::OperationConflict(format!(
                        "creating branch {id} must be aborted through its operation"
                    )));
                }
                if old.name != name {
                    return Err(CatalogError::NotFound(format!("{name} ({id})")));
                }
                ensure_expected_revision(expected_revision, old.revision)?;
                if deleted_at < old.created_at {
                    return Err(CatalogError::Invalid(
                        "branch deletion cannot precede creation".to_string(),
                    ));
                }
                ensure_absent(self.db.as_ref(), tombstone_key(id), &id.to_string()).await?;
                batch.delete(branch_key(id));
                batch.delete(branch_name_key(&name));
                put_json(
                    &mut batch,
                    tombstone_key(id),
                    &TombstoneRecord {
                        id,
                        kind: TombstoneKind::Branch,
                        name,
                        parent_id: old.parent_id,
                        origin_checkpoint_id: old.origin_checkpoint_id,
                        created_at: old.created_at,
                        deleted_revision: Some(old.revision),
                        deleted_generation: next_generation,
                        deleted_at,
                    },
                )?;
            }
            CatalogMutation::CreateCheckpoint(record) => {
                record.validate()?;
                ensure_initial_revision(record.revision)?;
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
                put_json(&mut batch, checkpoint_key(record.id), &record)?;
                batch.put(
                    checkpoint_name_key(record.branch_id, &record.name),
                    record.id.to_string(),
                );
            }
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
                batch.delete(checkpoint_key(id));
                batch.delete(checkpoint_name_key(old.branch_id, &name));
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

    async fn lease(&self, id: Uuid) -> Result<Option<LeaseRecord>, CatalogError> {
        let _guard = self.lock.lock().await;
        self.get_record(lease_key(id)).await
    }

    async fn tombstone(&self, id: Uuid) -> Result<Option<TombstoneRecord>, CatalogError> {
        let _guard = self.lock.lock().await;
        self.get_record(tombstone_key(id)).await
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

async fn ensure_absent(db: &Db, key: Bytes, label: &str) -> Result<(), CatalogError> {
    if db.get(key).await?.is_some() {
        return Err(CatalogError::AlreadyExists(label.to_string()));
    }
    Ok(())
}

async fn ensure_resource_id_available(db: &Db, id: Uuid) -> Result<(), CatalogError> {
    for key in [
        branch_key(id),
        checkpoint_key(id),
        tombstone_key(id),
        branch_create_operation_key(id),
        lease_key(id),
        lease_tombstone_key(id),
    ] {
        ensure_absent(db, key, &id.to_string()).await?;
    }
    Ok(())
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
    use crate::catalog::{BranchState, DurableRoot, catalog_timestamp, lease::LEASE_CLOCK_SKEW};
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

        let deleted_at = catalog_timestamp(Utc::now());
        catalog
            .apply(CatalogMutation::DeleteBranch {
                id: record.id,
                expected_revision: record.revision,
                name: record.name,
                deleted_at,
            })
            .await
            .unwrap();
        let snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(snapshot.generation, 2);
        assert!(snapshot.branches.is_empty());
        assert_eq!(
            snapshot.tombstones.get(&record.id).unwrap().deleted_at,
            deleted_at
        );
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
        catalog
            .apply(CatalogMutation::DeleteBranch {
                id: branch.id,
                expected_revision: branch.revision,
                name: branch.name,
                deleted_at: issued_at,
            })
            .await
            .unwrap();
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
        assert_eq!(catalog.apply(expire.clone()).await.unwrap(), 4);
        assert_eq!(catalog.apply(expire).await.unwrap(), 4);
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

        catalog
            .apply(CatalogMutation::DeleteBranch {
                id: first.id,
                expected_revision: first.revision,
                name: first.name,
                deleted_at: catalog_timestamp(Utc::now()),
            })
            .await
            .unwrap();
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
    async fn migrates_v2_through_v4_catalogs_to_deletion_schema_without_rewriting_records() {
        for prior_version in [
            PREVIOUS_SCHEMA_VERSION,
            OPERATION_SCHEMA_VERSION,
            LEASE_SCHEMA_VERSION,
        ] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = Path::from(format!("migration-v{prior_version}-v5"));
            let db = slatedb::DbBuilder::new(path.clone(), Arc::clone(&store))
                .build()
                .await
                .unwrap();
            let existing = branch("existing");
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
            db.write_with_options(batch, &durable_write_options())
                .await
                .unwrap();
            db.close().await.unwrap();

            let catalog = SlateDbCatalog::open(path, store).await.unwrap();
            let snapshot = catalog.snapshot().await.unwrap();
            assert_eq!(snapshot.schema_version, CATALOG_SCHEMA_VERSION);
            assert_eq!(snapshot.branches[&existing.id], existing);
            assert!(snapshot.branch_create_operations.is_empty());
            assert!(snapshot.leases.is_empty());
            assert!(snapshot.lease_tombstones.is_empty());
            catalog.close().await.unwrap();
        }
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
