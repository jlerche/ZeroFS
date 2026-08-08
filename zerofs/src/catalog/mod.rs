//! Durable metadata catalog for copy-on-write branches.
//!
//! Storage-critical state has one authority: SlateDB. PostgreSQL is an
//! optional, reconstructible customer-facing projection and is never consulted
//! to mount a branch or decide storage liveness. JSON and PostgreSQL implement
//! the same projection contract.

mod deletion;
mod json;
mod lease;
mod lifecycle;
mod postgres;
mod root_store;
#[path = "slatedb.rs"]
mod slate;

use async_trait::async_trait;
use chrono::{DateTime, Timelike, Utc};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

pub use deletion::{
    BranchDeleteRequest, BranchDeleteResult, CheckpointDeleteRequest, DeletionLifecycle,
    DeletionLifecycleError,
};
pub use json::JsonCatalogProjection;
pub use lease::{
    LeaseAcquireRequest, LeaseGrant, LeaseLifecycle, LeaseLifecycleError, LeaseRenewRequest,
};
pub use lifecycle::{
    BranchCreateFromCheckpointNameRequest, BranchCreateRequest, BranchLifecycle,
    BranchLifecycleError,
};
pub use postgres::PostgresCatalogProjection;
pub use root_store::{ImmutableCheckpoint, RootStoreError, SlateDbRootStore};
pub(crate) use slate::SlateDbCatalog;

pub const CATALOG_SCHEMA_VERSION: u32 = 6;
pub const CATALOG_PROJECTION_SCHEMA_VERSION: u32 = 1;
pub const MAX_CATALOG_NAME_BYTES: usize = 255;
pub const MAX_ROOT_IDENTIFIER_BYTES: usize = 4 * 1024;
pub const MAX_CUSTOMER_METADATA_BYTES: usize = 64 * 1024;

pub type CustomerMetadata = Map<String, Value>;

/// Normalize a timestamp to PostgreSQL's microsecond precision so projections
/// and authoritative backends have identical round-trip behavior.
pub fn catalog_timestamp(value: DateTime<Utc>) -> DateTime<Utc> {
    value
        .with_nanosecond((value.timestamp_subsec_nanos() / 1_000) * 1_000)
        .expect("truncating a valid nanosecond value remains valid")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogConfig {
    pub slatedb_path: String,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            slatedb_path: ".zerofs/catalog".to_string(),
        }
    }
}

impl CatalogConfig {
    /// Open the authoritative SlateDB catalog behind the safe branch lifecycle
    /// boundary. Raw catalog mutations remain crate-private.
    pub async fn open_branch_lifecycle(
        &self,
        object_store: Arc<dyn ObjectStore>,
        branch_database_root: slatedb::object_store::path::Path,
    ) -> Result<BranchLifecycle, BranchLifecycleError> {
        let catalog_path = slatedb::object_store::path::Path::from(self.slatedb_path.as_str());
        root_store::ensure_database_namespaces_disjoint(
            "catalog",
            &catalog_path,
            "branch root",
            &branch_database_root,
        )?;
        let catalog: Arc<dyn Catalog> =
            Arc::new(SlateDbCatalog::open(catalog_path, Arc::clone(&object_store)).await?);
        Ok(BranchLifecycle::new(
            catalog,
            SlateDbRootStore::new(object_store, branch_database_root),
        ))
    }
}

/// Customer projection selection. JSON is the local/testing default;
/// PostgreSQL is selected explicitly in production.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
pub enum CatalogProjectionConfig {
    Json { path: PathBuf },
    Postgres { connection_string: String },
}

impl Default for CatalogProjectionConfig {
    fn default() -> Self {
        Self::Json {
            path: PathBuf::from(".zerofs/catalog-projection.json"),
        }
    }
}

impl std::fmt::Debug for CatalogProjectionConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json { path } => formatter.debug_struct("Json").field("path", path).finish(),
            Self::Postgres { .. } => formatter
                .debug_struct("Postgres")
                .field("connection_string", &"[REDACTED]")
                .finish(),
        }
    }
}

impl CatalogProjectionConfig {
    pub async fn open(&self) -> Result<Arc<dyn CatalogProjection>, CatalogError> {
        match self {
            Self::Json { path } => Ok(Arc::new(JsonCatalogProjection::new(path))),
            Self::Postgres { connection_string } => {
                let projection = PostgresCatalogProjection::connect(connection_string).await?;
                projection.migrate().await?;
                Ok(Arc::new(projection))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchState {
    Creating,
    Ready,
    Deleting,
}

impl BranchState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Creating => "creating",
            Self::Ready => "ready",
            Self::Deleting => "deleting",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableRoot {
    /// Opaque identity understood only by the storage layer.
    pub identity: String,
    /// Immutable manifest/checkpoint identity that pins the branch contents.
    pub manifest_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchRecord {
    pub id: Uuid,
    /// Per-record optimistic concurrency token. Creation starts at one.
    pub revision: u64,
    pub name: String,
    pub state: BranchState,
    pub root: Option<DurableRoot>,
    /// Historical lineage only; this is never a liveness dependency.
    pub parent_id: Option<Uuid>,
    pub origin_checkpoint_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl BranchRecord {
    pub fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.id, "branch")?;
        validate_revision(self.revision, "branch")?;
        validate_name(&self.name)?;
        validate_optional_id(self.parent_id, "branch parent")?;
        validate_optional_id(self.origin_checkpoint_id, "origin checkpoint")?;
        if self.parent_id == Some(self.id) {
            return Err(CatalogError::Invalid(
                "a branch cannot be its own parent".to_string(),
            ));
        }
        if self.state == BranchState::Ready && self.root.is_none() {
            return Err(CatalogError::Invalid(
                "a ready branch must have an independent durable root".to_string(),
            ));
        }
        if let Some(root) = &self.root {
            validate_root(root)?;
        }
        validate_times(self.created_at, self.updated_at, "branch")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub id: Uuid,
    pub revision: u64,
    pub branch_id: Uuid,
    pub name: String,
    pub root: DurableRoot,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseSubjectKind {
    Branch,
    Checkpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseAccessMode {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeaseRecord {
    pub id: Uuid,
    pub revision: u64,
    pub subject_kind: LeaseSubjectKind,
    pub subject_id: Uuid,
    pub root: DurableRoot,
    pub access_mode: LeaseAccessMode,
    pub token_hash: String,
    pub issued_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl LeaseRecord {
    pub fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.id, "lease")?;
        validate_revision(self.revision, "lease")?;
        validate_id(self.subject_id, "lease subject")?;
        validate_root(&self.root)?;
        if self.subject_kind == LeaseSubjectKind::Checkpoint
            && self.access_mode != LeaseAccessMode::Read
        {
            return Err(CatalogError::Invalid(
                "checkpoint leases must be read-only".to_string(),
            ));
        }
        if self.token_hash.len() != 64
            || !self.token_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(CatalogError::Invalid(
                "lease token hash must be 64 hexadecimal bytes".to_string(),
            ));
        }
        validate_timestamp(self.issued_at, "lease issued_at")?;
        validate_timestamp(self.updated_at, "lease updated_at")?;
        validate_timestamp(self.expires_at, "lease expires_at")?;
        if self.updated_at < self.issued_at || self.expires_at <= self.updated_at {
            return Err(CatalogError::Invalid(
                "lease times must satisfy issued_at <= updated_at < expires_at".to_string(),
            ));
        }
        if self.expires_at - self.updated_at > lease::MAX_LEASE_DURATION {
            return Err(CatalogError::Invalid(
                "lease duration exceeds the production maximum".to_string(),
            ));
        }
        Ok(())
    }

    pub fn is_unexpired(&self, now: DateTime<Utc>) -> bool {
        now < self.expires_at
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeaseTombstone {
    pub id: Uuid,
    pub token_hash: String,
    pub ended_at: DateTime<Utc>,
}

impl LeaseTombstone {
    fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.id, "lease tombstone")?;
        validate_timestamp(self.ended_at, "lease tombstone ended_at")?;
        if self.token_hash.len() != 64
            || !self.token_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(CatalogError::Invalid(
                "lease tombstone token hash must be 64 hexadecimal bytes".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchCreatePhase {
    Reserved,
    RootCreated,
    Published,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchCreateOperation {
    pub id: Uuid,
    pub revision: u64,
    pub destination_id: Uuid,
    pub destination_name: String,
    pub source_checkpoint_id: Uuid,
    pub source_root: DurableRoot,
    pub parent_id: Option<Uuid>,
    pub phase: BranchCreatePhase,
    pub destination_root: Option<DurableRoot>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchDeletePhase {
    Draining,
    Published,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchDeleteOperation {
    pub id: Uuid,
    pub revision: u64,
    pub branch_id: Uuid,
    pub branch_name: String,
    pub expected_branch_revision: u64,
    pub root: DurableRoot,
    pub parent_id: Option<Uuid>,
    pub origin_checkpoint_id: Option<Uuid>,
    pub phase: BranchDeletePhase,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl BranchDeleteOperation {
    pub fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.id, "branch delete operation")?;
        validate_revision(self.revision, "branch delete operation")?;
        validate_id(self.branch_id, "branch delete subject")?;
        validate_name(&self.branch_name)?;
        validate_revision(self.expected_branch_revision, "deleted branch")?;
        validate_root(&self.root)?;
        validate_optional_id(self.parent_id, "deleted branch parent")?;
        validate_optional_id(
            self.origin_checkpoint_id,
            "deleted branch origin checkpoint",
        )?;
        if self.id == self.branch_id {
            return Err(CatalogError::Invalid(
                "branch and deletion operation UUIDs must differ".to_string(),
            ));
        }
        validate_times(self.created_at, self.updated_at, "branch delete operation")
    }
}

impl BranchCreateOperation {
    pub fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.id, "branch create operation")?;
        validate_revision(self.revision, "branch create operation")?;
        validate_id(self.destination_id, "branch create destination")?;
        validate_name(&self.destination_name)?;
        validate_id(self.source_checkpoint_id, "branch create source checkpoint")?;
        validate_optional_id(self.parent_id, "branch create parent")?;
        validate_root(&self.source_root)?;
        if self.destination_id == self.source_checkpoint_id
            || self.id == self.destination_id
            || self.id == self.source_checkpoint_id
        {
            return Err(CatalogError::Invalid(
                "branch operation, destination, and source checkpoint UUIDs must differ"
                    .to_string(),
            ));
        }
        match (&self.phase, &self.destination_root) {
            (BranchCreatePhase::Reserved, None) => {}
            (BranchCreatePhase::RootCreated | BranchCreatePhase::Published, Some(root)) => {
                validate_root(root)?;
            }
            (BranchCreatePhase::Reserved, Some(_)) => {
                return Err(CatalogError::Invalid(
                    "a reserved branch operation cannot have a destination root".to_string(),
                ));
            }
            (BranchCreatePhase::RootCreated | BranchCreatePhase::Published, None) => {
                return Err(CatalogError::Invalid(
                    "a root-created or published branch operation requires a destination root"
                        .to_string(),
                ));
            }
        }
        validate_times(self.created_at, self.updated_at, "branch create operation")
    }

    pub(crate) fn immutable_inputs_equal(&self, other: &Self) -> bool {
        self.id == other.id
            && self.destination_id == other.destination_id
            && self.destination_name == other.destination_name
            && self.source_checkpoint_id == other.source_checkpoint_id
            && self.source_root == other.source_root
            && self.parent_id == other.parent_id
            && self.created_at == other.created_at
    }

    /// Exact storage roots retained while this operation is incomplete.
    /// Published records remain only as idempotency reservations.
    pub fn gc_roots(&self) -> Vec<&DurableRoot> {
        match self.phase {
            BranchCreatePhase::Reserved => vec![&self.source_root],
            BranchCreatePhase::RootCreated => self.destination_root.iter().collect(),
            BranchCreatePhase::Published => Vec::new(),
        }
    }
}

impl CheckpointRecord {
    pub fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.id, "checkpoint")?;
        validate_revision(self.revision, "checkpoint")?;
        validate_id(self.branch_id, "checkpoint branch")?;
        validate_name(&self.name)?;
        validate_root(&self.root)?;
        validate_times(self.created_at, self.updated_at, "checkpoint")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TombstoneKind {
    Branch,
    Checkpoint,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TombstoneRecord {
    pub id: Uuid,
    pub kind: TombstoneKind,
    pub name: String,
    /// Historical customer-facing lineage only; never a GC dependency.
    pub parent_id: Option<Uuid>,
    pub origin_checkpoint_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    /// Exact live-record revision consumed by deletion. Older migrated
    /// tombstones may not carry this proof and cannot satisfy exact retries.
    #[serde(default)]
    pub deleted_revision: Option<u64>,
    #[serde(default)]
    pub deletion_operation_id: Option<Uuid>,
    pub deleted_generation: u64,
    pub deleted_at: DateTime<Utc>,
}

impl TombstoneRecord {
    fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.id, "tombstone")?;
        validate_name(&self.name)?;
        validate_optional_id(self.parent_id, "tombstone parent")?;
        validate_optional_id(self.origin_checkpoint_id, "tombstone origin checkpoint")?;
        if self.deleted_revision == Some(0) {
            return Err(CatalogError::Invalid(
                "tombstone deleted revision cannot be zero".to_string(),
            ));
        }
        validate_optional_id(self.deletion_operation_id, "tombstone deletion operation")?;
        validate_timestamp(self.created_at, "tombstone created_at")?;
        validate_timestamp(self.deleted_at, "tombstone deleted_at")?;
        if self.deleted_at < self.created_at {
            return Err(CatalogError::Invalid(
                "tombstone deletion cannot precede creation".to_string(),
            ));
        }
        if self.deleted_generation == 0 {
            return Err(CatalogError::Invalid(
                "tombstone deletion generation must be nonzero".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub schema_version: u32,
    pub generation: u64,
    #[serde(default)]
    pub branches: BTreeMap<Uuid, BranchRecord>,
    #[serde(default)]
    pub checkpoints: BTreeMap<Uuid, CheckpointRecord>,
    #[serde(default)]
    pub branch_create_operations: BTreeMap<Uuid, BranchCreateOperation>,
    #[serde(default)]
    pub branch_delete_operations: BTreeMap<Uuid, BranchDeleteOperation>,
    #[serde(default)]
    pub leases: BTreeMap<Uuid, LeaseRecord>,
    #[serde(default)]
    pub lease_tombstones: BTreeMap<Uuid, LeaseTombstone>,
    #[serde(default)]
    pub tombstones: BTreeMap<Uuid, TombstoneRecord>,
}

impl Default for CatalogSnapshot {
    fn default() -> Self {
        Self {
            schema_version: CATALOG_SCHEMA_VERSION,
            generation: 0,
            branches: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            branch_create_operations: BTreeMap::new(),
            branch_delete_operations: BTreeMap::new(),
            leases: BTreeMap::new(),
            lease_tombstones: BTreeMap::new(),
            tombstones: BTreeMap::new(),
        }
    }
}

impl CatalogSnapshot {
    pub fn validate(&self) -> Result<(), CatalogError> {
        if self.schema_version != CATALOG_SCHEMA_VERSION {
            return Err(CatalogError::Corrupt(format!(
                "unsupported catalog schema version {}",
                self.schema_version
            )));
        }
        validate_records(&self.branches, |record| record.id, BranchRecord::validate)?;
        validate_records(
            &self.checkpoints,
            |record| record.id,
            CheckpointRecord::validate,
        )?;
        validate_records(
            &self.branch_create_operations,
            |record| record.id,
            BranchCreateOperation::validate,
        )?;
        validate_records(
            &self.branch_delete_operations,
            |record| record.id,
            BranchDeleteOperation::validate,
        )?;
        validate_records(&self.leases, |record| record.id, LeaseRecord::validate)?;
        validate_records(
            &self.lease_tombstones,
            |record| record.id,
            LeaseTombstone::validate,
        )?;
        validate_records(
            &self.tombstones,
            |record| record.id,
            TombstoneRecord::validate,
        )?;
        if self
            .tombstones
            .values()
            .any(|record| record.deleted_generation > self.generation)
        {
            return Err(CatalogError::Corrupt(
                "tombstone deletion generation exceeds snapshot generation".to_string(),
            ));
        }
        let branch_ids = self
            .branches
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let checkpoint_ids = self
            .checkpoints
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let tombstone_ids = self
            .tombstones
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let operation_ids = self
            .branch_create_operations
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let delete_operation_ids = self
            .branch_delete_operations
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let lease_ids = self
            .leases
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let lease_tombstone_ids = self
            .lease_tombstones
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if !branch_ids.is_disjoint(&checkpoint_ids)
            || !branch_ids.is_disjoint(&tombstone_ids)
            || !checkpoint_ids.is_disjoint(&tombstone_ids)
            || !operation_ids.is_disjoint(&branch_ids)
            || !operation_ids.is_disjoint(&checkpoint_ids)
            || !operation_ids.is_disjoint(&tombstone_ids)
            || !lease_ids.is_disjoint(&branch_ids)
            || !lease_ids.is_disjoint(&checkpoint_ids)
            || !lease_ids.is_disjoint(&operation_ids)
            || !lease_ids.is_disjoint(&tombstone_ids)
            || !lease_tombstone_ids.is_disjoint(&branch_ids)
            || !lease_tombstone_ids.is_disjoint(&checkpoint_ids)
            || !lease_tombstone_ids.is_disjoint(&operation_ids)
            || !lease_tombstone_ids.is_disjoint(&tombstone_ids)
            || !lease_tombstone_ids.is_disjoint(&lease_ids)
            || !delete_operation_ids.is_disjoint(&branch_ids)
            || !delete_operation_ids.is_disjoint(&checkpoint_ids)
            || !delete_operation_ids.is_disjoint(&operation_ids)
            || !delete_operation_ids.is_disjoint(&lease_ids)
            || !delete_operation_ids.is_disjoint(&lease_tombstone_ids)
            || !delete_operation_ids.is_disjoint(&tombstone_ids)
        {
            return Err(CatalogError::Corrupt(
                "resource UUID appears in more than one catalog collection".to_string(),
            ));
        }
        validate_unique_names(
            self.branches.values().map(|record| record.name.as_str()),
            "branch",
        )?;
        validate_unique_names(
            self.checkpoints
                .values()
                .map(|record| (record.branch_id, record.name.as_str())),
            "checkpoint",
        )?;
        validate_branch_create_relationships(self)?;
        validate_branch_delete_relationships(self)?;
        for lease in self.leases.values() {
            let expected_kind = match lease.subject_kind {
                LeaseSubjectKind::Branch => TombstoneKind::Branch,
                LeaseSubjectKind::Checkpoint => TombstoneKind::Checkpoint,
            };
            let live = match lease.subject_kind {
                LeaseSubjectKind::Branch => self.branches.contains_key(&lease.subject_id),
                LeaseSubjectKind::Checkpoint => self.checkpoints.contains_key(&lease.subject_id),
            };
            let deleted = self
                .tombstones
                .get(&lease.subject_id)
                .is_some_and(|tombstone| tombstone.kind == expected_kind);
            if !live && !deleted {
                return Err(CatalogError::Corrupt(format!(
                    "lease {} refers to an unknown subject {}",
                    lease.id, lease.subject_id
                )));
            }
        }
        Ok(())
    }

    /// Complete authoritative durable-root set at one captured generation.
    pub fn gc_roots(&self) -> Vec<&DurableRoot> {
        let mut roots = Vec::new();
        roots.extend(
            self.branches
                .values()
                .filter_map(|branch| branch.root.as_ref()),
        );
        roots.extend(self.checkpoints.values().map(|checkpoint| &checkpoint.root));
        roots.extend(
            self.branch_create_operations
                .values()
                .flat_map(BranchCreateOperation::gc_roots),
        );
        roots.extend(
            self.branch_delete_operations
                .values()
                .filter(|operation| operation.phase == BranchDeletePhase::Draining)
                .map(|operation| &operation.root),
        );
        roots.extend(self.leases.values().map(|lease| &lease.root));
        roots
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // General resource mutations are consumed by later lifecycle slices.
pub(crate) enum CatalogMutation {
    StartBranchDelete {
        operation: BranchDeleteOperation,
    },
    FinalizeBranchDelete {
        operation_id: Uuid,
        expected_revision: u64,
        deleted_at: DateTime<Utc>,
    },
    AcquireLease {
        expected_subject_revision: u64,
        lease: LeaseRecord,
    },
    RenewLease {
        id: Uuid,
        expected_revision: u64,
        token_hash: String,
        renewed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    },
    EndLease {
        id: Uuid,
        expected_revision: u64,
        token_hash: String,
        ended_at: DateTime<Utc>,
    },
    ExpireLease {
        id: Uuid,
        expected_revision: u64,
        observed_at: DateTime<Utc>,
    },
    ReserveBranchCreate {
        branch: BranchRecord,
        operation: Box<BranchCreateOperation>,
    },
    RecordBranchCreateRoot {
        operation_id: Uuid,
        expected_revision: u64,
        destination_root: DurableRoot,
        updated_at: DateTime<Utc>,
    },
    PublishBranchCreate {
        operation_id: Uuid,
        expected_revision: u64,
        updated_at: DateTime<Utc>,
    },
    CreateBranch(BranchRecord),
    ReplaceBranch {
        expected_revision: u64,
        record: BranchRecord,
    },
    CreateCheckpoint(CheckpointRecord),
    ReplaceCheckpoint {
        expected_revision: u64,
        record: CheckpointRecord,
    },
    DeleteCheckpoint {
        id: Uuid,
        expected_revision: u64,
        name: String,
        deleted_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CustomerResourceKind {
    Branch,
    Checkpoint,
}

impl CustomerResourceKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Branch => "branch",
            Self::Checkpoint => "checkpoint",
        }
    }
}

/// Reconstructible customer/control-plane view. Durable roots and manifests
/// are deliberately absent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CustomerCatalogRecord {
    pub volume_id: Uuid,
    pub resource_id: Uuid,
    pub kind: CustomerResourceKind,
    pub name: String,
    pub state: String,
    pub parent_id: Option<Uuid>,
    pub origin_checkpoint_id: Option<Uuid>,
    pub observed_generation: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub customer_metadata: CustomerMetadata,
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("branch {0} still has an active writer lease")]
    WriterLeaseActive(Uuid),
    #[error("branch operation conflicts with its immutable request or phase: {0}")]
    OperationConflict(String),
    #[error("catalog record revision conflict: expected {expected}, found {actual}")]
    RevisionConflict { expected: u64, actual: u64 },
    #[error("catalog record already exists: {0}")]
    AlreadyExists(String),
    #[error("catalog record not found: {0}")]
    NotFound(String),
    #[error("invalid catalog record: {0}")]
    Invalid(String),
    #[error("corrupt catalog: {0}")]
    Corrupt(String),
    #[error("catalog I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("catalog JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("SlateDB catalog failed: {0}")]
    SlateDb(#[from] slatedb::Error),
    #[error("PostgreSQL catalog projection failed: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("PostgreSQL TLS configuration failed: {0}")]
    PostgresTls(String),
}

#[async_trait]
#[allow(dead_code)] // Read methods are consumed incrementally as lifecycle APIs land.
pub(crate) trait Catalog: Send + Sync {
    async fn snapshot(&self) -> Result<CatalogSnapshot, CatalogError>;
    async fn branch(&self, id: Uuid) -> Result<Option<BranchRecord>, CatalogError>;
    async fn branch_by_name(&self, name: &str) -> Result<Option<BranchRecord>, CatalogError>;
    async fn checkpoint(&self, id: Uuid) -> Result<Option<CheckpointRecord>, CatalogError>;
    async fn checkpoint_by_name(
        &self,
        branch_id: Uuid,
        name: &str,
    ) -> Result<Option<CheckpointRecord>, CatalogError>;
    async fn branch_create_operation(
        &self,
        id: Uuid,
    ) -> Result<Option<BranchCreateOperation>, CatalogError>;
    async fn branch_delete_operation(
        &self,
        id: Uuid,
    ) -> Result<Option<BranchDeleteOperation>, CatalogError>;
    async fn lease(&self, id: Uuid) -> Result<Option<LeaseRecord>, CatalogError>;
    async fn tombstone(&self, id: Uuid) -> Result<Option<TombstoneRecord>, CatalogError>;

    /// Apply one atomic mutation and advance the root-snapshot generation.
    /// Updates/deletes carry per-record revisions, so unrelated mutations do
    /// not invalidate one another merely because the global generation moved.
    async fn apply(&self, mutation: CatalogMutation) -> Result<u64, CatalogError>;
}

/// Optional customer-facing index. Failures here never invalidate an already
/// committed authoritative catalog mutation; reconciliation is repeatable.
#[async_trait]
pub trait CatalogProjection: Send + Sync {
    async fn reconcile(
        &self,
        volume_id: Uuid,
        snapshot: &CatalogSnapshot,
    ) -> Result<(), CatalogError>;
    async fn record(
        &self,
        volume_id: Uuid,
        resource_id: Uuid,
    ) -> Result<Option<CustomerCatalogRecord>, CatalogError>;
    async fn set_customer_metadata(
        &self,
        volume_id: Uuid,
        resource_id: Uuid,
        metadata: CustomerMetadata,
    ) -> Result<(), CatalogError>;
}

fn validate_records<T>(
    records: &BTreeMap<Uuid, T>,
    id: impl Fn(&T) -> Uuid,
    validate: impl Fn(&T) -> Result<(), CatalogError>,
) -> Result<(), CatalogError> {
    for (key, record) in records {
        if *key != id(record) {
            return Err(CatalogError::Corrupt(format!(
                "catalog key {key} does not match record {}",
                id(record)
            )));
        }
        validate(record)?;
    }
    Ok(())
}

fn validate_branch_create_relationships(snapshot: &CatalogSnapshot) -> Result<(), CatalogError> {
    let mut destinations = std::collections::BTreeSet::new();
    for operation in snapshot.branch_create_operations.values() {
        if !destinations.insert(operation.destination_id) {
            return Err(CatalogError::Corrupt(format!(
                "multiple create operations target branch {}",
                operation.destination_id
            )));
        }
        match operation.phase {
            BranchCreatePhase::Reserved | BranchCreatePhase::RootCreated => {
                let branch = snapshot
                    .branches
                    .get(&operation.destination_id)
                    .ok_or_else(|| {
                        CatalogError::Corrupt(format!(
                            "incomplete operation {} has no destination branch",
                            operation.id
                        ))
                    })?;
                if branch.state != BranchState::Creating
                    || branch.name != operation.destination_name
                    || branch.parent_id != operation.parent_id
                    || branch.origin_checkpoint_id != Some(operation.source_checkpoint_id)
                    || branch.root.is_some()
                {
                    return Err(CatalogError::Corrupt(format!(
                        "incomplete operation {} disagrees with its destination branch",
                        operation.id
                    )));
                }
                if operation.phase == BranchCreatePhase::Reserved {
                    let source = snapshot
                        .checkpoints
                        .get(&operation.source_checkpoint_id)
                        .ok_or_else(|| {
                            CatalogError::Corrupt(format!(
                                "reserved operation {} has no live source checkpoint",
                                operation.id
                            ))
                        })?;
                    if source.root != operation.source_root {
                        return Err(CatalogError::Corrupt(format!(
                            "reserved operation {} source root changed",
                            operation.id
                        )));
                    }
                } else {
                    let live_source_matches = snapshot
                        .checkpoints
                        .get(&operation.source_checkpoint_id)
                        .is_some_and(|source| source.root == operation.source_root);
                    let deleted_source_matches = snapshot
                        .tombstones
                        .get(&operation.source_checkpoint_id)
                        .is_some_and(|tombstone| tombstone.kind == TombstoneKind::Checkpoint);
                    if !live_source_matches && !deleted_source_matches {
                        return Err(CatalogError::Corrupt(format!(
                            "root-created operation {} has no matching source history",
                            operation.id
                        )));
                    }
                }
            }
            BranchCreatePhase::Published => {
                if let Some(branch) = snapshot.branches.get(&operation.destination_id) {
                    if branch.state != BranchState::Ready
                        || branch.parent_id != operation.parent_id
                        || branch.origin_checkpoint_id != Some(operation.source_checkpoint_id)
                    {
                        return Err(CatalogError::Corrupt(format!(
                            "published operation {} disagrees with its destination branch",
                            operation.id
                        )));
                    }
                } else if !snapshot
                    .tombstones
                    .get(&operation.destination_id)
                    .is_some_and(|tombstone| tombstone.kind == TombstoneKind::Branch)
                {
                    return Err(CatalogError::Corrupt(format!(
                        "published operation {} has no branch or tombstone",
                        operation.id
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_branch_delete_relationships(snapshot: &CatalogSnapshot) -> Result<(), CatalogError> {
    let mut subjects = std::collections::BTreeSet::new();
    for operation in snapshot.branch_delete_operations.values() {
        if !subjects.insert(operation.branch_id) {
            return Err(CatalogError::Corrupt(format!(
                "multiple delete operations target branch {}",
                operation.branch_id
            )));
        }
        match operation.phase {
            BranchDeletePhase::Draining => {
                let deleting_revision = operation
                    .expected_branch_revision
                    .checked_add(1)
                    .ok_or_else(|| {
                        CatalogError::Corrupt("deleted branch revision overflow".to_string())
                    })?;
                let branch = snapshot.branches.get(&operation.branch_id).ok_or_else(|| {
                    CatalogError::Corrupt(format!(
                        "draining delete operation {} has no branch",
                        operation.id
                    ))
                })?;
                if branch.state != BranchState::Deleting
                    || branch.name != operation.branch_name
                    || branch.root.as_ref() != Some(&operation.root)
                    || branch.parent_id != operation.parent_id
                    || branch.origin_checkpoint_id != operation.origin_checkpoint_id
                    || branch.revision != deleting_revision
                {
                    return Err(CatalogError::Corrupt(format!(
                        "draining delete operation {} disagrees with its branch",
                        operation.id
                    )));
                }
            }
            BranchDeletePhase::Published => {
                let tombstone = snapshot
                    .tombstones
                    .get(&operation.branch_id)
                    .ok_or_else(|| {
                        CatalogError::Corrupt(format!(
                            "published delete operation {} has no tombstone",
                            operation.id
                        ))
                    })?;
                if tombstone.kind != TombstoneKind::Branch
                    || tombstone.name != operation.branch_name
                    || tombstone.parent_id != operation.parent_id
                    || tombstone.origin_checkpoint_id != operation.origin_checkpoint_id
                    || tombstone.deleted_revision != Some(operation.expected_branch_revision)
                    || tombstone.deletion_operation_id != Some(operation.id)
                    || tombstone.deleted_at != operation.updated_at
                    || snapshot.branches.contains_key(&operation.branch_id)
                {
                    return Err(CatalogError::Corrupt(format!(
                        "published delete operation {} disagrees with its tombstone",
                        operation.id
                    )));
                }
            }
        }
    }
    for branch in snapshot
        .branches
        .values()
        .filter(|branch| branch.state == BranchState::Deleting)
    {
        if !snapshot.branch_delete_operations.values().any(|operation| {
            operation.branch_id == branch.id && operation.phase == BranchDeletePhase::Draining
        }) {
            return Err(CatalogError::Corrupt(format!(
                "deleting branch {} has no draining delete operation",
                branch.id
            )));
        }
    }
    Ok(())
}

fn validate_id(id: Uuid, label: &str) -> Result<(), CatalogError> {
    if id.is_nil() {
        return Err(CatalogError::Invalid(format!("{label} UUID cannot be nil")));
    }
    Ok(())
}

fn validate_optional_id(id: Option<Uuid>, label: &str) -> Result<(), CatalogError> {
    if id.is_some_and(|id| id.is_nil()) {
        return Err(CatalogError::Invalid(format!("{label} UUID cannot be nil")));
    }
    Ok(())
}

fn validate_revision(revision: u64, label: &str) -> Result<(), CatalogError> {
    if revision == 0 {
        return Err(CatalogError::Invalid(format!(
            "{label} revision must start at one"
        )));
    }
    Ok(())
}

fn validate_root(root: &DurableRoot) -> Result<(), CatalogError> {
    for (label, value) in [
        ("durable root identity", root.identity.as_str()),
        ("durable manifest identity", root.manifest_id.as_str()),
    ] {
        if value.is_empty() {
            return Err(CatalogError::Invalid(format!("{label} cannot be empty")));
        }
        if value.len() > MAX_ROOT_IDENTIFIER_BYTES {
            return Err(CatalogError::Invalid(format!(
                "{label} cannot exceed {MAX_ROOT_IDENTIFIER_BYTES} bytes"
            )));
        }
        if value.chars().any(char::is_control) {
            return Err(CatalogError::Invalid(format!(
                "{label} cannot contain control characters"
            )));
        }
    }
    Ok(())
}

fn validate_unique_names<T: Ord>(
    names: impl Iterator<Item = T>,
    label: &str,
) -> Result<(), CatalogError> {
    let mut seen = std::collections::BTreeSet::new();
    for name in names {
        if !seen.insert(name) {
            return Err(CatalogError::Corrupt(format!(
                "duplicate {label} name in catalog"
            )));
        }
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<(), CatalogError> {
    if name.is_empty() || name.trim() != name {
        return Err(CatalogError::Invalid(
            "catalog names cannot be empty or have surrounding whitespace".to_string(),
        ));
    }
    if name.len() > MAX_CATALOG_NAME_BYTES {
        return Err(CatalogError::Invalid(format!(
            "catalog names cannot exceed {MAX_CATALOG_NAME_BYTES} bytes"
        )));
    }
    if name.chars().any(char::is_control) {
        return Err(CatalogError::Invalid(
            "catalog names cannot contain control characters".to_string(),
        ));
    }
    if name.as_bytes().starts_with(b"__zerofs_") {
        return Err(CatalogError::Invalid(
            "catalog names beginning with __zerofs_ are reserved".to_string(),
        ));
    }
    Ok(())
}

fn validate_metadata(metadata: &CustomerMetadata) -> Result<(), CatalogError> {
    let encoded = serde_json::to_vec(metadata)?;
    if encoded.len() > MAX_CUSTOMER_METADATA_BYTES {
        return Err(CatalogError::Invalid(format!(
            "customer metadata cannot exceed {MAX_CUSTOMER_METADATA_BYTES} encoded bytes"
        )));
    }
    Ok(())
}

fn validate_times(
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    label: &str,
) -> Result<(), CatalogError> {
    if updated_at < created_at {
        return Err(CatalogError::Invalid(format!(
            "{label} updated_at predates created_at"
        )));
    }
    validate_timestamp(created_at, &format!("{label} created_at"))?;
    validate_timestamp(updated_at, &format!("{label} updated_at"))
}

fn validate_timestamp(value: DateTime<Utc>, field: &str) -> Result<(), CatalogError> {
    if !value.timestamp_subsec_nanos().is_multiple_of(1_000) {
        return Err(CatalogError::Invalid(format!(
            "{field} must use microsecond precision; call catalog_timestamp first"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unbounded_roots_and_nil_lineage() {
        let now = catalog_timestamp(Utc::now());
        let oversized = BranchRecord {
            id: Uuid::new_v4(),
            revision: 1,
            name: "oversized".to_string(),
            state: BranchState::Ready,
            root: Some(DurableRoot {
                identity: "x".repeat(MAX_ROOT_IDENTIFIER_BYTES + 1),
                manifest_id: "manifest".to_string(),
            }),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        };
        assert!(matches!(
            oversized.validate(),
            Err(CatalogError::Invalid(_))
        ));

        let nil_lineage = BranchRecord {
            root: Some(DurableRoot {
                identity: "root".to_string(),
                manifest_id: "manifest".to_string(),
            }),
            parent_id: Some(Uuid::nil()),
            ..oversized
        };
        assert!(matches!(
            nil_lineage.validate(),
            Err(CatalogError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_reserved_internal_names() {
        assert!(matches!(
            validate_name("__zerofs_branch_create_private"),
            Err(CatalogError::Invalid(message)) if message.contains("reserved")
        ));
        validate_name("__ZeroFS_customer_name").unwrap();
    }

    #[test]
    fn snapshot_rejects_cross_kind_resource_id_collision() {
        let id = Uuid::new_v4();
        let branch_id = Uuid::new_v4();
        let now = catalog_timestamp(Utc::now());
        let mut snapshot = CatalogSnapshot::default();
        snapshot.branches.insert(
            id,
            BranchRecord {
                id,
                revision: 1,
                name: "branch".to_string(),
                state: BranchState::Ready,
                root: Some(DurableRoot {
                    identity: "branch-root".to_string(),
                    manifest_id: "branch-manifest".to_string(),
                }),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at: now,
                updated_at: now,
            },
        );
        snapshot.checkpoints.insert(
            id,
            CheckpointRecord {
                id,
                revision: 1,
                branch_id,
                name: "checkpoint".to_string(),
                root: DurableRoot {
                    identity: "checkpoint-root".to_string(),
                    manifest_id: "checkpoint-manifest".to_string(),
                },
                created_at: now,
                updated_at: now,
            },
        );
        assert!(matches!(snapshot.validate(), Err(CatalogError::Corrupt(_))));
    }

    #[test]
    fn snapshot_rejects_invalid_tombstone_time_and_generation() {
        let id = Uuid::new_v4();
        let created_at = catalog_timestamp(Utc::now());
        let mut snapshot = CatalogSnapshot {
            generation: 1,
            ..CatalogSnapshot::default()
        };
        snapshot.tombstones.insert(
            id,
            TombstoneRecord {
                id,
                kind: TombstoneKind::Branch,
                name: "old".to_string(),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at,
                deleted_revision: Some(1),
                deletion_operation_id: None,
                deleted_generation: 2,
                deleted_at: created_at - chrono::Duration::microseconds(1),
            },
        );
        assert!(snapshot.validate().is_err());

        snapshot.tombstones.get_mut(&id).unwrap().deleted_at = created_at;
        assert!(matches!(snapshot.validate(), Err(CatalogError::Corrupt(_))));
    }
}
