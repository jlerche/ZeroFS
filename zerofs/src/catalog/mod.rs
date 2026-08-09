//! Durable metadata catalog for copy-on-write branches.
//!
//! Storage-critical state has one authority: SlateDB. PostgreSQL is an
//! optional, reconstructible customer-facing projection and is never consulted
//! to mount a branch or decide storage liveness. JSON and PostgreSQL implement
//! the same projection contract.

mod deletion;
mod gc;
mod gc_inventory;
mod gc_mark;
mod json;
mod lease;
mod lifecycle;
mod postgres;
mod private_epoch;
mod root_store;
#[path = "slatedb.rs"]
mod slate;
#[cfg(test)]
mod stress;

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
pub use gc::{
    GcDeletionControl, GcDeletionPolicy, RootCaptureLifecycle, RootCaptureLifecycleError,
};
pub use json::JsonCatalogProjection;
pub use lease::{
    LeaseAcquireRequest, LeaseGrant, LeaseLifecycle, LeaseLifecycleError, LeaseRenewRequest,
};
pub use lifecycle::{
    AdministrativeInspectionKind, AdministrativeInspectionPage, AdministrativeInspectionRecord,
    AdministrativeInspectionRequest, AdministrativeLeaseRecord,
    BranchCreateFromCheckpointNameRequest, BranchCreateRequest, BranchFeatureConfig,
    BranchInspection, BranchLifecycle, BranchLifecycleError, BranchLineageInspection,
    BranchMountGrant, BranchMountRequest, CheckpointCreateRequest, HistoricalResource,
    HistoricalResourceStatus, InitialBranchCreateRequest, MAX_ADMINISTRATIVE_INSPECTION_RECORDS,
    ServerWriterMountDisposition, ServerWriterMountPreparation, ServerWriterMountRequest,
};
pub use postgres::PostgresCatalogProjection;
pub use private_epoch::{
    PrivateEpochLifecycle, PrivateEpochLifecycleError, PrivateEpochRegisterRequest,
    PrivateEpochSealRequest, PrivateGcGuardRequest, PrivateGcPolicy,
};
pub use root_store::{ImmutableCheckpoint, RootStoreError, SlateDbRootStore};
pub(crate) use slate::SlateDbCatalog;

pub const CATALOG_SCHEMA_VERSION: u32 = 20;
pub const CATALOG_PROJECTION_SCHEMA_VERSION: u32 = 1;
pub const MAX_CATALOG_NAME_BYTES: usize = 255;
pub const MAX_ROOT_IDENTIFIER_BYTES: usize = 4 * 1024;
pub const MAX_CUSTOMER_METADATA_BYTES: usize = 64 * 1024;
/// Maximum live catalog branch records, including creating and deleting
/// incarnations. Tombstones and permanently retired UUID reservations do not
/// consume this admission budget.
pub const MAX_LIVE_BRANCHES: usize = 4_096;
/// Maximum named checkpoints retained by one branch.
pub const MAX_CHECKPOINTS_PER_BRANCH: usize = 256;
/// Maximum active branch and checkpoint leases attributed to one branch.
pub const MAX_ACTIVE_LEASES_PER_BRANCH: usize = 64;
/// Maximum private allocation epochs awaiting publication for one branch.
pub const MAX_UNEXPOSED_PRIVATE_EPOCHS_PER_BRANCH: usize = 64;
/// Maximum retained historical parent edges admitted for a new branch.
pub const MAX_BRANCH_LINEAGE_DEPTH: usize = 64;

pub type CustomerMetadata = Map<String, Value>;

/// Validate a public catalog resource name before performing external storage
/// I/O. Internal `__zerofs_` namespaces are reserved and never customer-visible.
pub fn validate_resource_name(name: &str) -> Result<(), CatalogError> {
    validate_name(name)
}

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
    /// Default-off release controls for customer lifecycle mutations and mounts.
    #[serde(default)]
    pub features: BranchFeatureConfig,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            slatedb_path: ".zerofs/catalog".to_string(),
            features: BranchFeatureConfig::default(),
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
        segment_pool_path: slatedb::object_store::path::Path,
    ) -> Result<BranchLifecycle, BranchLifecycleError> {
        self.open_branch_lifecycle_with_wal(
            object_store,
            None,
            branch_database_root,
            segment_pool_path,
        )
        .await
    }

    pub async fn open_branch_lifecycle_with_wal(
        &self,
        object_store: Arc<dyn ObjectStore>,
        wal_object_store: Option<Arc<dyn ObjectStore>>,
        branch_database_root: slatedb::object_store::path::Path,
        segment_pool_path: slatedb::object_store::path::Path,
    ) -> Result<BranchLifecycle, BranchLifecycleError> {
        let catalog_path = slatedb::object_store::path::Path::from(self.slatedb_path.as_str());
        root_store::ensure_database_namespaces_disjoint(
            "catalog",
            &catalog_path,
            "branch root",
            &branch_database_root,
        )?;
        root_store::ensure_database_namespaces_disjoint(
            "catalog",
            &catalog_path,
            "segment pool",
            &segment_pool_path,
        )?;
        root_store::ensure_database_namespaces_disjoint(
            "branch root",
            &branch_database_root,
            "segment pool",
            &segment_pool_path,
        )?;
        let catalog: Arc<dyn Catalog> =
            Arc::new(SlateDbCatalog::open(catalog_path, Arc::clone(&object_store)).await?);
        let mut roots = SlateDbRootStore::new(object_store, branch_database_root)
            .with_segment_pool_root(segment_pool_path);
        if let Some(wal_object_store) = wal_object_store {
            roots = roots.with_wal_object_store(wal_object_store);
        }
        Ok(BranchLifecycle::new_with_features(
            catalog,
            roots,
            self.features,
        ))
    }
}

/// Customer projection selection. JSON is the local/testing default;
/// PostgreSQL is selected explicitly in production.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
pub enum CatalogProjectionConfig {
    Json {
        #[serde(deserialize_with = "deserialize_projection_path")]
        path: PathBuf,
    },
    Postgres {
        #[serde(deserialize_with = "deserialize_projection_string")]
        connection_string: String,
        /// Require certificate-authenticated TLS. Disable only for an isolated
        /// local test database; secure transport remains the default.
        #[serde(default = "default_postgres_tls")]
        tls: bool,
    },
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
            Self::Postgres {
                connection_string,
                tls,
            } => {
                let projection =
                    PostgresCatalogProjection::connect_with_tls(connection_string, *tls).await?;
                projection.migrate().await?;
                Ok(Arc::new(projection))
            }
        }
    }
}

const fn default_postgres_tls() -> bool {
    true
}

fn deserialize_projection_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    shellexpand::env(&value)
        .map(|expanded| expanded.into_owned())
        .map_err(serde::de::Error::custom)
}

fn deserialize_projection_path<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = deserialize_projection_string(deserializer)?;
    Ok(PathBuf::from(shellexpand::tilde(&value).into_owned()))
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
    #[serde(default)]
    pub writer_head: Option<WriterHeadPublication>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriterHeadPublication {
    pub branch_id: Uuid,
    pub consumed_branch_revision: u64,
    pub consumed_lease_revision: u64,
    pub previous_root: DurableRoot,
    pub root: DurableRoot,
    pub published_generation: u64,
    pub published_at: DateTime<Utc>,
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
        if let Some(publication) = &self.writer_head {
            publication.validate()?;
            if publication.published_at != self.ended_at {
                return Err(CatalogError::Invalid(
                    "writer head publication and lease tombstone times must match".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl WriterHeadPublication {
    fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.branch_id, "writer head branch")?;
        validate_revision(self.consumed_branch_revision, "writer head branch")?;
        validate_revision(self.consumed_lease_revision, "writer head lease")?;
        validate_root(&self.previous_root)?;
        validate_root(&self.root)?;
        validate_timestamp(self.published_at, "writer head published_at")?;
        if self.previous_root == self.root
            || self.previous_root.identity != self.root.identity
            || self.published_generation == 0
        {
            return Err(CatalogError::Invalid(
                "writer head must advance one database identity at a nonzero generation"
                    .to_string(),
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
        validate_optional_id(self.parent_id, "branch create source branch")?;
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

/// Permanent, root-free reservation left after bulky historical metadata is
/// compacted. Catalog UUIDs are never reusable, even after customer-visible
/// tombstone retention expires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[doc(hidden)]
pub enum RetiredCatalogKind {
    Branch,
    Checkpoint,
    BranchCreateOperation,
    BranchDeleteOperation,
    GcRun,
    Lease,
    LocalGcRun,
}

impl From<TombstoneKind> for RetiredCatalogKind {
    fn from(value: TombstoneKind) -> Self {
        match value {
            TombstoneKind::Branch => Self::Branch,
            TombstoneKind::Checkpoint => Self::Checkpoint,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[doc(hidden)]
pub struct RetiredCatalogId {
    pub id: Uuid,
    pub kind: RetiredCatalogKind,
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

pub const MAX_TOMBSTONE_CLEANUP_SCAN: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TombstoneCleanupPolicy {
    /// Tombstones deleted at or before this caller-selected retention cutoff
    /// may be compacted if every authoritative dependency is also clear.
    pub retain_after: DateTime<Utc>,
    pub scan_limit: usize,
    pub compact_limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TombstoneCleanupReport {
    pub examined: u64,
    pub compacted: u64,
    pub retained_by_age: u64,
    pub retained_by_roots: u64,
    pub retained_by_dependency: u64,
    /// Eligible records observed after the mutation budget was exhausted.
    /// This is a bounded lower-bound backlog signal, not a catalog-wide scan.
    pub eligible_backlog_lower_bound: u64,
    pub cursor_wrapped: bool,
}

/// Publish one bounded metadata-cleanup pass. The backlog gauge is deliberately
/// a conservative, kind-specific signal: tombstones report an observed eligible
/// lower bound, while artifact cleanup reports `0` or `1` for no-more/more work.
pub(crate) fn record_cleanup_metrics(
    kind: &'static str,
    examined: u64,
    removed: u64,
    already_absent: u64,
    retained: u64,
    backlog_signal: u64,
) {
    metrics::counter!("zerofs_catalog_cleanup_passes_total", "kind" => kind).increment(1);
    metrics::counter!("zerofs_catalog_cleanup_examined_total", "kind" => kind).increment(examined);
    metrics::counter!("zerofs_catalog_cleanup_removed_total", "kind" => kind).increment(removed);
    metrics::counter!("zerofs_catalog_cleanup_already_absent_total", "kind" => kind)
        .increment(already_absent);
    metrics::counter!("zerofs_catalog_cleanup_retained_total", "kind" => kind).increment(retained);
    metrics::gauge!("zerofs_catalog_cleanup_backlog_signal", "kind" => kind)
        .set(backlog_signal as f64);
}

/// Authoritative local-GC eligibility state for one authenticated pool epoch.
/// Merely having this record never authorizes deletion; storage authentication,
/// sealing, exclusion guards, and local liveness proof are separate gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivateEpochState {
    Open,
    SealedPrivate,
    Exposed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateEpochRecord {
    pub epoch: u64,
    pub revision: u64,
    pub pool_id: Uuid,
    pub reservation_id: Uuid,
    pub branch_id: Uuid,
    pub database_identity: String,
    pub state: PrivateEpochState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub sealed_at: Option<DateTime<Utc>>,
    pub exposed_at: Option<DateTime<Utc>>,
}

impl PrivateEpochRecord {
    pub fn validate(&self) -> Result<(), CatalogError> {
        if self.epoch == 0 {
            return Err(CatalogError::Invalid(
                "private segment epoch must be nonzero".to_string(),
            ));
        }
        validate_revision(self.revision, "private segment epoch")?;
        validate_id(self.pool_id, "private epoch pool")?;
        validate_id(self.reservation_id, "private epoch reservation")?;
        validate_id(self.branch_id, "private epoch branch")?;
        if self.database_identity.is_empty()
            || self.database_identity.len() > MAX_ROOT_IDENTIFIER_BYTES
        {
            return Err(CatalogError::Invalid(
                "private epoch database identity must be nonempty and bounded".to_string(),
            ));
        }
        validate_times(self.created_at, self.updated_at, "private segment epoch")?;
        match self.state {
            PrivateEpochState::Open if self.sealed_at.is_none() && self.exposed_at.is_none() => {}
            PrivateEpochState::SealedPrivate
                if self.sealed_at == Some(self.updated_at) && self.exposed_at.is_none() => {}
            PrivateEpochState::Exposed
                if self.exposed_at == Some(self.updated_at)
                    && self
                        .sealed_at
                        .is_none_or(|sealed| sealed <= self.updated_at) => {}
            _ => {
                return Err(CatalogError::Invalid(
                    "private epoch timestamps disagree with lifecycle state".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// Durable, non-expiring exclusion for one bounded local-GC batch. There is no
/// generic release mutation: a later deletion slice must durably publish every
/// candidate outcome before it can retire this guard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalGcGuardRecord {
    pub id: Uuid,
    pub revision: u64,
    pub branch_id: Uuid,
    pub epoch: u64,
    pub epoch_revision: u64,
    pub candidate_count: u32,
    pub candidate_digest: String,
    pub created_at: DateTime<Utc>,
}

impl LocalGcGuardRecord {
    pub fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.id, "local GC guard")?;
        validate_revision(self.revision, "local GC guard")?;
        validate_id(self.branch_id, "local GC guard branch")?;
        if self.epoch == 0 || self.epoch_revision == 0 {
            return Err(CatalogError::Invalid(
                "local GC guard epoch identity must be nonzero".to_string(),
            ));
        }
        if self.candidate_count == 0
            || self.candidate_count as usize > crate::fs::MAX_LOCAL_GC_CANDIDATES
        {
            return Err(CatalogError::Invalid(
                "local GC guard candidate count is outside the safety bound".to_string(),
            ));
        }
        if self.candidate_digest.len() != 64
            || !self
                .candidate_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(CatalogError::Invalid(
                "local GC guard candidate digest must be 64 hexadecimal characters".to_string(),
            ));
        }
        validate_timestamp(self.created_at, "local GC guard created_at")
    }
}

/// Durable bounded cursor and aggregate audit for one local-GC guard. Active
/// progress keeps the matching guard present; the completed record is written
/// in the same catalog mutation that retires that guard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalGcProgressRecord {
    pub id: Uuid,
    pub revision: u64,
    pub branch_id: Uuid,
    pub epoch: u64,
    pub epoch_revision: u64,
    pub candidate_count: u32,
    pub candidate_digest: String,
    pub next_candidate: u32,
    pub deleted_objects: u32,
    pub deleted_bytes: u64,
    pub already_absent: u32,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl LocalGcProgressRecord {
    pub fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.id, "local GC progress")?;
        validate_revision(self.revision, "local GC progress")?;
        validate_id(self.branch_id, "local GC progress branch")?;
        if self.epoch == 0 || self.epoch_revision == 0 {
            return Err(CatalogError::Invalid(
                "local GC progress epoch identity must be nonzero".to_string(),
            ));
        }
        if self.candidate_count == 0
            || self.candidate_count as usize > crate::fs::MAX_LOCAL_GC_CANDIDATES
        {
            return Err(CatalogError::Invalid(
                "local GC progress candidate count is outside the safety bound".to_string(),
            ));
        }
        if self.candidate_digest.len() != 64
            || !self
                .candidate_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(CatalogError::Invalid(
                "local GC progress candidate digest must be 64 hexadecimal characters".to_string(),
            ));
        }
        let classified = self
            .deleted_objects
            .checked_add(self.already_absent)
            .ok_or_else(|| CatalogError::Invalid("local GC progress count overflow".to_string()))?;
        if self.next_candidate > self.candidate_count || classified != self.next_candidate {
            return Err(CatalogError::Invalid(
                "local GC progress cursor disagrees with aggregate outcomes".to_string(),
            ));
        }
        validate_times(self.started_at, self.updated_at, "local GC progress")?;
        match self.completed_at {
            Some(completed_at)
                if completed_at == self.updated_at
                    && self.next_candidate == self.candidate_count => {}
            None if self.next_candidate < self.candidate_count => {}
            _ => {
                return Err(CatalogError::Invalid(
                    "local GC completion disagrees with its bounded cursor".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn matches_guard(&self, guard: &LocalGcGuardRecord) -> bool {
        self.id == guard.id
            && self.branch_id == guard.branch_id
            && self.epoch == guard.epoch
            && self.epoch_revision == guard.epoch_revision
            && self.candidate_count == guard.candidate_count
            && self.candidate_digest == guard.candidate_digest
            && self.started_at >= guard.created_at
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PrivateGcOwnerView {
    pub active_guard: Option<LocalGcGuardRecord>,
    pub sealed_epochs: Vec<PrivateEpochRecord>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PrivateGcGuardView {
    pub guard: Option<LocalGcGuardRecord>,
    pub progress: Option<LocalGcProgressRecord>,
    pub guarded_epoch: Option<PrivateEpochRecord>,
    pub writer_epoch: Option<PrivateEpochRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GcRootKind {
    Branch,
    Checkpoint,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GcRootPin {
    pub kind: GcRootKind,
    pub root: DurableRoot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GcRunPhase {
    Captured,
    Marking,
    /// Terminal shadow-mode inventory report. No candidate was quarantined or
    /// deleted, and the run no longer retains its captured roots.
    Reported,
    Quarantined,
    Revalidating,
    Validated,
    Deleting,
    Completed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GcRunRecord {
    pub id: Uuid,
    pub revision: u64,
    pub catalog_generation: u64,
    pub inventory_cutoff: DateTime<Utc>,
    pub roots: Vec<GcRootPin>,
    pub root_digest: String,
    /// Immutable volume-wide physical segment pool scanned by this run.
    /// Empty is accepted only so pre-v9 active runs fail closed after migration.
    #[serde(default)]
    pub segment_pool: String,
    #[serde(default)]
    pub mark_shards: Vec<GcMarkShard>,
    #[serde(default)]
    pub mark_stats: Option<GcMarkStats>,
    #[serde(default)]
    pub quarantine_shards: Vec<GcQuarantineShard>,
    #[serde(default)]
    pub inventory_stats: Option<GcInventoryStats>,
    pub phase: GcRunPhase,
    pub quarantine_at: Option<DateTime<Utc>>,
    /// Durable fresh-generation observation used to prove that first-pass
    /// candidates remained unreachable after the configured grace period.
    #[serde(default)]
    pub revalidation: Option<GcRevalidationRecord>,
    #[serde(default)]
    pub deletion: Option<GcDeletionProgress>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GcMarkShard {
    pub shard: u8,
    pub location: String,
    pub checksum: String,
    pub segment_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcMarkStats {
    pub roots_enumerated: u64,
    pub references_enumerated: u64,
    pub intermediate_runs: u64,
    pub unique_segments: u64,
    /// Derived worst-case writes of one record through initial, carry,
    /// finalization, and authoritative-output stages for this observation.
    #[serde(default)]
    pub max_write_passes: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GcQuarantineShard {
    pub shard: u8,
    pub location: String,
    pub checksum: String,
    pub candidate_count: u64,
    pub candidate_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcInventoryStats {
    pub objects_seen: u64,
    pub objects_newer_than_cutoff: u64,
    pub reachable_objects: u64,
    pub candidate_objects: u64,
    pub candidate_bytes: u64,
    pub intermediate_runs: u64,
    /// Maximum derived record-write passes among inventory shards.
    #[serde(default)]
    pub max_write_passes: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GcRevalidationRecord {
    pub id: Uuid,
    pub catalog_generation: u64,
    pub grace_seconds: u64,
    pub not_before: DateTime<Utc>,
    pub inventory_cutoff: DateTime<Utc>,
    pub roots: Vec<GcRootPin>,
    pub root_digest: String,
    #[serde(default)]
    pub mark_shards: Vec<GcMarkShard>,
    #[serde(default)]
    pub mark_stats: Option<GcMarkStats>,
    #[serde(default)]
    pub candidate_shards: Vec<GcQuarantineShard>,
    #[serde(default)]
    pub stats: Option<GcRevalidationStats>,
    pub captured_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcRevalidationStats {
    pub first_observation_candidates: u64,
    pub became_reachable: u64,
    pub already_absent: u64,
    pub retained_candidates: u64,
    pub retained_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcDeletionProgress {
    pub batch_size: u32,
    /// 0..=256; 256 is the durable end-of-stream sentinel.
    pub next_shard: u16,
    pub next_record: u64,
    pub deleted_objects: u64,
    pub deleted_bytes: u64,
    pub already_absent: u64,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub(crate) struct GcDeletionPublication {
    pub(crate) run_id: Uuid,
    pub(crate) expected_revision: u64,
    pub(crate) expected_generation: u64,
    pub(crate) progress: GcDeletionProgress,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(crate) struct GcRevalidationCapture {
    pub(crate) run_id: Uuid,
    pub(crate) expected_revision: u64,
    pub(crate) expected_generation: u64,
    pub(crate) observation: GcRevalidationRecord,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(crate) struct GcRevalidationPublication {
    pub(crate) run_id: Uuid,
    pub(crate) expected_revision: u64,
    pub(crate) expected_generation: u64,
    pub(crate) observation_id: Uuid,
    pub(crate) root_digest: String,
    pub(crate) mark_shards: Vec<GcMarkShard>,
    pub(crate) mark_stats: GcMarkStats,
    pub(crate) candidate_shards: Vec<GcQuarantineShard>,
    pub(crate) stats: GcRevalidationStats,
    pub(crate) completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(crate) struct GcQuarantinePublication {
    pub(crate) id: Uuid,
    pub(crate) expected_revision: u64,
    pub(crate) expected_generation: u64,
    pub(crate) root_digest: String,
    pub(crate) quarantine_shards: Vec<GcQuarantineShard>,
    pub(crate) inventory_stats: GcInventoryStats,
    pub(crate) quarantine_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(crate) struct GcReportPublication {
    pub(crate) id: Uuid,
    pub(crate) expected_revision: u64,
    pub(crate) expected_generation: u64,
    pub(crate) root_digest: String,
    pub(crate) candidate_shards: Vec<GcQuarantineShard>,
    pub(crate) inventory_stats: GcInventoryStats,
    pub(crate) reported_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GcBlockerKind {
    MissingRoot,
    CorruptMetadata,
    GenerationChanged,
    LeaseUncertainty,
    StorageUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcBlockerRecord {
    pub run_id: Uuid,
    pub kind: GcBlockerKind,
    pub occurrences: u64,
    pub detail: String,
    pub first_observed_at: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
}

impl GcBlockerRecord {
    fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.run_id, "GC blocker run")?;
        if self.occurrences == 0
            || self.detail.is_empty()
            || self.detail.len() > MAX_ROOT_IDENTIFIER_BYTES
        {
            return Err(CatalogError::Invalid(
                "GC blocker count and bounded detail are required".to_string(),
            ));
        }
        validate_timestamp(self.first_observed_at, "GC blocker first observation")?;
        validate_timestamp(self.last_observed_at, "GC blocker last observation")?;
        if self.last_observed_at < self.first_observed_at {
            return Err(CatalogError::Invalid(
                "GC blocker observations cannot move backwards".to_string(),
            ));
        }
        Ok(())
    }
}

impl GcRunRecord {
    fn retains_roots(&self) -> bool {
        // Only a fully schema-valid terminal report or completed deletion
        // releases pins. A fabricated, partial, future, or corrupt record
        // retains.
        !matches!(self.phase, GcRunPhase::Reported | GcRunPhase::Completed)
            || self.validate().is_err()
    }

    pub fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.id, "GC run")?;
        validate_revision(self.revision, "GC run")?;
        match self.phase {
            GcRunPhase::Captured
                if self.revision == 1
                    && self.mark_shards.is_empty()
                    && self.mark_stats.is_none()
                    && self.quarantine_shards.is_empty()
                    && self.inventory_stats.is_none()
                    && self.quarantine_at.is_none()
                    && self.revalidation.is_none()
                    && self.deletion.is_none() => {}
            GcRunPhase::Marking
                if self.revision == 2
                    && self.mark_shards.len() == 256
                    && self.mark_stats.is_some()
                    && self.quarantine_shards.is_empty()
                    && self.inventory_stats.is_none()
                    && self.quarantine_at.is_none()
                    && self.revalidation.is_none()
                    && self.deletion.is_none() => {}
            GcRunPhase::Reported
                if self.revision == 3
                    && !self.segment_pool.is_empty()
                    && self.mark_shards.len() == 256
                    && self.mark_stats.is_some()
                    && self.quarantine_shards.len() == 256
                    && self.inventory_stats.is_some()
                    && self
                        .quarantine_at
                        .is_some_and(|reported_at| reported_at == self.updated_at)
                    && self.revalidation.is_none()
                    && self.deletion.is_none() => {}
            GcRunPhase::Quarantined
                if self.revision == 3
                    && !self.segment_pool.is_empty()
                    && self.mark_shards.len() == 256
                    && self.mark_stats.is_some()
                    && self.quarantine_shards.len() == 256
                    && self.inventory_stats.is_some()
                    && self.quarantine_at.is_some()
                    && self.revalidation.is_none()
                    && self.deletion.is_none() => {}
            GcRunPhase::Revalidating
                if self.revision == 4
                    && self.revalidation.as_ref().is_some_and(|observation| {
                        observation.mark_shards.is_empty()
                            && observation.mark_stats.is_none()
                            && observation.candidate_shards.is_empty()
                            && observation.stats.is_none()
                            && observation.completed_at.is_none()
                            && observation.captured_at == self.updated_at
                    })
                    && self.deletion.is_none() => {}
            GcRunPhase::Validated
                if self.revision == 5
                    && self.revalidation.as_ref().is_some_and(|observation| {
                        observation.mark_shards.len() == 256
                            && observation.mark_stats.is_some()
                            && observation.candidate_shards.len() == 256
                            && observation.stats.is_some()
                            && observation.completed_at == Some(self.updated_at)
                    })
                    && self.deletion.is_none() => {}
            GcRunPhase::Deleting
                if self.revision >= 6
                    && self.revalidation.as_ref().is_some_and(|observation| {
                        observation.mark_shards.len() == 256
                            && observation.mark_stats.is_some()
                            && observation.candidate_shards.len() == 256
                            && observation.stats.is_some()
                            && observation.completed_at.is_some()
                    })
                    && self
                        .deletion
                        .as_ref()
                        .is_some_and(|progress| progress.completed_at.is_none()) => {}
            GcRunPhase::Completed
                if self.revision >= 7
                    && self.revalidation.as_ref().is_some_and(|observation| {
                        observation.mark_shards.len() == 256
                            && observation.mark_stats.is_some()
                            && observation.candidate_shards.len() == 256
                            && observation.stats.is_some()
                            && observation.completed_at.is_some()
                    })
                    && self
                        .deletion
                        .as_ref()
                        .is_some_and(|progress| progress.completed_at == Some(self.updated_at)) => {
            }
            _ => {
                return Err(CatalogError::Invalid(
                    "GC schema v17 supports captured through reported/completed revisions"
                        .to_string(),
                ));
            }
        }
        validate_timestamp(self.inventory_cutoff, "GC inventory cutoff")?;
        validate_timestamp(self.created_at, "GC run created_at")?;
        validate_timestamp(self.updated_at, "GC run updated_at")?;
        if self.updated_at < self.created_at || self.inventory_cutoff < self.created_at {
            return Err(CatalogError::Invalid(
                "GC run times cannot move backwards".to_string(),
            ));
        }
        if self.root_digest.len() != 64
            || !self
                .root_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(CatalogError::Invalid(
                "GC root digest must be 64 hexadecimal bytes".to_string(),
            ));
        }
        if self.segment_pool.len() > MAX_ROOT_IDENTIFIER_BYTES {
            return Err(CatalogError::Invalid(
                "GC segment-pool identity exceeds the storage-identity bound".to_string(),
            ));
        }
        if !self.segment_pool.is_empty() {
            let parsed =
                slatedb::object_store::path::Path::parse(&self.segment_pool).map_err(|error| {
                    CatalogError::Invalid(format!("invalid GC segment pool: {error}"))
                })?;
            if parsed.to_string() != self.segment_pool {
                return Err(CatalogError::Invalid(
                    "GC segment-pool identity must be canonical".to_string(),
                ));
            }
        }
        for pin in &self.roots {
            validate_root(&pin.root)?;
        }
        let mut canonical_roots = self.roots.clone();
        canonicalize_gc_root_pins(&mut canonical_roots);
        if canonical_roots != self.roots {
            return Err(CatalogError::Invalid(
                "GC roots must be sorted and deduplicated".to_string(),
            ));
        }
        if gc_root_digest(&self.roots)? != self.root_digest {
            return Err(CatalogError::Invalid(
                "GC root digest does not match its immutable root list".to_string(),
            ));
        }
        for (expected_shard, shard) in (0u8..=u8::MAX).zip(&self.mark_shards) {
            if shard.shard != expected_shard
                || shard.location.is_empty()
                || shard.location.len() > MAX_ROOT_IDENTIFIER_BYTES
                || shard.checksum.len() != 64
                || !shard.checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(CatalogError::Invalid(
                    "GC mark shards must be complete, ordered, bounded, and checksummed"
                        .to_string(),
                ));
            }
        }
        for (expected_shard, shard) in (0u8..=u8::MAX).zip(&self.quarantine_shards) {
            if shard.shard != expected_shard
                || shard.location.is_empty()
                || shard.location.len() > MAX_ROOT_IDENTIFIER_BYTES
                || shard.checksum.len() != 64
                || !shard.checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(CatalogError::Invalid(
                    "GC quarantine shards must be complete, ordered, bounded, and checksummed"
                        .to_string(),
                ));
            }
        }
        if let Some(stats) = &self.mark_stats {
            let unique_segments = self.mark_shards.iter().try_fold(0u64, |total, shard| {
                total.checked_add(shard.segment_count).ok_or_else(|| {
                    CatalogError::Invalid("GC unique-segment count overflow".to_string())
                })
            })?;
            if stats.roots_enumerated != self.roots.len() as u64
                || stats.unique_segments != unique_segments
                || stats.unique_segments > stats.references_enumerated
                || !valid_gc_write_pass_bound(stats.intermediate_runs, stats.max_write_passes)
            {
                return Err(CatalogError::Invalid(
                    "GC mark statistics disagree with roots or shards".to_string(),
                ));
            }
        }
        if let Some(stats) = &self.inventory_stats {
            let (candidate_objects, candidate_bytes) = self.quarantine_shards.iter().try_fold(
                (0u64, 0u64),
                |(objects, bytes), shard| {
                    Ok::<_, CatalogError>((
                        objects.checked_add(shard.candidate_count).ok_or_else(|| {
                            CatalogError::Invalid("GC candidate count overflow".to_string())
                        })?,
                        bytes.checked_add(shard.candidate_bytes).ok_or_else(|| {
                            CatalogError::Invalid("GC candidate byte count overflow".to_string())
                        })?,
                    ))
                },
            )?;
            let classified = stats
                .objects_newer_than_cutoff
                .checked_add(stats.reachable_objects)
                .and_then(|value| value.checked_add(stats.candidate_objects))
                .ok_or_else(|| CatalogError::Invalid("GC inventory count overflow".to_string()))?;
            if stats.objects_seen != classified
                || stats.candidate_objects != candidate_objects
                || stats.candidate_bytes != candidate_bytes
                || !valid_gc_write_pass_bound(stats.intermediate_runs, stats.max_write_passes)
            {
                return Err(CatalogError::Invalid(
                    "GC inventory statistics disagree with quarantine shards".to_string(),
                ));
            }
        }
        if let Some(quarantine_at) = self.quarantine_at {
            validate_timestamp(quarantine_at, "GC quarantine timestamp")?;
            if quarantine_at < self.created_at {
                return Err(CatalogError::Invalid(
                    "GC quarantine cannot precede run creation".to_string(),
                ));
            }
        }
        if let Some(observation) = &self.revalidation {
            observation.validate(self.quarantine_at.ok_or_else(|| {
                CatalogError::Invalid("GC revalidation requires quarantine time".to_string())
            })?)?;
            if let Some(stats) = &observation.stats
                && self.inventory_stats.as_ref().is_none_or(|inventory| {
                    stats.first_observation_candidates != inventory.candidate_objects
                })
            {
                return Err(CatalogError::Invalid(
                    "GC revalidation did not classify every first-observation candidate"
                        .to_string(),
                ));
            }
        }
        if let Some(progress) = &self.deletion {
            progress.validate(self)?;
        }
        Ok(())
    }
}

impl GcDeletionProgress {
    fn validate(&self, run: &GcRunRecord) -> Result<(), CatalogError> {
        if self.batch_size == 0 || self.batch_size > gc::MAX_DELETE_BATCH_SIZE {
            return Err(CatalogError::Invalid(
                "GC delete batch size is outside the safety bound".to_string(),
            ));
        }
        if self.next_shard > 256 || (self.next_shard == 256 && self.next_record != 0) {
            return Err(CatalogError::Invalid(
                "GC deletion cursor is invalid".to_string(),
            ));
        }
        validate_timestamp(self.started_at, "GC deletion start")?;
        let revalidation_stats = run
            .revalidation
            .as_ref()
            .and_then(|observation| observation.stats.as_ref())
            .ok_or_else(|| {
                CatalogError::Invalid("GC deletion requires revalidation".to_string())
            })?;
        let revalidation_completed = run
            .revalidation
            .as_ref()
            .and_then(|observation| observation.completed_at)
            .ok_or_else(|| {
                CatalogError::Invalid("GC deletion requires completed revalidation".to_string())
            })?;
        let retained = revalidation_stats.retained_candidates;
        let candidate_shards = &run
            .revalidation
            .as_ref()
            .expect("checked above")
            .candidate_shards;
        if self.next_shard < 256
            && self.next_record > candidate_shards[self.next_shard as usize].candidate_count
        {
            return Err(CatalogError::Invalid(
                "GC deletion cursor exceeds its candidate shard".to_string(),
            ));
        }
        let processed = candidate_shards
            .iter()
            .take(self.next_shard as usize)
            .try_fold(0u64, |total, shard| {
                total.checked_add(shard.candidate_count).ok_or_else(|| {
                    CatalogError::Invalid("GC deletion cursor count overflow".to_string())
                })
            })?
            .checked_add(self.next_record)
            .ok_or_else(|| {
                CatalogError::Invalid("GC deletion cursor count overflow".to_string())
            })?;
        let classified = self
            .deleted_objects
            .checked_add(self.already_absent)
            .ok_or_else(|| CatalogError::Invalid("GC deletion count overflow".to_string()))?;
        if self.started_at < revalidation_completed
            || self.deleted_bytes > revalidation_stats.retained_bytes
            || classified != processed
            || classified > retained
            || (self.completed_at.is_some() && (self.next_shard != 256 || classified != retained))
        {
            return Err(CatalogError::Invalid(
                "GC deletion progress disagrees with retained candidates".to_string(),
            ));
        }
        if let Some(completed_at) = self.completed_at {
            validate_timestamp(completed_at, "GC deletion completion")?;
            if completed_at < self.started_at {
                return Err(CatalogError::Invalid(
                    "GC deletion completion precedes its start".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl GcRevalidationRecord {
    fn validate(&self, quarantine_at: DateTime<Utc>) -> Result<(), CatalogError> {
        validate_id(self.id, "GC revalidation")?;
        if self.grace_seconds < gc::MIN_REVALIDATION_GRACE_SECONDS {
            return Err(CatalogError::Invalid(
                "GC revalidation grace is below the safety minimum".to_string(),
            ));
        }
        for timestamp in [self.not_before, self.inventory_cutoff, self.captured_at] {
            validate_timestamp(timestamp, "GC revalidation timestamp")?;
        }
        if self.not_before
            != quarantine_at
                .checked_add_signed(chrono::Duration::seconds(
                    i64::try_from(self.grace_seconds).map_err(|_| {
                        CatalogError::Invalid("GC revalidation grace is too large".to_string())
                    })?,
                ))
                .ok_or_else(|| {
                    CatalogError::Invalid("GC revalidation grace overflows time".to_string())
                })?
            || self.captured_at < self.not_before
            || self.inventory_cutoff != self.captured_at
        {
            return Err(CatalogError::Invalid(
                "GC revalidation did not observe the configured grace boundary".to_string(),
            ));
        }
        let mut roots = self.roots.clone();
        for pin in &roots {
            validate_root(&pin.root)?;
        }
        canonicalize_gc_root_pins(&mut roots);
        if roots != self.roots || gc_root_digest(&self.roots)? != self.root_digest {
            return Err(CatalogError::Invalid(
                "GC revalidation roots or digest are not canonical".to_string(),
            ));
        }
        validate_gc_mark_descriptors(&self.mark_shards)?;
        validate_gc_quarantine_descriptors(&self.candidate_shards)?;
        if let Some(stats) = &self.mark_stats {
            let unique_segments = self.mark_shards.iter().try_fold(0u64, |total, shard| {
                total.checked_add(shard.segment_count).ok_or_else(|| {
                    CatalogError::Invalid("GC revalidation mark count overflow".to_string())
                })
            })?;
            if stats.roots_enumerated != self.roots.len() as u64
                || stats.unique_segments != unique_segments
                || stats.unique_segments > stats.references_enumerated
                || !valid_gc_write_pass_bound(stats.intermediate_runs, stats.max_write_passes)
            {
                return Err(CatalogError::Invalid(
                    "GC revalidation mark statistics are inconsistent".to_string(),
                ));
            }
        }
        if let Some(stats) = &self.stats {
            let (objects, bytes) = self.candidate_shards.iter().try_fold(
                (0u64, 0u64),
                |(objects, bytes), shard| {
                    Ok::<_, CatalogError>((
                        objects.checked_add(shard.candidate_count).ok_or_else(|| {
                            CatalogError::Invalid(
                                "GC revalidated candidate count overflow".to_string(),
                            )
                        })?,
                        bytes.checked_add(shard.candidate_bytes).ok_or_else(|| {
                            CatalogError::Invalid(
                                "GC revalidated candidate bytes overflow".to_string(),
                            )
                        })?,
                    ))
                },
            )?;
            let classified = stats
                .became_reachable
                .checked_add(stats.already_absent)
                .and_then(|value| value.checked_add(stats.retained_candidates))
                .ok_or_else(|| {
                    CatalogError::Invalid("GC revalidation count overflow".to_string())
                })?;
            if stats.first_observation_candidates != classified
                || stats.retained_candidates != objects
                || stats.retained_bytes != bytes
            {
                return Err(CatalogError::Invalid(
                    "GC revalidation statistics disagree with candidate shards".to_string(),
                ));
            }
        }
        if let Some(completed_at) = self.completed_at {
            validate_timestamp(completed_at, "GC revalidation completion")?;
            if completed_at < self.captured_at {
                return Err(CatalogError::Invalid(
                    "GC revalidation completion precedes capture".to_string(),
                ));
            }
        }
        Ok(())
    }
}

fn validate_gc_mark_descriptors(shards: &[GcMarkShard]) -> Result<(), CatalogError> {
    for (expected_shard, shard) in (0u8..=u8::MAX).zip(shards) {
        if shard.shard != expected_shard
            || shard.location.is_empty()
            || shard.location.len() > MAX_ROOT_IDENTIFIER_BYTES
            || shard.checksum.len() != 64
            || !shard.checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(CatalogError::Invalid(
                "GC mark shards must be complete, ordered, bounded, and checksummed".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_gc_quarantine_descriptors(shards: &[GcQuarantineShard]) -> Result<(), CatalogError> {
    for (expected_shard, shard) in (0u8..=u8::MAX).zip(shards) {
        if shard.shard != expected_shard
            || shard.location.is_empty()
            || shard.location.len() > MAX_ROOT_IDENTIFIER_BYTES
            || shard.checksum.len() != 64
            || !shard.checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(CatalogError::Invalid(
                "GC quarantine shards must be complete, ordered, bounded, and checksummed"
                    .to_string(),
            ));
        }
    }
    Ok(())
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

impl RetiredCatalogId {
    fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.id, "retired catalog")
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
    pub gc_runs: BTreeMap<Uuid, GcRunRecord>,
    #[serde(default)]
    pub leases: BTreeMap<Uuid, LeaseRecord>,
    #[serde(default)]
    pub lease_tombstones: BTreeMap<Uuid, LeaseTombstone>,
    #[serde(default)]
    pub tombstones: BTreeMap<Uuid, TombstoneRecord>,
    #[serde(default)]
    #[doc(hidden)]
    pub retired_catalog_ids: BTreeMap<Uuid, RetiredCatalogId>,
    #[serde(default)]
    pub private_epochs: BTreeMap<u64, PrivateEpochRecord>,
    #[serde(default)]
    pub local_gc_guards: BTreeMap<Uuid, LocalGcGuardRecord>,
    #[serde(default)]
    pub local_gc_progress: BTreeMap<Uuid, LocalGcProgressRecord>,
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
            gc_runs: BTreeMap::new(),
            leases: BTreeMap::new(),
            lease_tombstones: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            retired_catalog_ids: BTreeMap::new(),
            private_epochs: BTreeMap::new(),
            local_gc_guards: BTreeMap::new(),
            local_gc_progress: BTreeMap::new(),
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
        validate_records(&self.gc_runs, |record| record.id, GcRunRecord::validate)?;
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
        validate_records(
            &self.retired_catalog_ids,
            |record| record.id,
            RetiredCatalogId::validate,
        )?;
        let mut unexposed_private_epoch_counts = BTreeMap::<Uuid, usize>::new();
        for (epoch, record) in &self.private_epochs {
            record.validate()?;
            if *epoch != record.epoch {
                return Err(CatalogError::Corrupt(
                    "private epoch key disagrees with its record".to_string(),
                ));
            }
            let compatible_live = self.branches.get(&record.branch_id).is_some_and(|branch| {
                branch.state == BranchState::Ready
                    || record.state == PrivateEpochState::Exposed
                        && branch.state == BranchState::Deleting
            });
            let deleted = self
                .tombstones
                .get(&record.branch_id)
                .is_some_and(|tombstone| tombstone.kind == TombstoneKind::Branch);
            let retired = self
                .retired_catalog_ids
                .get(&record.branch_id)
                .is_some_and(|record| record.kind == RetiredCatalogKind::Branch);
            if !(compatible_live
                || record.state == PrivateEpochState::Exposed && (deleted || retired))
            {
                return Err(CatalogError::Corrupt(format!(
                    "private epoch {} has no compatible exact branch incarnation",
                    record.epoch
                )));
            }
            if record.state != PrivateEpochState::Exposed {
                let count = unexposed_private_epoch_counts
                    .entry(record.branch_id)
                    .or_default();
                *count += 1;
                if *count > MAX_UNEXPOSED_PRIVATE_EPOCHS_PER_BRANCH {
                    return Err(CatalogError::Capacity {
                        resource: "unexposed private epoch per branch",
                        limit: MAX_UNEXPOSED_PRIVATE_EPOCHS_PER_BRANCH,
                    });
                }
            }
        }
        validate_records(
            &self.local_gc_guards,
            |record| record.id,
            LocalGcGuardRecord::validate,
        )?;
        validate_records(
            &self.local_gc_progress,
            |record| record.id,
            LocalGcProgressRecord::validate,
        )?;
        for guard in self.local_gc_guards.values() {
            let epoch = self.private_epochs.get(&guard.epoch).ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "local GC guard {} refers to missing epoch {}",
                    guard.id, guard.epoch
                ))
            })?;
            if epoch.branch_id != guard.branch_id
                || epoch.revision != guard.epoch_revision
                || epoch.state != PrivateEpochState::SealedPrivate
                || !self
                    .branches
                    .get(&guard.branch_id)
                    .is_some_and(|branch| branch.state == BranchState::Ready)
            {
                return Err(CatalogError::Corrupt(format!(
                    "local GC guard {} lost its sealed exact branch epoch",
                    guard.id
                )));
            }
        }
        for progress in self.local_gc_progress.values() {
            match (
                progress.completed_at,
                self.local_gc_guards.get(&progress.id),
            ) {
                (None, Some(guard)) if progress.matches_guard(guard) => {}
                (Some(_), None) => {}
                _ => {
                    return Err(CatalogError::Corrupt(format!(
                        "local GC progress {} disagrees with guard retirement",
                        progress.id
                    )));
                }
            }
        }
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
        let retired_ids = self
            .retired_catalog_ids
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
        let gc_run_ids = self
            .gc_runs
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
        let local_gc_ids = self
            .local_gc_guards
            .keys()
            .chain(self.local_gc_progress.keys())
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
            || !gc_run_ids.is_disjoint(&branch_ids)
            || !gc_run_ids.is_disjoint(&checkpoint_ids)
            || !gc_run_ids.is_disjoint(&operation_ids)
            || !gc_run_ids.is_disjoint(&delete_operation_ids)
            || !gc_run_ids.is_disjoint(&lease_ids)
            || !gc_run_ids.is_disjoint(&lease_tombstone_ids)
            || !gc_run_ids.is_disjoint(&tombstone_ids)
            || !local_gc_ids.is_disjoint(&branch_ids)
            || !local_gc_ids.is_disjoint(&checkpoint_ids)
            || !local_gc_ids.is_disjoint(&operation_ids)
            || !local_gc_ids.is_disjoint(&delete_operation_ids)
            || !local_gc_ids.is_disjoint(&gc_run_ids)
            || !local_gc_ids.is_disjoint(&lease_ids)
            || !local_gc_ids.is_disjoint(&lease_tombstone_ids)
            || !local_gc_ids.is_disjoint(&tombstone_ids)
            || !retired_ids.is_disjoint(&branch_ids)
            || !retired_ids.is_disjoint(&checkpoint_ids)
            || !retired_ids.is_disjoint(&operation_ids)
            || !retired_ids.is_disjoint(&delete_operation_ids)
            || !retired_ids.is_disjoint(&gc_run_ids)
            || !retired_ids.is_disjoint(&lease_ids)
            || !retired_ids.is_disjoint(&lease_tombstone_ids)
            || !retired_ids.is_disjoint(&tombstone_ids)
            || !retired_ids.is_disjoint(&local_gc_ids)
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
        for run in self.gc_runs.values() {
            if run.catalog_generation > self.generation {
                return Err(CatalogError::Corrupt(format!(
                    "GC run {} captured future catalog generation {}",
                    run.id, run.catalog_generation
                )));
            }
            if gc_root_digest(&run.roots)? != run.root_digest {
                return Err(CatalogError::Corrupt(format!(
                    "GC run {} root digest does not match its immutable root list",
                    run.id
                )));
            }
        }
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
        roots.extend(
            self.gc_runs
                .values()
                .filter(|run| run.retains_roots())
                .flat_map(|run| run.roots.iter().map(|pin| &pin.root)),
        );
        roots.extend(
            self.gc_runs
                .values()
                .filter(|run| run.retains_roots())
                .filter_map(|run| run.revalidation.as_ref())
                .flat_map(|observation| observation.roots.iter().map(|pin| &pin.root)),
        );
        roots
    }

    /// Stable, typed root list captured at this exact catalog generation.
    pub fn gc_root_pins(&self) -> Vec<GcRootPin> {
        let mut pins = Vec::new();
        let mut push = |kind, root: &DurableRoot| {
            pins.push(GcRootPin {
                kind,
                root: root.clone(),
            });
        };
        for branch in self.branches.values() {
            if let Some(root) = &branch.root {
                push(GcRootKind::Branch, root);
            }
        }
        for checkpoint in self.checkpoints.values() {
            push(GcRootKind::Checkpoint, &checkpoint.root);
        }
        for operation in self.branch_create_operations.values() {
            match operation.phase {
                BranchCreatePhase::Reserved => push(GcRootKind::Checkpoint, &operation.source_root),
                BranchCreatePhase::RootCreated => {
                    if let Some(root) = &operation.destination_root {
                        push(GcRootKind::Branch, root);
                    }
                }
                BranchCreatePhase::Published => {}
            }
        }
        for operation in self.branch_delete_operations.values() {
            if operation.phase == BranchDeletePhase::Draining {
                push(GcRootKind::Branch, &operation.root);
            }
        }
        for lease in self.leases.values() {
            let kind = match lease.subject_kind {
                LeaseSubjectKind::Branch => GcRootKind::Branch,
                LeaseSubjectKind::Checkpoint => GcRootKind::Checkpoint,
            };
            push(kind, &lease.root);
        }
        for run in self.gc_runs.values().filter(|run| run.retains_roots()) {
            for pin in &run.roots {
                push(pin.kind, &pin.root);
            }
            if let Some(observation) = &run.revalidation {
                for pin in &observation.roots {
                    push(pin.kind, &pin.root);
                }
            }
        }
        canonicalize_gc_root_pins(&mut pins);
        pins
    }
}

fn canonicalize_gc_root_pins(pins: &mut Vec<GcRootPin>) {
    pins.sort_by(|left, right| {
        (
            gc_root_kind_order(left.kind),
            &left.root.identity,
            &left.root.manifest_id,
        )
            .cmp(&(
                gc_root_kind_order(right.kind),
                &right.root.identity,
                &right.root.manifest_id,
            ))
    });
    pins.dedup();
}

fn valid_gc_write_pass_bound(intermediate_runs: u64, max_write_passes: u32) -> bool {
    // Zero preserves compatibility with run records written before the bound
    // became durable. New nonempty observations publish at least two passes.
    max_write_passes == 0
        || (intermediate_runs != 0
            && max_write_passes >= 2
            && max_write_passes
                <= gc_mark::binary_carry_global_write_pass_upper_bound(intermediate_runs))
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // General resource mutations are consumed by later lifecycle slices.
pub(crate) enum CatalogMutation {
    AcquireLocalGcGuard(LocalGcGuardRecord),
    PublishLocalGcProgress(LocalGcProgressRecord),
    RegisterPrivateEpoch(PrivateEpochRecord),
    SealPrivateEpoch {
        epoch: u64,
        branch_id: Uuid,
        expected_revision: u64,
        next_epoch: u64,
        expected_next_revision: u64,
        sealed_at: DateTime<Utc>,
    },
    ExposePrivateEpoch {
        epoch: u64,
        branch_id: Uuid,
        expected_revision: u64,
        exposed_at: DateTime<Utc>,
    },
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
    PublishWriterHead {
        lease_id: Uuid,
        expected_lease_revision: u64,
        token_hash: String,
        previous_root: DurableRoot,
        root: DurableRoot,
        published_at: DateTime<Utc>,
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
    /// Test-only fixture insertion. Production root publication must use an
    /// authenticated lifecycle transition; there is intentionally no generic
    /// root-bearing create escape hatch.
    #[cfg(test)]
    CreateBranch(BranchRecord),
    #[cfg(test)]
    ReplaceBranch {
        expected_revision: u64,
        record: BranchRecord,
    },
    CreateCheckpoint(CheckpointRecord),
    #[cfg(test)]
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

/// Keep customer projection reads bounded independently of the catalog's
/// production admission limits.
pub const MAX_CUSTOMER_CATALOG_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomerCatalogListRequest {
    pub kind: Option<CustomerResourceKind>,
    pub parent_id: Option<Uuid>,
    pub state: Option<String>,
    pub after: Option<Uuid>,
    pub limit: usize,
}

impl CustomerCatalogListRequest {
    fn validate(&self) -> Result<(), CatalogError> {
        if self.limit == 0 || self.limit > MAX_CUSTOMER_CATALOG_PAGE_SIZE {
            return Err(CatalogError::Invalid(format!(
                "customer catalog page size must be within 1..={MAX_CUSTOMER_CATALOG_PAGE_SIZE}"
            )));
        }
        if self.after.is_some_and(|after| after.is_nil()) {
            return Err(CatalogError::Invalid(
                "customer catalog cursor UUID cannot be nil".to_string(),
            ));
        }
        if self.parent_id.is_some_and(|parent_id| parent_id.is_nil()) {
            return Err(CatalogError::Invalid(
                "customer catalog parent UUID cannot be nil".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CustomerCatalogPage {
    pub records: Vec<CustomerCatalogRecord>,
    pub next_after: Option<Uuid>,
}

/// Read/write handle for the reconstructible customer view selected by server
/// configuration. This capability contains no lifecycle or storage authority.
#[derive(Clone)]
pub struct CustomerCatalog {
    volume_id: Uuid,
    projection: Arc<dyn CatalogProjection>,
}

impl std::fmt::Debug for CustomerCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CustomerCatalog")
            .field("volume_id", &self.volume_id)
            .finish_non_exhaustive()
    }
}

impl CustomerCatalog {
    pub fn new(volume_id: Uuid, projection: Arc<dyn CatalogProjection>) -> Self {
        Self {
            volume_id,
            projection,
        }
    }

    pub async fn record(
        &self,
        resource_id: Uuid,
    ) -> Result<Option<CustomerCatalogRecord>, CatalogError> {
        self.projection.record(self.volume_id, resource_id).await
    }

    pub async fn list(
        &self,
        request: CustomerCatalogListRequest,
    ) -> Result<CustomerCatalogPage, CatalogError> {
        self.projection.list(self.volume_id, request).await
    }

    pub async fn set_customer_metadata(
        &self,
        resource_id: Uuid,
        metadata: CustomerMetadata,
    ) -> Result<(), CatalogError> {
        self.projection
            .set_customer_metadata(self.volume_id, resource_id, metadata)
            .await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("catalog {resource} capacity of {limit} has been reached")]
    Capacity {
        resource: &'static str,
        limit: usize,
    },
    #[error("branch {0} still has an unreconciled writer lease")]
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
    /// Close backend resources owned by this catalog. In-memory test adapters
    /// need no action; durable backends override this boundary.
    async fn close(&self) -> Result<(), CatalogError> {
        Ok(())
    }
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
    async fn gc_run(&self, id: Uuid) -> Result<Option<GcRunRecord>, CatalogError>;
    async fn gc_blockers(&self, run_id: Uuid) -> Result<Vec<GcBlockerRecord>, CatalogError>;
    async fn private_epoch(&self, _epoch: u64) -> Result<Option<PrivateEpochRecord>, CatalogError> {
        Err(CatalogError::Invalid(
            "catalog backend does not support private epochs".to_string(),
        ))
    }
    async fn private_gc_owner_view(
        &self,
        branch_id: Uuid,
        database_identity: &str,
        epoch_limit: usize,
    ) -> Result<PrivateGcOwnerView, CatalogError> {
        let snapshot = self.snapshot().await?;
        if !snapshot.branches.get(&branch_id).is_some_and(|branch| {
            branch.state == BranchState::Ready
                && branch
                    .root
                    .as_ref()
                    .is_some_and(|root| root.identity == database_identity)
        }) {
            return Err(CatalogError::OperationConflict(format!(
                "private GC owner branch {branch_id} is not the exact ready database"
            )));
        }
        let active_guard = snapshot
            .local_gc_guards
            .values()
            .filter(|guard| {
                snapshot
                    .private_epochs
                    .get(&guard.epoch)
                    .is_some_and(|epoch| {
                        epoch.branch_id == branch_id && epoch.database_identity == database_identity
                    })
            })
            .min_by_key(|guard| (guard.created_at, guard.id))
            .cloned();
        let sealed_epochs = if active_guard.is_some() {
            Vec::new()
        } else {
            snapshot
                .private_epochs
                .values()
                .filter(|epoch| {
                    epoch.state == PrivateEpochState::SealedPrivate
                        && epoch.branch_id == branch_id
                        && epoch.database_identity == database_identity
                })
                .take(epoch_limit)
                .cloned()
                .collect()
        };
        Ok(PrivateGcOwnerView {
            active_guard,
            sealed_epochs,
        })
    }
    async fn private_gc_guard_view(
        &self,
        guard_id: Uuid,
        writer_epoch: u64,
    ) -> Result<PrivateGcGuardView, CatalogError> {
        let snapshot = self.snapshot().await?;
        let guard = snapshot.local_gc_guards.get(&guard_id).cloned();
        let progress = snapshot.local_gc_progress.get(&guard_id).cloned();
        let guarded_epoch_id = guard
            .as_ref()
            .map(|record| record.epoch)
            .or_else(|| progress.as_ref().map(|record| record.epoch));
        let guarded_epoch =
            guarded_epoch_id.and_then(|epoch| snapshot.private_epochs.get(&epoch).cloned());
        if let Some(guard) = &guard {
            let epoch = guarded_epoch.as_ref().ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "local GC guard {guard_id} refers to a missing epoch"
                ))
            })?;
            if !snapshot
                .branches
                .get(&guard.branch_id)
                .is_some_and(|branch| {
                    branch.state == BranchState::Ready
                        && branch
                            .root
                            .as_ref()
                            .is_some_and(|root| root.identity == epoch.database_identity)
                })
            {
                return Err(CatalogError::OperationConflict(format!(
                    "private GC guard {guard_id} lost its exact ready owner"
                )));
            }
        }
        Ok(PrivateGcGuardView {
            guard,
            progress,
            guarded_epoch,
            writer_epoch: snapshot.private_epochs.get(&writer_epoch).cloned(),
        })
    }
    /// Persist immutable root pins only if no root-affecting catalog mutation
    /// occurred since capture. The pins duplicate roots present at that same
    /// generation, so this bookkeeping write does not advance it.
    async fn begin_gc_run(
        &self,
        expected_generation: u64,
        run: GcRunRecord,
    ) -> Result<(), CatalogError>;
    async fn publish_gc_marks(
        &self,
        id: Uuid,
        expected_revision: u64,
        root_digest: String,
        mark_shards: Vec<GcMarkShard>,
        mark_stats: GcMarkStats,
        updated_at: DateTime<Utc>,
    ) -> Result<GcRunRecord, CatalogError>;
    /// Publish a terminal mark/inventory report and atomically release its
    /// captured root pins. This transition cannot authorize quarantine,
    /// revalidation, or physical deletion.
    async fn publish_gc_report(
        &self,
        _publication: GcReportPublication,
    ) -> Result<GcRunRecord, CatalogError> {
        Err(CatalogError::Invalid(
            "catalog backend does not support GC reporting".to_string(),
        ))
    }
    /// Publish a verified first unreachable observation only if the catalog
    /// still has the exact root generation captured by this run.
    async fn publish_gc_quarantine(
        &self,
        publication: GcQuarantinePublication,
    ) -> Result<GcRunRecord, CatalogError>;
    /// Pin a fresh root snapshot after the first-observation grace period. The
    /// bookkeeping write is generation-neutral and succeeds only at the exact
    /// newly observed catalog generation.
    async fn begin_gc_revalidation(
        &self,
        _capture: GcRevalidationCapture,
    ) -> Result<GcRunRecord, CatalogError> {
        Err(CatalogError::Invalid(
            "catalog backend does not support GC revalidation".to_string(),
        ))
    }
    /// Accept the independently rebuilt mark and surviving candidate sets only
    /// if the catalog still has the fresh generation captured above.
    async fn publish_gc_revalidation(
        &self,
        _publication: GcRevalidationPublication,
    ) -> Result<GcRunRecord, CatalogError> {
        Err(CatalogError::Invalid(
            "catalog backend does not support GC revalidation".to_string(),
        ))
    }
    /// Persist a deletion cursor transition while the catalog still has the
    /// second observation's exact generation.
    async fn publish_gc_deletion(
        &self,
        _publication: GcDeletionPublication,
    ) -> Result<GcRunRecord, CatalogError> {
        Err(CatalogError::Invalid(
            "catalog backend does not support GC deletion".to_string(),
        ))
    }
    async fn record_gc_blocker(
        &self,
        run_id: Uuid,
        kind: GcBlockerKind,
        detail: String,
        observed_at: DateTime<Utc>,
    ) -> Result<GcBlockerRecord, CatalogError>;
    async fn lease(&self, id: Uuid) -> Result<Option<LeaseRecord>, CatalogError>;
    async fn lease_tombstone(&self, id: Uuid) -> Result<Option<LeaseTombstone>, CatalogError> {
        Ok(self.snapshot().await?.lease_tombstones.get(&id).cloned())
    }
    async fn tombstone(&self, id: Uuid) -> Result<Option<TombstoneRecord>, CatalogError>;
    async fn cleanup_tombstones(
        &self,
        _policy: TombstoneCleanupPolicy,
    ) -> Result<TombstoneCleanupReport, CatalogError> {
        Err(CatalogError::Invalid(
            "catalog backend does not support tombstone cleanup".to_string(),
        ))
    }

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
    /// List a stable UUID-ordered page from the root-free customer view.
    /// Deleted and compacted (`absent`) records remain visible for audit.
    async fn list(
        &self,
        volume_id: Uuid,
        request: CustomerCatalogListRequest,
    ) -> Result<CustomerCatalogPage, CatalogError>;
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

fn gc_root_kind_order(kind: GcRootKind) -> u8 {
    match kind {
        GcRootKind::Branch => 0,
        GcRootKind::Checkpoint => 1,
    }
}

pub(crate) fn gc_root_digest(roots: &[GcRootPin]) -> Result<String, CatalogError> {
    use sha2::{Digest, Sha256};
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(roots)?)))
}

fn validate_branch_create_relationships(snapshot: &CatalogSnapshot) -> Result<(), CatalogError> {
    let mut destinations = std::collections::BTreeSet::new();
    let mut genesis_operations = 0usize;
    for operation in snapshot.branch_create_operations.values() {
        if operation.parent_id.is_none() {
            genesis_operations += 1;
            if genesis_operations > 1 {
                return Err(CatalogError::Corrupt(
                    "catalog contains multiple parentless genesis operations".to_string(),
                ));
            }
        }
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
                let expected_origin = operation.parent_id.map(|_| operation.source_checkpoint_id);
                if branch.state != BranchState::Creating
                    || branch.name != operation.destination_name
                    || branch.parent_id != operation.parent_id
                    || branch.origin_checkpoint_id != expected_origin
                    || branch.root.is_some()
                {
                    return Err(CatalogError::Corrupt(format!(
                        "incomplete operation {} disagrees with its destination branch",
                        operation.id
                    )));
                }
                if operation.parent_id.is_none() {
                    // A genesis operation is sourced from an exact physical
                    // checkpoint outside the previously empty catalog.
                } else if operation.phase == BranchCreatePhase::Reserved {
                    let source = snapshot
                        .checkpoints
                        .get(&operation.source_checkpoint_id)
                        .ok_or_else(|| {
                            CatalogError::Corrupt(format!(
                                "reserved operation {} has no live source checkpoint",
                                operation.id
                            ))
                        })?;
                    if source.root != operation.source_root
                        || operation.parent_id != Some(source.branch_id)
                    {
                        return Err(CatalogError::Corrupt(format!(
                            "reserved operation {} source identity changed",
                            operation.id
                        )));
                    }
                } else {
                    let live_source_matches = snapshot
                        .checkpoints
                        .get(&operation.source_checkpoint_id)
                        .is_some_and(|source| {
                            source.root == operation.source_root
                                && operation.parent_id == Some(source.branch_id)
                        });
                    let deleted_source_matches = snapshot
                        .tombstones
                        .get(&operation.source_checkpoint_id)
                        .is_some_and(|tombstone| {
                            tombstone.kind == TombstoneKind::Checkpoint
                                && tombstone.parent_id == operation.parent_id
                        });
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
                    let state_matches = branch.state == BranchState::Ready
                        || (branch.state == BranchState::Deleting
                            && snapshot.branch_delete_operations.values().any(|deletion| {
                                deletion.branch_id == branch.id
                                    && deletion.phase == BranchDeletePhase::Draining
                            }));
                    let expected_origin =
                        operation.parent_id.map(|_| operation.source_checkpoint_id);
                    if !state_matches
                        || branch.parent_id != operation.parent_id
                        || branch.origin_checkpoint_id != expected_origin
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
                    && !snapshot
                        .retired_catalog_ids
                        .get(&operation.destination_id)
                        .is_some_and(|record| record.kind == RetiredCatalogKind::Branch)
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
    fn durable_gc_write_pass_bound_accepts_legacy_zero_and_rejects_impossible_values() {
        assert!(valid_gc_write_pass_bound(0, 0));
        assert!(valid_gc_write_pass_bound(5, 0));
        assert!(valid_gc_write_pass_bound(1, 2));
        assert!(valid_gc_write_pass_bound(5, 5));
        // Seven runs in one shard plus one in another legitimately publish a
        // six-pass maximum even though the exact eight-run formula is five.
        assert!(valid_gc_write_pass_bound(8, 6));
        assert!(valid_gc_write_pass_bound(8, 7));
        assert!(!valid_gc_write_pass_bound(0, 2));
        assert!(!valid_gc_write_pass_bound(5, 1));
        assert!(!valid_gc_write_pass_bound(5, 7));
        assert!(!valid_gc_write_pass_bound(8, 8));
    }

    fn gc_scale_snapshot(branch_count: usize, checkpoints_per_branch: usize) -> CatalogSnapshot {
        let now = catalog_timestamp(Utc::now());
        let mut snapshot = CatalogSnapshot::default();
        for branch_index in 0..branch_count {
            let branch_id = Uuid::from_u128(branch_index as u128 + 1);
            snapshot.branches.insert(
                branch_id,
                BranchRecord {
                    id: branch_id,
                    revision: 1,
                    name: format!("scale-branch-{branch_index}"),
                    state: BranchState::Ready,
                    root: Some(DurableRoot {
                        identity: format!("scale/branches/{branch_index}"),
                        manifest_id: format!("branch-manifest-{branch_index}"),
                    }),
                    parent_id: None,
                    origin_checkpoint_id: None,
                    created_at: now,
                    updated_at: now,
                },
            );
            for checkpoint_index in 0..checkpoints_per_branch {
                let ordinal = branch_index
                    .checked_mul(checkpoints_per_branch)
                    .and_then(|value| value.checked_add(checkpoint_index))
                    .expect("scale fixture ordinal fits usize");
                let checkpoint_id = Uuid::from_u128((1u128 << 64) | (ordinal as u128 + 1));
                snapshot.checkpoints.insert(
                    checkpoint_id,
                    CheckpointRecord {
                        id: checkpoint_id,
                        revision: 1,
                        branch_id,
                        name: format!("checkpoint-{checkpoint_index}"),
                        root: DurableRoot {
                            identity: format!("scale/checkpoints/{ordinal}"),
                            manifest_id: format!("checkpoint-manifest-{ordinal}"),
                        },
                        created_at: now,
                        updated_at: now,
                    },
                );
            }
        }
        snapshot
    }

    #[test]
    fn gc_root_pin_collection_sorts_and_deduplicates_after_linear_collection() {
        let mut snapshot = gc_scale_snapshot(64, 32);
        let shared = snapshot
            .branches
            .values()
            .next()
            .and_then(|branch| branch.root.clone())
            .unwrap();
        for checkpoint in snapshot.checkpoints.values_mut().take(64) {
            checkpoint.root = shared.clone();
        }
        let pins = snapshot.gc_root_pins();
        assert_eq!(pins.len(), 64 + 64 * 32 - 64 + 1);
        assert!(pins.windows(2).all(|pair| {
            (
                gc_root_kind_order(pair[0].kind),
                &pair[0].root.identity,
                &pair[0].root.manifest_id,
            ) < (
                gc_root_kind_order(pair[1].kind),
                &pair[1].root.identity,
                &pair[1].root.manifest_id,
            )
        }));
    }

    /// Manual release-mode qualification of the complete declared catalog
    /// envelope. Run with:
    /// `cargo test --release gc_root_capture_supported_envelope -- --ignored --nocapture`.
    #[test]
    #[ignore = "release-mode supported-envelope benchmark"]
    fn gc_root_capture_supported_envelope() {
        let fixture_started = std::time::Instant::now();
        let snapshot = gc_scale_snapshot(MAX_LIVE_BRANCHES, MAX_CHECKPOINTS_PER_BRANCH);
        let fixture_ms = fixture_started.elapsed().as_millis();

        let validation_started = std::time::Instant::now();
        snapshot.validate().unwrap();
        let validation_ms = validation_started.elapsed().as_millis();

        let capture_started = std::time::Instant::now();
        let pins = snapshot.gc_root_pins();
        let capture_ms = capture_started.elapsed().as_millis();
        let expected_roots = MAX_LIVE_BRANCHES
            .checked_mul(MAX_CHECKPOINTS_PER_BRANCH + 1)
            .unwrap();
        assert_eq!(pins.len(), expected_roots);

        let digest_started = std::time::Instant::now();
        let digest = gc_root_digest(&pins).unwrap();
        let digest_ms = digest_started.elapsed().as_millis();
        let encoded_bytes = serde_json::to_vec(&pins).unwrap().len();
        println!(
            "{}",
            serde_json::json!({
                "branches": MAX_LIVE_BRANCHES,
                "checkpoints_per_branch": MAX_CHECKPOINTS_PER_BRANCH,
                "roots": pins.len(),
                "encoded_root_bytes": encoded_bytes,
                "fixture_ms": fixture_ms,
                "snapshot_validation_ms": validation_ms,
                "root_collection_ms": capture_ms,
                "root_digest_ms": digest_ms,
                "root_digest": digest,
            })
        );
    }

    #[test]
    fn bounded_cleanup_metrics_export_kind_and_backlog_signal() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            record_cleanup_metrics("tombstones", 7, 2, 1, 4, 3);
        });
        let rendered = handle.render();
        assert!(rendered.contains("zerofs_catalog_cleanup_passes_total{kind=\"tombstones\"} 1"));
        assert!(rendered.contains("zerofs_catalog_cleanup_examined_total{kind=\"tombstones\"} 7"));
        assert!(rendered.contains("zerofs_catalog_cleanup_removed_total{kind=\"tombstones\"} 2"));
        assert!(
            rendered.contains("zerofs_catalog_cleanup_already_absent_total{kind=\"tombstones\"} 1")
        );
        assert!(rendered.contains("zerofs_catalog_cleanup_retained_total{kind=\"tombstones\"} 4"));
        assert!(rendered.contains("zerofs_catalog_cleanup_backlog_signal{kind=\"tombstones\"} 3"));
    }

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
    fn snapshot_rejects_completed_local_gc_audit_id_collision() {
        let id = Uuid::new_v4();
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
        snapshot.local_gc_progress.insert(
            id,
            LocalGcProgressRecord {
                id,
                revision: 2,
                branch_id: Uuid::new_v4(),
                epoch: 1,
                epoch_revision: 2,
                candidate_count: 1,
                candidate_digest: "a".repeat(64),
                next_candidate: 1,
                deleted_objects: 1,
                deleted_bytes: 4096,
                already_absent: 0,
                started_at: now,
                updated_at: now,
                completed_at: Some(now),
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

    #[test]
    fn snapshot_accepts_published_create_while_branch_delete_is_draining() {
        let now = catalog_timestamp(Utc::now());
        let branch_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();
        let checkpoint_id = Uuid::new_v4();
        let create_id = Uuid::new_v4();
        let delete_id = Uuid::new_v4();
        let root = DurableRoot {
            identity: "branches/draining".to_string(),
            manifest_id: "manifest-1".to_string(),
        };
        let mut snapshot = CatalogSnapshot::default();
        snapshot.branches.insert(
            branch_id,
            BranchRecord {
                id: branch_id,
                revision: 3,
                name: "draining".to_string(),
                state: BranchState::Deleting,
                root: Some(root.clone()),
                parent_id: Some(parent_id),
                origin_checkpoint_id: Some(checkpoint_id),
                created_at: now,
                updated_at: now,
            },
        );
        snapshot.branch_create_operations.insert(
            create_id,
            BranchCreateOperation {
                id: create_id,
                revision: 3,
                destination_id: branch_id,
                destination_name: "draining".to_string(),
                source_checkpoint_id: checkpoint_id,
                source_root: root.clone(),
                parent_id: Some(parent_id),
                phase: BranchCreatePhase::Published,
                destination_root: Some(root.clone()),
                created_at: now,
                updated_at: now,
            },
        );
        snapshot.branch_delete_operations.insert(
            delete_id,
            BranchDeleteOperation {
                id: delete_id,
                revision: 1,
                branch_id,
                branch_name: "draining".to_string(),
                expected_branch_revision: 2,
                root,
                parent_id: Some(parent_id),
                origin_checkpoint_id: Some(checkpoint_id),
                phase: BranchDeletePhase::Draining,
                created_at: now,
                updated_at: now,
            },
        );

        snapshot.validate().unwrap();
    }
}
