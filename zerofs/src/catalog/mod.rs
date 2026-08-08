//! Durable metadata catalog for copy-on-write branches.
//!
//! Storage-critical state has one authority: SlateDB. PostgreSQL is an
//! optional, reconstructible customer-facing projection and is never consulted
//! to mount a branch or decide storage liveness. JSON and PostgreSQL implement
//! the same projection contract.

mod json;
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

pub use json::JsonCatalogProjection;
pub use postgres::PostgresCatalogProjection;
pub use root_store::{ImmutableCheckpoint, RootStoreError, SlateDbRootStore};
pub use slate::SlateDbCatalog;

pub const CATALOG_SCHEMA_VERSION: u32 = 2;
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
    pub async fn open(
        &self,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<dyn Catalog>, CatalogError> {
        Ok(Arc::new(
            SlateDbCatalog::open(
                slatedb::object_store::path::Path::from(self.slatedb_path.as_str()),
                object_store,
            )
            .await?,
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
    pub deleted_generation: u64,
    pub deleted_at: DateTime<Utc>,
}

impl TombstoneRecord {
    fn validate(&self) -> Result<(), CatalogError> {
        validate_id(self.id, "tombstone")?;
        validate_name(&self.name)?;
        validate_optional_id(self.parent_id, "tombstone parent")?;
        validate_optional_id(self.origin_checkpoint_id, "tombstone origin checkpoint")?;
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
    pub tombstones: BTreeMap<Uuid, TombstoneRecord>,
}

impl Default for CatalogSnapshot {
    fn default() -> Self {
        Self {
            schema_version: CATALOG_SCHEMA_VERSION,
            generation: 0,
            branches: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
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
        if !branch_ids.is_disjoint(&checkpoint_ids)
            || !branch_ids.is_disjoint(&tombstone_ids)
            || !checkpoint_ids.is_disjoint(&tombstone_ids)
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
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum CatalogMutation {
    CreateBranch(BranchRecord),
    ReplaceBranch {
        expected_revision: u64,
        record: BranchRecord,
    },
    DeleteBranch {
        id: Uuid,
        expected_revision: u64,
        name: String,
        deleted_at: DateTime<Utc>,
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
pub trait Catalog: Send + Sync {
    async fn snapshot(&self) -> Result<CatalogSnapshot, CatalogError>;
    async fn branch(&self, id: Uuid) -> Result<Option<BranchRecord>, CatalogError>;
    async fn branch_by_name(&self, name: &str) -> Result<Option<BranchRecord>, CatalogError>;
    async fn checkpoint(&self, id: Uuid) -> Result<Option<CheckpointRecord>, CatalogError>;
    async fn checkpoint_by_name(
        &self,
        branch_id: Uuid,
        name: &str,
    ) -> Result<Option<CheckpointRecord>, CatalogError>;

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
                deleted_generation: 2,
                deleted_at: created_at - chrono::Duration::microseconds(1),
            },
        );
        assert!(snapshot.validate().is_err());

        snapshot.tombstones.get_mut(&id).unwrap().deleted_at = created_at;
        assert!(matches!(snapshot.validate(), Err(CatalogError::Corrupt(_))));
    }
}
