use super::{
    BranchRecord, CATALOG_SCHEMA_VERSION, Catalog, CatalogError, CatalogMutation, CatalogSnapshot,
    CheckpointRecord, TombstoneKind, TombstoneRecord, validate_name, validate_timestamp,
};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::ObjectStore;
use serde::{Serialize, de::DeserializeOwned};
use slatedb::config::WriteOptions;
use slatedb::object_store::path::Path;
use slatedb::{Db, WriteBatch};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

const STATE_KEY: &[u8] = b"catalog/state";
const BRANCH_PREFIX: &[u8] = b"catalog/branch/";
const BRANCH_NAME_PREFIX: &[u8] = b"catalog/branch-name/";
const CHECKPOINT_PREFIX: &[u8] = b"catalog/checkpoint/";
const CHECKPOINT_NAME_PREFIX: &[u8] = b"catalog/checkpoint-name/";
const TOMBSTONE_PREFIX: &[u8] = b"catalog/tombstone/";
const PREVIOUS_SCHEMA_VERSION: u32 = 1;

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
        if state.schema_version != PREVIOUS_SCHEMA_VERSION {
            return Err(CatalogError::Corrupt(format!(
                "unsupported SlateDB catalog schema version {}",
                state.schema_version
            )));
        }

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
        self.snapshot_unlocked(CatalogState {
            schema_version: CATALOG_SCHEMA_VERSION,
            generation: state.generation,
        })
        .await
        .map_err(|error| {
            CatalogError::Invalid(format!(
                "SlateDB catalog v{PREVIOUS_SCHEMA_VERSION} cannot migrate to v{CATALOG_SCHEMA_VERSION}: {error}"
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
        let tombstones = self
            .scan_records::<TombstoneRecord>(TOMBSTONE_PREFIX)
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
            CatalogMutation::CreateBranch(record) => {
                record.validate()?;
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
                        deleted_generation: next_generation,
                        deleted_at,
                    },
                )?;
            }
            CatalogMutation::CreateCheckpoint(record) => {
                record.validate()?;
                ensure_initial_revision(record.revision)?;
                ensure_resource_id_available(self.db.as_ref(), record.id).await?;
                if self.db.get(branch_key(record.branch_id)).await?.is_none() {
                    return Err(CatalogError::NotFound(format!(
                        "live checkpoint branch {}",
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
                validate_revision_change(expected_revision, old.revision, record.revision)?;
                if self.db.get(branch_key(record.branch_id)).await?.is_none() {
                    return Err(CatalogError::NotFound(format!(
                        "live checkpoint branch {}",
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
    for key in [branch_key(id), checkpoint_key(id), tombstone_key(id)] {
        ensure_absent(db, key, &id.to_string()).await?;
    }
    Ok(())
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
    use crate::catalog::{BranchState, DurableRoot, catalog_timestamp};
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
            Err(CatalogError::NotFound(_))
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
            Err(CatalogError::NotFound(_))
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
                schema_version: PREVIOUS_SCHEMA_VERSION,
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
                schema_version: PREVIOUS_SCHEMA_VERSION,
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
            assert_eq!(state.schema_version, PREVIOUS_SCHEMA_VERSION);
        }
        catalog.close().await.unwrap();
    }
}
