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

    async fn apply_unlocked(
        &self,
        expected_generation: u64,
        mutation: CatalogMutation,
    ) -> Result<u64, CatalogError> {
        let state = self.state_unlocked().await?;
        if state.generation != expected_generation {
            return Err(CatalogError::Conflict {
                expected: expected_generation,
                actual: state.generation,
            });
        }
        let next_generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| CatalogError::Corrupt("catalog generation overflow".to_string()))?;
        let mut batch = WriteBatch::new();

        match mutation {
            CatalogMutation::CreateBranch(record) => {
                record.validate()?;
                ensure_absent(self.db.as_ref(), branch_key(record.id), &record.name).await?;
                ensure_absent(
                    self.db.as_ref(),
                    branch_name_key(&record.name),
                    &record.name,
                )
                .await?;
                put_json(&mut batch, branch_key(record.id), &record)?;
                batch.put(branch_name_key(&record.name), record.id.to_string());
            }
            CatalogMutation::ReplaceBranch(record) => {
                record.validate()?;
                let old = self
                    .get_record::<BranchRecord>(branch_key(record.id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(record.id.to_string()))?;
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
                        deleted_generation: next_generation,
                        deleted_at,
                    },
                )?;
            }
            CatalogMutation::CreateCheckpoint(record) => {
                record.validate()?;
                ensure_absent(self.db.as_ref(), checkpoint_key(record.id), &record.name).await?;
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
            CatalogMutation::ReplaceCheckpoint(record) => {
                record.validate()?;
                let old = self
                    .get_record::<CheckpointRecord>(checkpoint_key(record.id))
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(record.id.to_string()))?;
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

    async fn apply(
        &self,
        expected_generation: u64,
        mutation: CatalogMutation,
    ) -> Result<u64, CatalogError> {
        let _guard = self.lock.lock().await;
        self.apply_unlocked(expected_generation, mutation).await
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

    #[tokio::test]
    async fn records_are_independent_and_deletion_is_atomic_with_tombstone() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = SlateDbCatalog::open(Path::from("catalog"), store)
            .await
            .unwrap();
        let record = branch("main");
        catalog
            .apply(0, CatalogMutation::CreateBranch(record.clone()))
            .await
            .unwrap();
        assert_eq!(
            catalog.branch_by_name("main").await.unwrap(),
            Some(record.clone())
        );

        let deleted_at = catalog_timestamp(Utc::now());
        catalog
            .apply(
                1,
                CatalogMutation::DeleteBranch {
                    id: record.id,
                    name: record.name,
                    deleted_at,
                },
            )
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
}
