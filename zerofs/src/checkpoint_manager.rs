use crate::db::SlateDbHandle;
use anyhow::{Result, anyhow};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use slatedb::admin::Admin;
use slatedb::config::{CheckpointOptions, CheckpointScope};
use slatedb::object_store::path::Path;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Seal the data-plane open segment and flush the metadata memtable under the
/// flush barrier, so a subsequent durable-scope checkpoint captures only
/// already-sealed state (never a FrameLoc whose segment is still in RAM).
pub type PreCheckpointFlush =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInfo {
    pub id: Uuid,
    pub name: String,
    pub created_at: u64,
}

#[derive(Debug, Clone)]
pub struct ExactCheckpointInfo {
    pub info: CheckpointInfo,
    pub source: zerofs::catalog::ImmutableCheckpoint,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

pub struct CheckpointManager {
    db_handle: SlateDbHandle,
    admin: Admin,
    path: Path,
    mutation_lock: Mutex<()>,
    /// Set once at bring-up (see [`PreCheckpointFlush`]); unset in tests, which
    /// checkpoint durable state as-is.
    pre_flush: Arc<OnceLock<PreCheckpointFlush>>,
}

impl CheckpointManager {
    pub fn new(
        db_handle: SlateDbHandle,
        path: Path,
        object_store: Arc<dyn ObjectStore>,
        wal_object_store: Option<Arc<dyn ObjectStore>>,
    ) -> Self {
        let mut admin_builder = slatedb::admin::AdminBuilder::new(path.clone(), object_store);
        if let Some(wal_store) = wal_object_store {
            admin_builder = admin_builder.with_wal_object_store(wal_store);
        }
        let admin = admin_builder.build();
        Self {
            db_handle,
            admin,
            path,
            mutation_lock: Mutex::new(()),
            pre_flush: Arc::new(OnceLock::new()),
        }
    }

    /// Install the pre-checkpoint seal+flush hook (first call wins). Wired at
    /// bring-up to the filesystem's flush coordinator.
    pub fn set_pre_flush(&self, hook: PreCheckpointFlush) {
        let _ = self.pre_flush.set(hook);
    }

    pub async fn create_checkpoint_exact(&self, name: &str) -> Result<ExactCheckpointInfo> {
        let db = match &self.db_handle {
            SlateDbHandle::ReadWrite(db) => db,
            SlateDbHandle::ReadOnly(_) => {
                return Err(anyhow!(
                    "Cannot create checkpoints in read-only mode. Start the server without --read-only or --checkpoint flags."
                ));
            }
        };

        zerofs::catalog::validate_resource_name(name)
            .map_err(|error| anyhow!("Invalid checkpoint name: {error}"))?;
        let _guard = self.mutation_lock.lock().await;

        let existing = self
            .admin
            .list_checkpoints(Some(name))
            .await
            .map_err(|e| anyhow!("Failed to list checkpoints: {}", e))?;

        match existing.as_slice() {
            [checkpoint] => {
                if checkpoint.expire_time.is_some() {
                    return Err(anyhow!(
                        "Physical checkpoint '{}' is expiring and cannot become a catalog root",
                        name
                    ));
                }
                return Ok(self.exact_info(name, checkpoint));
            }
            [] => {}
            _ => {
                return Err(anyhow!(
                    "Multiple physical checkpoints use the reserved public name '{}'",
                    name
                ));
            }
        }

        // Seal + flush under the barrier, then checkpoint `Durable` scope,
        // which captures only the already-durable manifest. `Scope::All` would
        // freeze the memtable itself and durably publish FrameLocs whose
        // segment is still the RAM open buffer — dangling pointers the
        // checkpoint would pin forever.
        if let Some(pre_flush) = self.pre_flush.get() {
            pre_flush()
                .await
                .map_err(|e| anyhow!("Failed to seal+flush before checkpoint: {}", e))?;
        }

        let result = db
            .create_checkpoint(
                CheckpointScope::Durable,
                &CheckpointOptions {
                    lifetime: None,
                    source: None,
                    name: Some(name.to_string()),
                },
            )
            .await
            .map_err(|e| anyhow!("Failed to create checkpoint: {}", e))?;

        let checkpoints = self
            .admin
            .list_checkpoints(Some(name))
            .await
            .map_err(|e| anyhow!("Failed to get checkpoint info: {}", e))?;

        let checkpoint = match checkpoints.as_slice() {
            [checkpoint] if checkpoint.id == result.id => checkpoint,
            [checkpoint] => {
                return Err(anyhow!(
                    "Created checkpoint UUID '{}' was replaced by '{}' under name '{}'",
                    result.id,
                    checkpoint.id,
                    name
                ));
            }
            [] => return Err(anyhow!("Created checkpoint not found")),
            _ => {
                return Err(anyhow!(
                    "Multiple physical checkpoints use the reserved public name '{}'",
                    name
                ));
            }
        };

        Ok(self.exact_info(name, checkpoint))
    }

    pub async fn list_checkpoints(&self) -> Result<Vec<CheckpointInfo>> {
        let checkpoints = self
            .admin
            .list_checkpoints(None)
            .await
            .map_err(|e| anyhow!("Failed to list checkpoints: {}", e))?;

        Ok(checkpoints
            .into_iter()
            .filter_map(|cp| {
                let name = cp.name.as_ref()?;
                if zerofs::catalog::validate_resource_name(name).is_err() {
                    return None;
                }
                Some(CheckpointInfo {
                    id: cp.id,
                    name: name.clone(),
                    created_at: cp.create_time.timestamp() as u64,
                })
            })
            .collect())
    }

    pub async fn delete_checkpoint(&self, name: &str) -> Result<()> {
        zerofs::catalog::validate_resource_name(name)
            .map_err(|error| anyhow!("Invalid checkpoint name: {error}"))?;
        let _guard = self.mutation_lock.lock().await;

        let checkpoints = self
            .admin
            .list_checkpoints(Some(name))
            .await
            .map_err(|e| anyhow!("Failed to list checkpoints: {}", e))?;

        let checkpoint = match checkpoints.as_slice() {
            [checkpoint] => checkpoint,
            [] => return Err(anyhow!("Checkpoint '{}' not found", name)),
            _ => {
                return Err(anyhow!(
                    "Multiple physical checkpoints use the reserved public name '{}'",
                    name
                ));
            }
        };

        self.delete_checkpoint_exact_unlocked(checkpoint.id, name)
            .await
    }

    /// Delete only one exact physical checkpoint. An absent exact UUID is an
    /// idempotent success and a same-name replacement is never targeted.
    #[allow(dead_code)] // Consumed by the authoritative checkpoint RPC workflow landing next.
    pub async fn delete_checkpoint_exact(&self, id: Uuid, name: &str) -> Result<()> {
        zerofs::catalog::validate_resource_name(name)
            .map_err(|error| anyhow!("Invalid checkpoint name: {error}"))?;
        let _guard = self.mutation_lock.lock().await;
        self.delete_checkpoint_exact_unlocked(id, name).await
    }

    async fn delete_checkpoint_exact_unlocked(&self, id: Uuid, name: &str) -> Result<()> {
        let checkpoint = self
            .admin
            .list_checkpoints(None)
            .await
            .map_err(|e| anyhow!("Failed to list checkpoints: {}", e))?
            .into_iter()
            .find(|checkpoint| checkpoint.id == id);
        let Some(checkpoint) = checkpoint else {
            return Ok(());
        };
        if checkpoint.name.as_deref() != Some(name) {
            return Err(anyhow!(
                "Checkpoint UUID '{}' does not have expected name '{}'",
                id,
                name
            ));
        }

        self.admin
            .delete_checkpoint(id)
            .await
            .map_err(|e| anyhow!("Failed to delete checkpoint: {}", e))?;

        Ok(())
    }

    pub async fn get_checkpoint_info(&self, name: &str) -> Result<Option<CheckpointInfo>> {
        zerofs::catalog::validate_resource_name(name)
            .map_err(|error| anyhow!("Invalid checkpoint name: {error}"))?;

        let checkpoints = self
            .admin
            .list_checkpoints(Some(name))
            .await
            .map_err(|e| anyhow!("Failed to list checkpoints: {}", e))?;

        match checkpoints.as_slice() {
            [checkpoint] => Ok(Some(CheckpointInfo {
                id: checkpoint.id,
                name: name.to_string(),
                created_at: checkpoint.create_time.timestamp() as u64,
            })),
            [] => Ok(None),
            _ => Err(anyhow!(
                "Multiple physical checkpoints use the reserved public name '{}'",
                name
            )),
        }
    }

    fn exact_info(&self, name: &str, checkpoint: &slatedb::Checkpoint) -> ExactCheckpointInfo {
        ExactCheckpointInfo {
            info: CheckpointInfo {
                id: checkpoint.id,
                name: name.to_string(),
                created_at: checkpoint.create_time.timestamp() as u64,
            },
            source: zerofs::catalog::ImmutableCheckpoint {
                database_path: self.path.clone(),
                checkpoint_id: checkpoint.id,
                manifest_id: checkpoint.manifest_id,
            },
            created_at: zerofs::catalog::catalog_timestamp(checkpoint.create_time),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::config::{CheckpointOptions, CheckpointScope};
    use slatedb::object_store::memory::InMemory;

    async fn manager() -> (Arc<slatedb::Db>, Arc<dyn ObjectStore>, CheckpointManager) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("checkpoint-manager-tests");
        let db = Arc::new(
            slatedb::Db::open(path.clone(), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let manager = CheckpointManager::new(
            SlateDbHandle::ReadWrite(Arc::clone(&db)),
            path,
            Arc::clone(&store),
            None,
        );
        (db, store, manager)
    }

    #[tokio::test]
    async fn exact_create_is_idempotent_and_returns_physical_identity() {
        let (db, _store, manager) = manager().await;
        db.put(b"key", b"value").await.unwrap();

        let first = manager.create_checkpoint_exact("snapshot").await.unwrap();
        let retry = manager.create_checkpoint_exact("snapshot").await.unwrap();

        assert_eq!(retry.info.id, first.info.id);
        assert_eq!(retry.source, first.source);
        assert_eq!(retry.created_at, first.created_at);
        assert_eq!(first.source.checkpoint_id, first.info.id);
        assert_eq!(
            first.source.database_path,
            Path::from("checkpoint-manager-tests")
        );
    }

    #[tokio::test]
    async fn internal_names_are_rejected_and_hidden() {
        let (db, _store, manager) = manager().await;
        let error = manager
            .create_checkpoint_exact("__zerofs_internal")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("reserved"));

        db.create_checkpoint(
            CheckpointScope::All,
            &CheckpointOptions {
                lifetime: None,
                source: None,
                name: Some("__zerofs_internal".to_string()),
            },
        )
        .await
        .unwrap();
        assert!(manager.list_checkpoints().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn existing_expiring_checkpoint_cannot_be_adopted() {
        let (db, _store, manager) = manager().await;
        db.create_checkpoint(
            CheckpointScope::All,
            &CheckpointOptions {
                lifetime: Some(std::time::Duration::from_secs(3600)),
                source: None,
                name: Some("snapshot".to_string()),
            },
        )
        .await
        .unwrap();

        let error = manager
            .create_checkpoint_exact("snapshot")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("expiring"));
    }

    #[tokio::test]
    async fn exact_delete_retry_preserves_same_name_replacement() {
        let (_db, _store, manager) = manager().await;
        let first = manager.create_checkpoint_exact("snapshot").await.unwrap();
        manager
            .delete_checkpoint_exact(first.info.id, "snapshot")
            .await
            .unwrap();
        let replacement = manager.create_checkpoint_exact("snapshot").await.unwrap();
        assert_ne!(replacement.info.id, first.info.id);

        manager
            .delete_checkpoint_exact(first.info.id, "snapshot")
            .await
            .unwrap();

        assert_eq!(
            manager
                .get_checkpoint_info("snapshot")
                .await
                .unwrap()
                .unwrap()
                .id,
            replacement.info.id
        );
    }
}
