use super::catalog_timestamp;
use super::{Catalog, CatalogError, CatalogMutation, TombstoneKind, TombstoneRecord};
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointDeleteRequest {
    pub checkpoint_id: Uuid,
    pub expected_revision: u64,
    pub name: String,
}

#[derive(Clone)]
pub struct DeletionLifecycle {
    catalog: Arc<dyn Catalog>,
}

impl std::fmt::Debug for DeletionLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeletionLifecycle")
            .finish_non_exhaustive()
    }
}

impl DeletionLifecycle {
    pub(crate) fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    /// Logically delete one exact checkpoint incarnation.
    ///
    /// Existing leases retain the exact root independently. Ready descendants
    /// use their own roots and therefore never block this operation.
    pub async fn delete_checkpoint(
        &self,
        request: CheckpointDeleteRequest,
    ) -> Result<TombstoneRecord, DeletionLifecycleError> {
        if let Some(tombstone) = self.catalog.tombstone(request.checkpoint_id).await? {
            return exact_checkpoint_tombstone(&request, tombstone);
        }
        let checkpoint = self
            .catalog
            .checkpoint(request.checkpoint_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(request.checkpoint_id.to_string()))?;
        if checkpoint.name != request.name {
            return Err(CatalogError::NotFound(format!(
                "{} ({})",
                request.name, request.checkpoint_id
            ))
            .into());
        }
        let applied = self
            .catalog
            .apply(CatalogMutation::DeleteCheckpoint {
                id: request.checkpoint_id,
                expected_revision: request.expected_revision,
                name: request.name.clone(),
                deleted_at: catalog_timestamp(Utc::now()),
            })
            .await;
        if let Err(error) = applied {
            if let Some(tombstone) = self.catalog.tombstone(request.checkpoint_id).await? {
                return exact_checkpoint_tombstone(&request, tombstone);
            }
            return Err(error.into());
        }
        let tombstone = self
            .catalog
            .tombstone(request.checkpoint_id)
            .await?
            .ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "checkpoint {} deletion committed without a tombstone",
                    request.checkpoint_id
                ))
            })?;
        exact_checkpoint_tombstone(&request, tombstone)
    }
}

fn exact_checkpoint_tombstone(
    request: &CheckpointDeleteRequest,
    tombstone: TombstoneRecord,
) -> Result<TombstoneRecord, DeletionLifecycleError> {
    if tombstone.kind != TombstoneKind::Checkpoint
        || tombstone.name != request.name
        || tombstone.deleted_revision != Some(request.expected_revision)
    {
        return Err(CatalogError::OperationConflict(request.checkpoint_id.to_string()).into());
    }
    Ok(tombstone)
}

#[derive(Debug, thiserror::Error)]
pub enum DeletionLifecycleError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        BranchRecord, BranchState, Catalog, CatalogMutation, CheckpointRecord, DurableRoot,
        SlateDbCatalog, catalog_timestamp,
    };
    use object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;

    #[tokio::test]
    async fn exact_checkpoint_delete_recovers_lost_response_and_ignores_name_reuse() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("checkpoint-delete"), store)
                .await
                .unwrap(),
        );
        let now = catalog_timestamp(Utc::now());
        let branch = BranchRecord {
            id: Uuid::new_v4(),
            revision: 1,
            name: "branch".to_string(),
            state: BranchState::Ready,
            root: Some(DurableRoot {
                identity: "branch/root".to_string(),
                manifest_id: "root@1".to_string(),
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
        let checkpoint = CheckpointRecord {
            id: Uuid::new_v4(),
            revision: 1,
            branch_id: branch.id,
            name: "point".to_string(),
            root: DurableRoot {
                identity: "checkpoint/root".to_string(),
                manifest_id: "point@1".to_string(),
            },
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::CreateCheckpoint(checkpoint.clone()))
            .await
            .unwrap();
        let request = CheckpointDeleteRequest {
            checkpoint_id: checkpoint.id,
            expected_revision: checkpoint.revision,
            name: checkpoint.name.clone(),
        };
        catalog
            .apply(CatalogMutation::DeleteCheckpoint {
                id: checkpoint.id,
                expected_revision: checkpoint.revision,
                name: checkpoint.name.clone(),
                deleted_at: now,
            })
            .await
            .expect("apply deletion and intentionally discard its response");
        catalog
            .apply(CatalogMutation::CreateCheckpoint(CheckpointRecord {
                id: Uuid::new_v4(),
                name: checkpoint.name,
                ..checkpoint
            }))
            .await
            .unwrap();
        let lifecycle = DeletionLifecycle::new(catalog.clone());
        let mut conflicting_revision = request.clone();
        conflicting_revision.expected_revision += 1;
        assert!(matches!(
            lifecycle.delete_checkpoint(conflicting_revision).await,
            Err(DeletionLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));
        let tombstone = lifecycle.delete_checkpoint(request.clone()).await.unwrap();
        assert_eq!(tombstone.id, request.checkpoint_id);
        assert_eq!(
            lifecycle.delete_checkpoint(request).await.unwrap(),
            tombstone
        );
        catalog.close().await.unwrap();
    }
}
