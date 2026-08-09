use super::{
    CATALOG_PROJECTION_SCHEMA_VERSION, CatalogError, CatalogProjection, CatalogSnapshot,
    CustomerCatalogListRequest, CustomerCatalogPage, CustomerCatalogRecord, CustomerMetadata,
    CustomerResourceKind, validate_metadata,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Default, Serialize, Deserialize)]
struct ProjectionDocument {
    schema_version: u32,
    #[serde(default)]
    volumes: BTreeMap<Uuid, ProjectedVolume>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ProjectedVolume {
    observed_generation: u64,
    #[serde(default)]
    records: BTreeMap<Uuid, CustomerCatalogRecord>,
}

/// Local/testing implementation of the same customer projection stored in
/// PostgreSQL in production. It never contains durable roots or manifests.
#[derive(Debug)]
pub struct JsonCatalogProjection {
    path: PathBuf,
    lock: Mutex<()>,
}

impl JsonCatalogProjection {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            lock: Mutex::new(()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    async fn load_unlocked(&self) -> Result<ProjectionDocument, CatalogError> {
        match tokio::fs::read(&self.path).await {
            Ok(bytes) => {
                let document = serde_json::from_slice::<ProjectionDocument>(&bytes)?;
                if document.schema_version != CATALOG_PROJECTION_SCHEMA_VERSION {
                    return Err(CatalogError::Corrupt(format!(
                        "unsupported JSON projection schema version {}",
                        document.schema_version
                    )));
                }
                Ok(document)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(ProjectionDocument {
                schema_version: CATALOG_PROJECTION_SCHEMA_VERSION,
                ..ProjectionDocument::default()
            }),
            Err(error) => Err(error.into()),
        }
    }

    async fn persist_unlocked(&self, document: &ProjectionDocument) -> Result<(), CatalogError> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent).await?;
        let file_name = self
            .path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("catalog-projection.json");
        let temporary = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
        let bytes = serde_json::to_vec_pretty(document)?;
        let result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .await?;
            tokio::io::AsyncWriteExt::write_all(&mut file, &bytes).await?;
            file.sync_all().await?;
            drop(file);
            tokio::fs::rename(&temporary, &self.path).await?;
            let parent = parent.to_path_buf();
            tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all())
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))??;
            Ok::<(), std::io::Error>(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result.map_err(Into::into)
    }
}

#[async_trait]
impl CatalogProjection for JsonCatalogProjection {
    async fn reconcile(
        &self,
        volume_id: Uuid,
        snapshot: &CatalogSnapshot,
    ) -> Result<(), CatalogError> {
        validate_volume_id(volume_id)?;
        snapshot.validate()?;
        let _guard = self.lock.lock().await;
        let mut document = self.load_unlocked().await?;
        let volume = document.volumes.entry(volume_id).or_default();
        if snapshot.generation <= volume.observed_generation {
            return Ok(());
        }
        reconcile_volume(volume_id, volume, snapshot);
        self.persist_unlocked(&document).await
    }

    async fn record(
        &self,
        volume_id: Uuid,
        resource_id: Uuid,
    ) -> Result<Option<CustomerCatalogRecord>, CatalogError> {
        validate_volume_id(volume_id)?;
        let _guard = self.lock.lock().await;
        Ok(self
            .load_unlocked()
            .await?
            .volumes
            .get(&volume_id)
            .and_then(|volume| volume.records.get(&resource_id))
            .cloned())
    }

    async fn list(
        &self,
        volume_id: Uuid,
        request: CustomerCatalogListRequest,
    ) -> Result<CustomerCatalogPage, CatalogError> {
        validate_volume_id(volume_id)?;
        request.validate()?;
        let _guard = self.lock.lock().await;
        let document = self.load_unlocked().await?;
        let Some(volume) = document.volumes.get(&volume_id) else {
            return Ok(CustomerCatalogPage {
                records: Vec::new(),
                next_after: None,
            });
        };
        let lower = request.after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut records = volume
            .records
            .range((lower, Bound::Unbounded))
            .map(|(_, record)| record)
            .filter(|record| request.kind.is_none_or(|kind| record.kind == kind))
            .filter(|record| {
                request
                    .parent_id
                    .is_none_or(|parent_id| record.parent_id == Some(parent_id))
            })
            .filter(|record| {
                request
                    .state
                    .as_deref()
                    .is_none_or(|state| record.state == state)
            })
            .take(request.limit + 1)
            .cloned()
            .collect::<Vec<_>>();
        let next_after =
            (records.len() > request.limit).then(|| records[request.limit - 1].resource_id);
        records.truncate(request.limit);
        Ok(CustomerCatalogPage {
            records,
            next_after,
        })
    }

    async fn set_customer_metadata(
        &self,
        volume_id: Uuid,
        resource_id: Uuid,
        metadata: CustomerMetadata,
    ) -> Result<(), CatalogError> {
        validate_volume_id(volume_id)?;
        validate_metadata(&metadata)?;
        let _guard = self.lock.lock().await;
        let mut document = self.load_unlocked().await?;
        let record = document
            .volumes
            .get_mut(&volume_id)
            .and_then(|volume| volume.records.get_mut(&resource_id))
            .ok_or_else(|| CatalogError::NotFound(resource_id.to_string()))?;
        record.customer_metadata = metadata;
        self.persist_unlocked(&document).await
    }
}

fn reconcile_volume(volume_id: Uuid, volume: &mut ProjectedVolume, snapshot: &CatalogSnapshot) {
    let mut present = BTreeSet::new();
    for branch in snapshot.branches.values() {
        present.insert(branch.id);
        let metadata = volume
            .records
            .get(&branch.id)
            .map(|record| record.customer_metadata.clone())
            .unwrap_or_default();
        volume.records.insert(
            branch.id,
            CustomerCatalogRecord {
                volume_id,
                resource_id: branch.id,
                kind: CustomerResourceKind::Branch,
                name: branch.name.clone(),
                state: branch.state.as_str().to_string(),
                parent_id: branch.parent_id,
                origin_checkpoint_id: branch.origin_checkpoint_id,
                observed_generation: snapshot.generation,
                created_at: branch.created_at,
                updated_at: branch.updated_at,
                deleted_at: None,
                customer_metadata: metadata,
            },
        );
    }
    for checkpoint in snapshot.checkpoints.values() {
        present.insert(checkpoint.id);
        let metadata = volume
            .records
            .get(&checkpoint.id)
            .map(|record| record.customer_metadata.clone())
            .unwrap_or_default();
        volume.records.insert(
            checkpoint.id,
            CustomerCatalogRecord {
                volume_id,
                resource_id: checkpoint.id,
                kind: CustomerResourceKind::Checkpoint,
                name: checkpoint.name.clone(),
                state: "ready".to_string(),
                parent_id: Some(checkpoint.branch_id),
                origin_checkpoint_id: None,
                observed_generation: snapshot.generation,
                created_at: checkpoint.created_at,
                updated_at: checkpoint.updated_at,
                deleted_at: None,
                customer_metadata: metadata,
            },
        );
    }
    for tombstone in snapshot.tombstones.values() {
        present.insert(tombstone.id);
        let previous = volume.records.get(&tombstone.id);
        let metadata = previous
            .map(|record| record.customer_metadata.clone())
            .unwrap_or_default();
        volume.records.insert(
            tombstone.id,
            CustomerCatalogRecord {
                volume_id,
                resource_id: tombstone.id,
                kind: match tombstone.kind {
                    super::TombstoneKind::Branch => CustomerResourceKind::Branch,
                    super::TombstoneKind::Checkpoint => CustomerResourceKind::Checkpoint,
                },
                name: tombstone.name.clone(),
                state: "deleted".to_string(),
                parent_id: tombstone.parent_id,
                origin_checkpoint_id: tombstone.origin_checkpoint_id,
                observed_generation: snapshot.generation,
                created_at: tombstone.created_at,
                updated_at: tombstone.deleted_at,
                deleted_at: Some(tombstone.deleted_at),
                customer_metadata: metadata,
            },
        );
    }
    for record in volume.records.values_mut() {
        if !present.contains(&record.resource_id) {
            record.state = "absent".to_string();
            record.observed_generation = snapshot.generation;
        }
    }
    volume.observed_generation = snapshot.generation;
}

fn validate_volume_id(volume_id: Uuid) -> Result<(), CatalogError> {
    if volume_id.is_nil() {
        return Err(CatalogError::Invalid(
            "volume UUID cannot be nil".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        BranchRecord, BranchState, CheckpointRecord, DurableRoot, MAX_CUSTOMER_CATALOG_PAGE_SIZE,
        catalog_timestamp,
    };
    use chrono::Utc;
    use serde_json::Value;

    #[tokio::test]
    async fn reconciles_without_roots_and_preserves_customer_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let projection = JsonCatalogProjection::new(directory.path().join("projection.json"));
        let volume_id = Uuid::new_v4();
        let branch_id = Uuid::new_v4();
        let historical_parent = Uuid::new_v4();
        let now = catalog_timestamp(Utc::now());
        let mut snapshot = CatalogSnapshot {
            generation: 1,
            ..CatalogSnapshot::default()
        };
        snapshot.branches.insert(
            branch_id,
            BranchRecord {
                id: branch_id,
                revision: 1,
                name: "main".to_string(),
                state: BranchState::Ready,
                root: Some(DurableRoot {
                    identity: "secret-root".to_string(),
                    manifest_id: "secret-manifest".to_string(),
                }),
                parent_id: Some(historical_parent),
                origin_checkpoint_id: None,
                created_at: now,
                updated_at: now,
            },
        );
        projection.reconcile(volume_id, &snapshot).await.unwrap();

        let mut metadata = CustomerMetadata::new();
        metadata.insert("project".to_string(), Value::String("alpha".to_string()));
        projection
            .set_customer_metadata(volume_id, branch_id, metadata.clone())
            .await
            .unwrap();
        snapshot.generation = 2;
        snapshot.branches.get_mut(&branch_id).unwrap().parent_id = None;
        projection.reconcile(volume_id, &snapshot).await.unwrap();

        let record = projection
            .record(volume_id, branch_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.customer_metadata, metadata);
        assert_eq!(record.parent_id, None);

        let deleted_at = catalog_timestamp(Utc::now());
        snapshot.generation = 3;
        snapshot.branches.clear();
        snapshot.tombstones.insert(
            branch_id,
            crate::catalog::TombstoneRecord {
                id: branch_id,
                kind: crate::catalog::TombstoneKind::Branch,
                name: "main".to_string(),
                parent_id: Some(historical_parent),
                origin_checkpoint_id: None,
                created_at: now,
                deleted_revision: Some(1),
                deletion_operation_id: None,
                deleted_generation: 3,
                deleted_at,
            },
        );
        projection.reconcile(volume_id, &snapshot).await.unwrap();
        let deleted = projection
            .record(volume_id, branch_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(deleted.state, "deleted");
        assert_eq!(deleted.parent_id, Some(historical_parent));
        assert_eq!(deleted.created_at, now);
        assert_eq!(deleted.customer_metadata, metadata);

        snapshot.generation = 4;
        snapshot.tombstones.get_mut(&branch_id).unwrap().parent_id = None;
        projection.reconcile(volume_id, &snapshot).await.unwrap();
        assert_eq!(
            projection
                .record(volume_id, branch_id)
                .await
                .unwrap()
                .unwrap()
                .parent_id,
            None
        );
        snapshot.generation = 5;
        snapshot.tombstones.clear();
        snapshot.retired_catalog_ids.insert(
            branch_id,
            crate::catalog::RetiredCatalogId {
                id: branch_id,
                kind: crate::catalog::RetiredCatalogKind::Branch,
            },
        );
        projection.reconcile(volume_id, &snapshot).await.unwrap();
        let compacted = projection
            .record(volume_id, branch_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(compacted.state, "absent");
        assert_eq!(compacted.customer_metadata, metadata);
        let json = tokio::fs::read_to_string(projection.path()).await.unwrap();
        assert!(!json.contains("secret-root"));
        assert!(!json.contains("secret-manifest"));
    }

    #[tokio::test]
    async fn lists_bounded_uuid_pages_with_identical_kind_filtering() {
        let directory = tempfile::tempdir().unwrap();
        let projection = JsonCatalogProjection::new(directory.path().join("projection.json"));
        let volume_id = Uuid::new_v4();
        let first_branch_id = Uuid::from_u128(1);
        let checkpoint_id = Uuid::from_u128(2);
        let second_branch_id = Uuid::from_u128(3);
        let now = catalog_timestamp(Utc::now());
        let mut snapshot = CatalogSnapshot {
            generation: 1,
            ..CatalogSnapshot::default()
        };
        for (id, name) in [(first_branch_id, "first"), (second_branch_id, "second")] {
            snapshot.branches.insert(
                id,
                BranchRecord {
                    id,
                    revision: 1,
                    name: name.to_string(),
                    state: BranchState::Ready,
                    root: Some(DurableRoot {
                        identity: format!("branches/{id}"),
                        manifest_id: format!("manifest-{id}"),
                    }),
                    parent_id: None,
                    origin_checkpoint_id: None,
                    created_at: now,
                    updated_at: now,
                },
            );
        }
        snapshot.checkpoints.insert(
            checkpoint_id,
            CheckpointRecord {
                id: checkpoint_id,
                revision: 1,
                branch_id: first_branch_id,
                name: "snapshot".to_string(),
                root: DurableRoot {
                    identity: "branches/first".to_string(),
                    manifest_id: "checkpoint".to_string(),
                },
                created_at: now,
                updated_at: now,
            },
        );
        projection.reconcile(volume_id, &snapshot).await.unwrap();

        let first = projection
            .list(
                volume_id,
                CustomerCatalogListRequest {
                    kind: None,
                    parent_id: None,
                    state: None,
                    after: None,
                    limit: 2,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            first
                .records
                .iter()
                .map(|record| record.resource_id)
                .collect::<Vec<_>>(),
            vec![first_branch_id, checkpoint_id]
        );
        assert_eq!(first.next_after, Some(checkpoint_id));

        let second = projection
            .list(
                volume_id,
                CustomerCatalogListRequest {
                    kind: None,
                    parent_id: None,
                    state: None,
                    after: first.next_after,
                    limit: 2,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            second
                .records
                .iter()
                .map(|record| record.resource_id)
                .collect::<Vec<_>>(),
            vec![second_branch_id]
        );
        assert_eq!(second.next_after, None);

        let branches = projection
            .list(
                volume_id,
                CustomerCatalogListRequest {
                    kind: Some(CustomerResourceKind::Branch),
                    parent_id: None,
                    state: None,
                    after: None,
                    limit: 2,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            branches
                .records
                .iter()
                .map(|record| record.resource_id)
                .collect::<Vec<_>>(),
            vec![first_branch_id, second_branch_id]
        );
        assert_eq!(branches.next_after, None);

        let checkpoints = projection
            .list(
                volume_id,
                CustomerCatalogListRequest {
                    kind: Some(CustomerResourceKind::Checkpoint),
                    parent_id: Some(first_branch_id),
                    state: Some("ready".to_string()),
                    after: None,
                    limit: 2,
                },
            )
            .await
            .unwrap();
        assert_eq!(checkpoints.records.len(), 1);
        assert_eq!(checkpoints.records[0].resource_id, checkpoint_id);
        assert!(
            projection
                .list(
                    volume_id,
                    CustomerCatalogListRequest {
                        kind: Some(CustomerResourceKind::Checkpoint),
                        parent_id: Some(second_branch_id),
                        state: Some("ready".to_string()),
                        after: None,
                        limit: 2,
                    },
                )
                .await
                .unwrap()
                .records
                .is_empty()
        );

        for request in [
            CustomerCatalogListRequest {
                kind: None,
                parent_id: None,
                state: None,
                after: None,
                limit: 0,
            },
            CustomerCatalogListRequest {
                kind: None,
                parent_id: None,
                state: None,
                after: None,
                limit: MAX_CUSTOMER_CATALOG_PAGE_SIZE + 1,
            },
            CustomerCatalogListRequest {
                kind: None,
                parent_id: None,
                state: None,
                after: Some(Uuid::nil()),
                limit: 1,
            },
        ] {
            assert!(projection.list(volume_id, request).await.is_err());
        }
    }
}
