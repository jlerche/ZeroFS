use super::{
    BranchCreateOperation, BranchCreatePhase, BranchRecord, BranchState, Catalog, CatalogError,
    CatalogMutation, ImmutableCheckpoint, RootStoreError, SlateDbRootStore, catalog_timestamp,
};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchCreateRequest {
    pub operation_id: Uuid,
    pub destination_id: Uuid,
    pub destination_name: String,
    pub source: ImmutableCheckpoint,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchCreateFromCheckpointNameRequest {
    pub operation_id: Uuid,
    pub destination_id: Uuid,
    pub destination_name: String,
    pub source_branch_id: Uuid,
    pub source_checkpoint_name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct BranchLifecycle {
    catalog: Arc<dyn Catalog>,
    roots: SlateDbRootStore,
}

impl std::fmt::Debug for BranchLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BranchLifecycle")
            .field("roots", &self.roots)
            .finish_non_exhaustive()
    }
}

impl BranchLifecycle {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, roots: SlateDbRootStore) -> Self {
        Self { catalog, roots }
    }

    pub fn leases(&self) -> super::LeaseLifecycle {
        super::LeaseLifecycle::new(Arc::clone(&self.catalog), self.roots.clone())
    }

    pub fn deletions(&self) -> super::DeletionLifecycle {
        super::DeletionLifecycle::new(Arc::clone(&self.catalog))
    }

    pub fn root_captures(&self) -> super::RootCaptureLifecycle {
        super::RootCaptureLifecycle::new(Arc::clone(&self.catalog), self.roots.clone())
    }

    /// Bind authenticated segment-pool reservations to this catalog's exact
    /// branch incarnations. The resulting records are local SlateDB authority,
    /// not part of the PostgreSQL/JSON customer projection.
    pub fn private_epochs(
        &self,
        segment_pool: Arc<dyn object_store::ObjectStore>,
        authority: crate::segment_store::SegmentPoolAuthority,
    ) -> super::PrivateEpochLifecycle {
        super::PrivateEpochLifecycle::new(Arc::clone(&self.catalog), segment_pool, authority)
    }

    /// Resolve a checkpoint name once to its stable catalog UUID and exact
    /// SlateDB checkpoint/manifest identity, then use the same creation
    /// primitive as an already-resolved request.
    pub async fn create_from_checkpoint_name(
        &self,
        request: BranchCreateFromCheckpointNameRequest,
    ) -> Result<BranchRecord, BranchLifecycleError> {
        if let Some(existing) = self
            .catalog
            .branch_create_operation(request.operation_id)
            .await?
        {
            if existing.destination_id != request.destination_id
                || existing.destination_name != request.destination_name
                || existing.parent_id != Some(request.source_branch_id)
                || existing.created_at != request.created_at
            {
                return Err(
                    CatalogError::OperationConflict(request.operation_id.to_string()).into(),
                );
            }
            let source = ImmutableCheckpoint::from_durable_root(&existing.source_root)?;
            if source.checkpoint_id != existing.source_checkpoint_id {
                return Err(BranchLifecycleError::SourceRootConflict(
                    existing.source_checkpoint_id,
                ));
            }
            return self
                .create_from_checkpoint(BranchCreateRequest {
                    operation_id: request.operation_id,
                    destination_id: request.destination_id,
                    destination_name: request.destination_name,
                    source,
                    created_at: request.created_at,
                })
                .await;
        }
        let source_record = self
            .catalog
            .checkpoint_by_name(request.source_branch_id, &request.source_checkpoint_name)
            .await?
            .ok_or_else(|| {
                CatalogError::NotFound(format!(
                    "checkpoint {}/{}",
                    request.source_branch_id, request.source_checkpoint_name
                ))
            })?;
        let source = ImmutableCheckpoint::from_durable_root(&source_record.root)?;
        if source.checkpoint_id != source_record.id {
            return Err(BranchLifecycleError::SourceRootConflict(source_record.id));
        }
        self.create_from_checkpoint(BranchCreateRequest {
            operation_id: request.operation_id,
            destination_id: request.destination_id,
            destination_name: request.destination_name,
            source,
            created_at: request.created_at,
        })
        .await
    }

    /// Create or resume one exact checkpoint-based branch operation.
    ///
    /// The catalog reserves the source before clone I/O. The resulting root is
    /// authenticated both before it becomes an incomplete GC root and directly
    /// before the atomic `Creating` to `Ready` publication.
    pub async fn create_from_checkpoint(
        &self,
        request: BranchCreateRequest,
    ) -> Result<BranchRecord, BranchLifecycleError> {
        let existing = self
            .catalog
            .branch_create_operation(request.operation_id)
            .await?;
        let operation = if let Some(existing) = existing {
            if existing.destination_id != request.destination_id
                || existing.destination_name != request.destination_name
                || existing.source_checkpoint_id != request.source.checkpoint_id
                || existing.source_root != request.source.durable_root()
                || existing.created_at != request.created_at
            {
                return Err(
                    CatalogError::OperationConflict(request.operation_id.to_string()).into(),
                );
            }
            if existing.phase == BranchCreatePhase::Published {
                return self.ready_branch(existing.destination_id).await;
            }
            existing
        } else {
            let source_record = self
                .catalog
                .checkpoint(request.source.checkpoint_id)
                .await?
                .ok_or_else(|| CatalogError::NotFound(request.source.checkpoint_id.to_string()))?;
            if source_record.root != request.source.durable_root() {
                return Err(BranchLifecycleError::SourceRootConflict(source_record.id));
            }
            let branch = BranchRecord {
                id: request.destination_id,
                revision: 1,
                name: request.destination_name.clone(),
                state: BranchState::Creating,
                root: None,
                parent_id: Some(source_record.branch_id),
                origin_checkpoint_id: Some(source_record.id),
                created_at: request.created_at,
                updated_at: request.created_at,
            };
            let reservation = BranchCreateOperation {
                id: request.operation_id,
                revision: 1,
                destination_id: request.destination_id,
                destination_name: request.destination_name.clone(),
                source_checkpoint_id: source_record.id,
                source_root: source_record.root,
                parent_id: Some(source_record.branch_id),
                phase: BranchCreatePhase::Reserved,
                destination_root: None,
                created_at: request.created_at,
                updated_at: request.created_at,
            };
            self.catalog
                .apply(CatalogMutation::ReserveBranchCreate {
                    branch,
                    operation: Box::new(reservation),
                })
                .await?;
            self.operation(request.operation_id).await?
        };
        if operation.phase == BranchCreatePhase::Published {
            return self.ready_branch(operation.destination_id).await;
        }
        let destination_root = match operation.phase {
            BranchCreatePhase::Reserved => {
                self.roots
                    .create_from_checkpoint(
                        request.operation_id,
                        request.destination_id,
                        &request.source,
                    )
                    .await?
            }
            BranchCreatePhase::RootCreated => {
                operation.destination_root.clone().ok_or_else(|| {
                    BranchLifecycleError::Invariant(
                        "root-created operation is missing its destination root".to_string(),
                    )
                })?
            }
            BranchCreatePhase::Published => unreachable!("handled above"),
        };
        self.roots.verify(&destination_root).await?;
        self.catalog
            .apply(CatalogMutation::RecordBranchCreateRoot {
                operation_id: request.operation_id,
                expected_revision: operation.revision,
                destination_root: destination_root.clone(),
                updated_at: transition_time(request.created_at),
            })
            .await?;

        let operation = self.operation(request.operation_id).await?;
        if operation.phase == BranchCreatePhase::Published {
            return self.ready_branch(operation.destination_id).await;
        }
        if operation.phase != BranchCreatePhase::RootCreated
            || operation.destination_root.as_ref() != Some(&destination_root)
        {
            return Err(BranchLifecycleError::Invariant(format!(
                "operation {} did not retain its authenticated destination root",
                operation.id
            )));
        }
        self.roots.verify(&destination_root).await?;
        self.catalog
            .apply(CatalogMutation::PublishBranchCreate {
                operation_id: request.operation_id,
                expected_revision: operation.revision,
                updated_at: transition_time(request.created_at),
            })
            .await?;
        self.ready_branch(request.destination_id).await
    }

    async fn operation(
        &self,
        operation_id: Uuid,
    ) -> Result<BranchCreateOperation, BranchLifecycleError> {
        self.catalog
            .branch_create_operation(operation_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(operation_id.to_string()).into())
    }

    async fn ready_branch(
        &self,
        destination_id: Uuid,
    ) -> Result<BranchRecord, BranchLifecycleError> {
        let branch = self
            .catalog
            .branch(destination_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(destination_id.to_string()))?;
        if branch.state != BranchState::Ready || branch.root.is_none() {
            return Err(BranchLifecycleError::Invariant(format!(
                "published operation destination {destination_id} is not ready"
            )));
        }
        Ok(branch)
    }
}

fn transition_time(created_at: DateTime<Utc>) -> DateTime<Utc> {
    std::cmp::max(created_at, catalog_timestamp(Utc::now()))
}

#[derive(Debug, thiserror::Error)]
pub enum BranchLifecycleError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    RootStore(#[from] RootStoreError),
    #[error("source checkpoint {0} root does not match its exact SlateDB checkpoint identity")]
    SourceRootConflict(Uuid),
    #[error("branch lifecycle invariant failed: {0}")]
    Invariant(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        CatalogMutation, CheckpointDeleteRequest, CheckpointRecord, DurableRoot, LeaseAccessMode,
        LeaseAcquireRequest, SlateDbCatalog, catalog_timestamp,
    };
    use slatedb::Db;
    use slatedb::admin::AdminBuilder;
    use slatedb::config::{CheckpointOptions, CheckpointScope};
    use slatedb::object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;

    #[tokio::test]
    async fn named_concurrent_creates_publish_once_and_ignore_source_name_reuse_on_retry() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("lifecycle/catalog"), Arc::clone(&store))
                .await
                .unwrap(),
        );
        let now = catalog_timestamp(Utc::now());
        let parent = BranchRecord {
            id: Uuid::new_v4(),
            revision: 1,
            name: "parent".to_string(),
            state: BranchState::Ready,
            root: Some(DurableRoot {
                identity: "lifecycle/parent-root".to_string(),
                manifest_id: "parent-manifest".to_string(),
            }),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::CreateBranch(parent.clone()))
            .await
            .unwrap();

        let source_path = Path::from("lifecycle/source");
        let source_db = Db::open(source_path.clone(), Arc::clone(&store))
            .await
            .unwrap();
        source_db.put(b"key", b"value").await.unwrap();
        let source_checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        let resumable_checkpoint = source_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        source_db.close().await.unwrap();
        let source = ImmutableCheckpoint {
            database_path: source_path.clone(),
            checkpoint_id: source_checkpoint.id,
            manifest_id: source_checkpoint.manifest_id,
        };
        let source_record = CheckpointRecord {
            id: source_checkpoint.id,
            revision: 1,
            branch_id: parent.id,
            name: "source".to_string(),
            root: source.durable_root(),
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::CreateCheckpoint(source_record.clone()))
            .await
            .unwrap();
        let resumable_source = ImmutableCheckpoint {
            database_path: source_path.clone(),
            checkpoint_id: resumable_checkpoint.id,
            manifest_id: resumable_checkpoint.manifest_id,
        };
        let resumable_record = CheckpointRecord {
            id: resumable_checkpoint.id,
            revision: 1,
            branch_id: parent.id,
            name: "resumable".to_string(),
            root: resumable_source.durable_root(),
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::CreateCheckpoint(resumable_record.clone()))
            .await
            .unwrap();

        let root_store =
            SlateDbRootStore::new(Arc::clone(&store), Path::from("lifecycle/branches"));
        let lifecycle = BranchLifecycle::new(catalog.clone(), root_store.clone());
        let operation_id = Uuid::new_v4();
        let destination_id = Uuid::new_v4();
        let named_request = BranchCreateFromCheckpointNameRequest {
            operation_id,
            destination_id,
            destination_name: "child".to_string(),
            source_branch_id: parent.id,
            source_checkpoint_name: source_record.name.clone(),
            created_at: now,
        };
        let (left, right) = tokio::join!(
            lifecycle.create_from_checkpoint_name(named_request.clone()),
            lifecycle.create_from_checkpoint_name(named_request.clone())
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(left, right);
        assert_eq!(left.state, BranchState::Ready);
        root_store
            .verify(left.root.as_ref().unwrap())
            .await
            .unwrap();
        let snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(snapshot.generation, 6);
        assert_eq!(
            snapshot.branch_create_operations[&operation_id].phase,
            BranchCreatePhase::Published
        );

        let checkpoint_lease_request = LeaseAcquireRequest {
            lease_id: Uuid::new_v4(),
            renewal_token: Uuid::new_v4(),
            subject_id: source_record.id,
            access_mode: LeaseAccessMode::Read,
            duration: chrono::Duration::minutes(2),
        };
        let checkpoint_grant = lifecycle
            .leases()
            .acquire_checkpoint_by_name(
                parent.id,
                &source_record.name,
                checkpoint_lease_request.clone(),
            )
            .await
            .unwrap();
        let mut invalid_writer = checkpoint_lease_request.clone();
        invalid_writer.lease_id = Uuid::new_v4();
        invalid_writer.renewal_token = Uuid::new_v4();
        invalid_writer.access_mode = LeaseAccessMode::Write;
        assert!(matches!(
            lifecycle
                .leases()
                .acquire_checkpoint_by_name(parent.id, &source_record.name, invalid_writer)
                .await,
            Err(crate::catalog::LeaseLifecycleError::Catalog(
                CatalogError::Invalid(_)
            ))
        ));

        let delete_request = CheckpointDeleteRequest {
            checkpoint_id: source_record.id,
            expected_revision: source_record.revision,
            name: source_record.name.clone(),
        };
        lifecycle
            .deletions()
            .delete_checkpoint(delete_request.clone())
            .await
            .unwrap();
        assert_eq!(
            lifecycle
                .leases()
                .acquire_checkpoint_by_name(
                    parent.id,
                    &source_record.name,
                    checkpoint_lease_request,
                )
                .await
                .expect("an acquired checkpoint lease must survive logical deletion"),
            checkpoint_grant
        );
        lifecycle
            .leases()
            .release(
                checkpoint_grant.lease.id,
                checkpoint_grant.lease.revision,
                checkpoint_grant.renewal_token,
            )
            .await
            .unwrap();
        AdminBuilder::new(source_path, Arc::clone(&store))
            .build()
            .delete_checkpoint(source_checkpoint.id)
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::CreateCheckpoint(CheckpointRecord {
                id: Uuid::new_v4(),
                revision: 1,
                branch_id: parent.id,
                name: source_record.name,
                root: DurableRoot {
                    identity: "lifecycle/replacement-source".to_string(),
                    manifest_id: format!("{}@1", Uuid::new_v4()),
                },
                created_at: now,
                updated_at: now,
            }))
            .await
            .unwrap();
        lifecycle
            .deletions()
            .delete_checkpoint(delete_request)
            .await
            .expect("an exact checkpoint deletion retry must ignore name reuse");
        assert_eq!(
            lifecycle
                .create_from_checkpoint_name(named_request)
                .await
                .expect("a published named retry must ignore source deletion and name reuse"),
            left
        );
        let resume_request = BranchCreateRequest {
            operation_id: Uuid::new_v4(),
            destination_id: Uuid::new_v4(),
            destination_name: "resumed-child".to_string(),
            source: resumable_source,
            created_at: now,
        };
        catalog
            .apply(CatalogMutation::ReserveBranchCreate {
                branch: BranchRecord {
                    id: resume_request.destination_id,
                    revision: 1,
                    name: resume_request.destination_name.clone(),
                    state: BranchState::Creating,
                    root: None,
                    parent_id: Some(parent.id),
                    origin_checkpoint_id: Some(resumable_record.id),
                    created_at: now,
                    updated_at: now,
                },
                operation: Box::new(BranchCreateOperation {
                    id: resume_request.operation_id,
                    revision: 1,
                    destination_id: resume_request.destination_id,
                    destination_name: resume_request.destination_name.clone(),
                    source_checkpoint_id: resumable_record.id,
                    source_root: resumable_record.root.clone(),
                    parent_id: Some(parent.id),
                    phase: BranchCreatePhase::Reserved,
                    destination_root: None,
                    created_at: now,
                    updated_at: now,
                }),
            })
            .await
            .unwrap();
        let resumed_root = root_store
            .create_from_checkpoint(
                resume_request.operation_id,
                resume_request.destination_id,
                &resume_request.source,
            )
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::RecordBranchCreateRoot {
                operation_id: resume_request.operation_id,
                expected_revision: 1,
                destination_root: resumed_root,
                updated_at: catalog_timestamp(Utc::now()),
            })
            .await
            .unwrap();
        lifecycle
            .deletions()
            .delete_checkpoint(CheckpointDeleteRequest {
                checkpoint_id: resumable_record.id,
                expected_revision: resumable_record.revision,
                name: resumable_record.name,
            })
            .await
            .unwrap();
        let resumed = lifecycle
            .create_from_checkpoint(resume_request)
            .await
            .expect("root-created operation must resume after source deletion");
        assert_eq!(resumed.state, BranchState::Ready);
        root_store
            .verify(resumed.root.as_ref().unwrap())
            .await
            .unwrap();
        let leases = lifecycle.leases();
        let lease_request = LeaseAcquireRequest {
            lease_id: Uuid::new_v4(),
            renewal_token: Uuid::new_v4(),
            subject_id: left.id,
            access_mode: LeaseAccessMode::Write,
            duration: chrono::Duration::minutes(2),
        };
        let (first_grant, retry_grant) = tokio::join!(
            leases.acquire_branch_by_name(&left.name, lease_request.clone()),
            leases.acquire_branch_by_name(&left.name, lease_request.clone())
        );
        let first_grant = first_grant.unwrap();
        assert_eq!(first_grant, retry_grant.unwrap());
        let renewal = crate::catalog::LeaseRenewRequest {
            lease_id: first_grant.lease.id,
            expected_revision: first_grant.lease.revision,
            renewal_token: first_grant.renewal_token,
            duration: chrono::Duration::minutes(2),
        };
        let renewed = leases.renew(renewal.clone()).await.unwrap();
        assert_eq!(
            leases
                .renew(renewal)
                .await
                .expect("an ambiguous renewal retry must reconcile"),
            renewed
        );
        leases
            .release(renewed.id, renewed.revision, first_grant.renewal_token)
            .await
            .unwrap();
        leases
            .release(renewed.id, renewed.revision, first_grant.renewal_token)
            .await
            .expect("an exact release retry must return success");
        let retained_request = LeaseAcquireRequest {
            lease_id: Uuid::new_v4(),
            renewal_token: Uuid::new_v4(),
            subject_id: left.id,
            access_mode: LeaseAccessMode::Read,
            duration: chrono::Duration::minutes(2),
        };
        let retained = leases
            .acquire_branch_by_name(&left.name, retained_request.clone())
            .await
            .unwrap();
        let writer_request = LeaseAcquireRequest {
            lease_id: Uuid::new_v4(),
            renewal_token: Uuid::new_v4(),
            subject_id: left.id,
            access_mode: LeaseAccessMode::Write,
            duration: chrono::Duration::minutes(2),
        };
        let writer = leases
            .acquire_branch_by_name(&left.name, writer_request)
            .await
            .unwrap();
        let writer_renewal = crate::catalog::LeaseRenewRequest {
            lease_id: writer.lease.id,
            expected_revision: writer.lease.revision,
            renewal_token: writer.renewal_token,
            duration: chrono::Duration::minutes(2),
        };
        let renewed_writer = leases.renew(writer_renewal.clone()).await.unwrap();
        let deleting = BranchLifecycle::new(catalog.clone(), root_store.clone())
            .deletions()
            .delete_branch(crate::catalog::BranchDeleteRequest {
                operation_id: Uuid::new_v4(),
                branch_id: left.id,
                expected_revision: left.revision,
                name: left.name.clone(),
            })
            .await
            .unwrap();
        assert!(matches!(
            deleting,
            crate::catalog::BranchDeleteResult::Draining(_)
        ));
        catalog
            .apply(CatalogMutation::CreateBranch(BranchRecord {
                id: Uuid::new_v4(),
                revision: 1,
                name: left.name.clone(),
                state: BranchState::Ready,
                root: Some(DurableRoot {
                    identity: "lifecycle/reused-name".to_string(),
                    manifest_id: "replacement@1".to_string(),
                }),
                parent_id: None,
                origin_checkpoint_id: None,
                created_at: now,
                updated_at: now,
            }))
            .await
            .unwrap();
        assert_eq!(
            leases
                .acquire_branch_by_name(&left.name, retained_request)
                .await
                .expect("the pre-deletion exact lease retry must retain the old root"),
            retained
        );
        assert!(matches!(
            leases.renew(writer_renewal).await,
            Err(crate::catalog::LeaseLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));
        leases
            .release(
                retained.lease.id,
                retained.lease.revision,
                retained.renewal_token,
            )
            .await
            .unwrap();
        leases
            .release(
                renewed_writer.id,
                renewed_writer.revision,
                writer.renewal_token,
            )
            .await
            .unwrap();
        catalog.close().await.unwrap();
    }
}
