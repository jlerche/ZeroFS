use super::catalog_timestamp;
use super::{
    BranchDeleteOperation, BranchDeletePhase, BranchRecord, Catalog, CatalogError, CatalogMutation,
    TombstoneKind, TombstoneRecord,
};
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointDeleteRequest {
    pub checkpoint_id: Uuid,
    pub expected_revision: u64,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchDeleteRequest {
    pub operation_id: Uuid,
    pub branch_id: Uuid,
    pub expected_revision: u64,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BranchDeleteResult {
    Draining(BranchRecord),
    Deleted(TombstoneRecord),
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

    /// Fence one exact branch incarnation, drain its writer lease, and then
    /// publish a root-free tombstone. Reader leases retain their own roots.
    pub async fn delete_branch(
        &self,
        request: BranchDeleteRequest,
    ) -> Result<BranchDeleteResult, DeletionLifecycleError> {
        let operation = match self
            .catalog
            .branch_delete_operation(request.operation_id)
            .await?
        {
            Some(operation) => {
                exact_branch_operation(&request, &operation)?;
                operation
            }
            None => {
                let branch = self
                    .catalog
                    .branch(request.branch_id)
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(request.branch_id.to_string()))?;
                if branch.name != request.name {
                    return Err(CatalogError::NotFound(format!(
                        "{} ({})",
                        request.name, request.branch_id
                    ))
                    .into());
                }
                let now = catalog_timestamp(Utc::now());
                let operation = BranchDeleteOperation {
                    id: request.operation_id,
                    revision: 1,
                    branch_id: branch.id,
                    branch_name: branch.name.clone(),
                    expected_branch_revision: request.expected_revision,
                    root: branch.root.clone().ok_or_else(|| {
                        CatalogError::OperationConflict(format!(
                            "branch {} has no durable root",
                            branch.id
                        ))
                    })?,
                    parent_id: branch.parent_id,
                    origin_checkpoint_id: branch.origin_checkpoint_id,
                    phase: BranchDeletePhase::Draining,
                    created_at: now,
                    updated_at: now,
                };
                let applied = self
                    .catalog
                    .apply(CatalogMutation::StartBranchDelete {
                        operation: operation.clone(),
                    })
                    .await;
                if let Err(error) = applied {
                    if let Some(existing) = self
                        .catalog
                        .branch_delete_operation(request.operation_id)
                        .await?
                    {
                        exact_branch_operation(&request, &existing)?;
                        existing
                    } else {
                        return Err(error.into());
                    }
                } else {
                    operation
                }
            }
        };

        if operation.phase == BranchDeletePhase::Published {
            return Ok(BranchDeleteResult::Deleted(
                self.exact_branch_tombstone(&request).await?,
            ));
        }
        let finalized = self
            .catalog
            .apply(CatalogMutation::FinalizeBranchDelete {
                operation_id: operation.id,
                expected_revision: operation.revision,
                deleted_at: catalog_timestamp(Utc::now()),
            })
            .await;
        match finalized {
            Ok(_) => Ok(BranchDeleteResult::Deleted(
                self.exact_branch_tombstone(&request).await?,
            )),
            Err(CatalogError::WriterLeaseActive(branch_id)) if branch_id == request.branch_id => {
                if self.branch_delete_is_published(&request).await? {
                    return Ok(BranchDeleteResult::Deleted(
                        self.exact_branch_tombstone(&request).await?,
                    ));
                }
                match self.catalog.branch(request.branch_id).await? {
                    Some(branch) => {
                        if self.branch_delete_is_published(&request).await? {
                            Ok(BranchDeleteResult::Deleted(
                                self.exact_branch_tombstone(&request).await?,
                            ))
                        } else {
                            Ok(BranchDeleteResult::Draining(branch))
                        }
                    }
                    None if self.branch_delete_is_published(&request).await? => Ok(
                        BranchDeleteResult::Deleted(self.exact_branch_tombstone(&request).await?),
                    ),
                    None => Err(CatalogError::Corrupt(format!(
                        "draining branch {} disappeared before publication",
                        request.branch_id
                    ))
                    .into()),
                }
            }
            Err(error) => {
                if self
                    .catalog
                    .branch_delete_operation(request.operation_id)
                    .await?
                    .is_some_and(|operation| operation.phase == BranchDeletePhase::Published)
                {
                    Ok(BranchDeleteResult::Deleted(
                        self.exact_branch_tombstone(&request).await?,
                    ))
                } else {
                    Err(error.into())
                }
            }
        }
    }

    async fn exact_branch_tombstone(
        &self,
        request: &BranchDeleteRequest,
    ) -> Result<TombstoneRecord, DeletionLifecycleError> {
        let tombstone = self
            .catalog
            .tombstone(request.branch_id)
            .await?
            .ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "branch {} deletion published without a tombstone",
                    request.branch_id
                ))
            })?;
        if tombstone.kind != TombstoneKind::Branch
            || tombstone.name != request.name
            || tombstone.deleted_revision != Some(request.expected_revision)
            || tombstone.deletion_operation_id != Some(request.operation_id)
        {
            return Err(CatalogError::OperationConflict(request.operation_id.to_string()).into());
        }
        Ok(tombstone)
    }

    async fn branch_delete_is_published(
        &self,
        request: &BranchDeleteRequest,
    ) -> Result<bool, DeletionLifecycleError> {
        let operation = self
            .catalog
            .branch_delete_operation(request.operation_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(request.operation_id.to_string()))?;
        exact_branch_operation(request, &operation)?;
        Ok(operation.phase == BranchDeletePhase::Published)
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

fn exact_branch_operation(
    request: &BranchDeleteRequest,
    operation: &BranchDeleteOperation,
) -> Result<(), DeletionLifecycleError> {
    if operation.branch_id != request.branch_id
        || operation.branch_name != request.name
        || operation.expected_branch_revision != request.expected_revision
    {
        return Err(CatalogError::OperationConflict(request.operation_id.to_string()).into());
    }
    Ok(())
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
        BranchCreateOperation, BranchCreatePhase, BranchRecord, BranchState, Catalog,
        CatalogMutation, CheckpointRecord, DurableRoot, LeaseAccessMode, LeaseRecord,
        LeaseSubjectKind, SlateDbCatalog, catalog_timestamp,
    };
    use object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FinalizedAfterWriterConflictCatalog {
        inner: Arc<SlateDbCatalog>,
        writer: LeaseRecord,
        intercept_once: AtomicBool,
    }

    #[async_trait::async_trait]
    impl Catalog for FinalizedAfterWriterConflictCatalog {
        async fn snapshot(&self) -> Result<crate::catalog::CatalogSnapshot, CatalogError> {
            self.inner.snapshot().await
        }
        async fn branch(&self, id: Uuid) -> Result<Option<BranchRecord>, CatalogError> {
            self.inner.branch(id).await
        }
        async fn branch_by_name(&self, name: &str) -> Result<Option<BranchRecord>, CatalogError> {
            self.inner.branch_by_name(name).await
        }
        async fn checkpoint(&self, id: Uuid) -> Result<Option<CheckpointRecord>, CatalogError> {
            self.inner.checkpoint(id).await
        }
        async fn checkpoint_by_name(
            &self,
            branch_id: Uuid,
            name: &str,
        ) -> Result<Option<CheckpointRecord>, CatalogError> {
            self.inner.checkpoint_by_name(branch_id, name).await
        }
        async fn branch_create_operation(
            &self,
            id: Uuid,
        ) -> Result<Option<BranchCreateOperation>, CatalogError> {
            self.inner.branch_create_operation(id).await
        }
        async fn branch_delete_operation(
            &self,
            id: Uuid,
        ) -> Result<Option<BranchDeleteOperation>, CatalogError> {
            self.inner.branch_delete_operation(id).await
        }
        async fn gc_run(
            &self,
            id: Uuid,
        ) -> Result<Option<crate::catalog::GcRunRecord>, CatalogError> {
            self.inner.gc_run(id).await
        }
        async fn gc_blockers(
            &self,
            run_id: Uuid,
        ) -> Result<Vec<crate::catalog::GcBlockerRecord>, CatalogError> {
            self.inner.gc_blockers(run_id).await
        }
        async fn begin_gc_run(
            &self,
            expected_generation: u64,
            run: crate::catalog::GcRunRecord,
        ) -> Result<(), CatalogError> {
            self.inner.begin_gc_run(expected_generation, run).await
        }
        async fn publish_gc_marks(
            &self,
            id: Uuid,
            expected_revision: u64,
            root_digest: String,
            mark_shards: Vec<crate::catalog::GcMarkShard>,
            mark_stats: crate::catalog::GcMarkStats,
            updated_at: chrono::DateTime<Utc>,
        ) -> Result<crate::catalog::GcRunRecord, CatalogError> {
            self.inner
                .publish_gc_marks(
                    id,
                    expected_revision,
                    root_digest,
                    mark_shards,
                    mark_stats,
                    updated_at,
                )
                .await
        }
        async fn publish_gc_quarantine(
            &self,
            publication: crate::catalog::GcQuarantinePublication,
        ) -> Result<crate::catalog::GcRunRecord, CatalogError> {
            self.inner.publish_gc_quarantine(publication).await
        }
        async fn record_gc_blocker(
            &self,
            run_id: Uuid,
            kind: crate::catalog::GcBlockerKind,
            detail: String,
            observed_at: chrono::DateTime<Utc>,
        ) -> Result<crate::catalog::GcBlockerRecord, CatalogError> {
            self.inner
                .record_gc_blocker(run_id, kind, detail, observed_at)
                .await
        }
        async fn lease(&self, id: Uuid) -> Result<Option<LeaseRecord>, CatalogError> {
            self.inner.lease(id).await
        }
        async fn tombstone(&self, id: Uuid) -> Result<Option<TombstoneRecord>, CatalogError> {
            self.inner.tombstone(id).await
        }
        async fn apply(&self, mutation: CatalogMutation) -> Result<u64, CatalogError> {
            if matches!(&mutation, CatalogMutation::FinalizeBranchDelete { .. })
                && self.intercept_once.swap(false, Ordering::SeqCst)
            {
                self.inner
                    .apply(CatalogMutation::EndLease {
                        id: self.writer.id,
                        expected_revision: self.writer.revision,
                        token_hash: self.writer.token_hash.clone(),
                        ended_at: self.writer.updated_at,
                    })
                    .await?;
                self.inner.apply(mutation).await?;
                return Err(CatalogError::WriterLeaseActive(self.writer.subject_id));
            }
            self.inner.apply(mutation).await
        }
    }

    fn branch(name: &str, parent_id: Option<Uuid>, now: chrono::DateTime<Utc>) -> BranchRecord {
        BranchRecord {
            id: Uuid::new_v4(),
            revision: 1,
            name: name.to_string(),
            state: BranchState::Ready,
            root: Some(DurableRoot {
                identity: format!("branch/{name}"),
                manifest_id: format!("{name}@1"),
            }),
            parent_id,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn writer_conflict_reconciles_when_a_concurrent_retry_publishes_deletion() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let inner = Arc::new(
            SlateDbCatalog::open(Path::from("branch-delete-writer-race"), store)
                .await
                .unwrap(),
        );
        let now = catalog_timestamp(Utc::now());
        let record = branch("writer-race", None, now);
        inner
            .apply(CatalogMutation::CreateBranch(record.clone()))
            .await
            .unwrap();
        let writer = LeaseRecord {
            id: Uuid::new_v4(),
            revision: 1,
            subject_kind: LeaseSubjectKind::Branch,
            subject_id: record.id,
            root: record.root.clone().unwrap(),
            access_mode: LeaseAccessMode::Write,
            token_hash: "c".repeat(64),
            issued_at: now,
            updated_at: now,
            expires_at: now + chrono::Duration::minutes(1),
        };
        inner
            .apply(CatalogMutation::AcquireLease {
                expected_subject_revision: record.revision,
                lease: writer.clone(),
            })
            .await
            .unwrap();
        let catalog: Arc<dyn Catalog> = Arc::new(FinalizedAfterWriterConflictCatalog {
            inner: inner.clone(),
            writer,
            intercept_once: AtomicBool::new(true),
        });
        let request = BranchDeleteRequest {
            operation_id: Uuid::new_v4(),
            branch_id: record.id,
            expected_revision: record.revision,
            name: record.name,
        };
        assert!(matches!(
            DeletionLifecycle::new(catalog)
                .delete_branch(request.clone())
                .await
                .unwrap(),
            BranchDeleteResult::Deleted(tombstone)
                if tombstone.deletion_operation_id == Some(request.operation_id)
        ));
        assert!(inner.branch(record.id).await.unwrap().is_none());
        inner.close().await.unwrap();
    }

    #[tokio::test]
    async fn branch_deletion_preserves_descendants_drains_writers_and_isolates_name_reuse() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("branch-delete"), store)
                .await
                .unwrap(),
        );
        let lifecycle = DeletionLifecycle::new(catalog.clone());
        let now = catalog_timestamp(Utc::now());
        let parent = branch("parent", None, now);
        let child = branch("child", Some(parent.id), now);
        let grandchild = branch("grandchild", Some(child.id), now);
        for record in [&parent, &child, &grandchild] {
            catalog
                .apply(CatalogMutation::CreateBranch(record.clone()))
                .await
                .unwrap();
        }

        let reader = LeaseRecord {
            id: Uuid::new_v4(),
            revision: 1,
            subject_kind: LeaseSubjectKind::Branch,
            subject_id: parent.id,
            root: parent.root.clone().unwrap(),
            access_mode: LeaseAccessMode::Read,
            token_hash: "a".repeat(64),
            issued_at: now,
            updated_at: now,
            expires_at: now + chrono::Duration::minutes(1),
        };
        catalog
            .apply(CatalogMutation::AcquireLease {
                expected_subject_revision: parent.revision,
                lease: reader.clone(),
            })
            .await
            .unwrap();
        let mounted_descendant = LeaseRecord {
            id: Uuid::new_v4(),
            subject_id: grandchild.id,
            root: grandchild.root.clone().unwrap(),
            ..reader.clone()
        };
        catalog
            .apply(CatalogMutation::AcquireLease {
                expected_subject_revision: grandchild.revision,
                lease: mounted_descendant.clone(),
            })
            .await
            .unwrap();
        let source = CheckpointRecord {
            id: Uuid::new_v4(),
            revision: 1,
            branch_id: parent.id,
            name: "creating-source".to_string(),
            root: DurableRoot {
                identity: "checkpoint/creating-source".to_string(),
                manifest_id: "creating-source@1".to_string(),
            },
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::CreateCheckpoint(source.clone()))
            .await
            .unwrap();
        let creating = branch("creating-child", Some(parent.id), now);
        let create_operation = BranchCreateOperation {
            id: Uuid::new_v4(),
            revision: 1,
            destination_id: creating.id,
            destination_name: creating.name.clone(),
            source_checkpoint_id: source.id,
            source_root: source.root.clone(),
            parent_id: Some(parent.id),
            phase: BranchCreatePhase::Reserved,
            destination_root: None,
            created_at: now,
            updated_at: now,
        };
        catalog
            .apply(CatalogMutation::ReserveBranchCreate {
                branch: BranchRecord {
                    state: BranchState::Creating,
                    root: None,
                    origin_checkpoint_id: Some(source.id),
                    ..creating.clone()
                },
                operation: Box::new(create_operation.clone()),
            })
            .await
            .unwrap();
        let parent_request = BranchDeleteRequest {
            operation_id: Uuid::new_v4(),
            branch_id: parent.id,
            expected_revision: parent.revision,
            name: parent.name.clone(),
        };
        let (first, retry) = tokio::join!(
            lifecycle.delete_branch(parent_request.clone()),
            lifecycle.delete_branch(parent_request.clone())
        );
        assert!(matches!(first.unwrap(), BranchDeleteResult::Deleted(_)));
        assert!(matches!(retry.unwrap(), BranchDeleteResult::Deleted(_)));
        let snapshot = catalog.snapshot().await.unwrap();
        assert_eq!(snapshot.branches[&child.id].root, child.root);
        assert_eq!(snapshot.branches[&grandchild.id].root, grandchild.root);
        assert!(snapshot.gc_roots().contains(&&reader.root));
        assert!(snapshot.gc_roots().contains(&&mounted_descendant.root));
        assert_eq!(snapshot.tombstones[&parent.id].parent_id, parent.parent_id);
        let created_root = DurableRoot {
            identity: "branch/creating-child".to_string(),
            manifest_id: "creating-child@1".to_string(),
        };
        catalog
            .apply(CatalogMutation::RecordBranchCreateRoot {
                operation_id: create_operation.id,
                expected_revision: create_operation.revision,
                destination_root: created_root.clone(),
                updated_at: catalog_timestamp(Utc::now()),
            })
            .await
            .unwrap();
        catalog
            .apply(CatalogMutation::PublishBranchCreate {
                operation_id: create_operation.id,
                expected_revision: 2,
                updated_at: catalog_timestamp(Utc::now()),
            })
            .await
            .unwrap();
        assert_eq!(
            catalog.branch(creating.id).await.unwrap().unwrap().root,
            Some(created_root)
        );

        let replacement = branch(&parent.name, None, now);
        catalog
            .apply(CatalogMutation::CreateBranch(replacement.clone()))
            .await
            .unwrap();
        assert_eq!(
            lifecycle.delete_branch(parent_request).await.unwrap(),
            BranchDeleteResult::Deleted(snapshot.tombstones[&parent.id].clone())
        );
        assert_eq!(
            catalog.branch_by_name(&parent.name).await.unwrap(),
            Some(replacement)
        );

        let writer = LeaseRecord {
            id: Uuid::new_v4(),
            subject_id: child.id,
            root: child.root.clone().unwrap(),
            access_mode: LeaseAccessMode::Write,
            token_hash: "b".repeat(64),
            ..reader
        };
        catalog
            .apply(CatalogMutation::AcquireLease {
                expected_subject_revision: child.revision,
                lease: writer.clone(),
            })
            .await
            .unwrap();
        let child_request = BranchDeleteRequest {
            operation_id: Uuid::new_v4(),
            branch_id: child.id,
            expected_revision: child.revision,
            name: child.name.clone(),
        };
        assert!(matches!(
            lifecycle.delete_branch(child_request.clone()).await.unwrap(),
            BranchDeleteResult::Draining(branch) if branch.state == BranchState::Deleting
        ));
        assert!(catalog.branch_by_name(&child.name).await.unwrap().is_none());
        assert!(matches!(
            catalog
                .apply(CatalogMutation::RenewLease {
                    id: writer.id,
                    expected_revision: writer.revision,
                    token_hash: writer.token_hash.clone(),
                    renewed_at: now,
                    expires_at: writer.expires_at + chrono::Duration::seconds(1),
                })
                .await,
            Err(CatalogError::OperationConflict(_))
        ));
        catalog
            .apply(CatalogMutation::EndLease {
                id: writer.id,
                expected_revision: writer.revision,
                token_hash: writer.token_hash,
                ended_at: now,
            })
            .await
            .unwrap();
        assert!(matches!(
            lifecycle.delete_branch(child_request).await.unwrap(),
            BranchDeleteResult::Deleted(_)
        ));
        assert_eq!(
            catalog.branch(grandchild.id).await.unwrap(),
            Some(grandchild)
        );
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn ancestor_deletion_order_does_not_change_descendant_identity() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(Path::from("branch-delete-orders"), store)
                .await
                .unwrap(),
        );
        let lifecycle = DeletionLifecycle::new(catalog.clone());
        let now = catalog_timestamp(Utc::now());
        let first_parent = branch("first-parent", None, now);
        let first_child = branch("first-child", Some(first_parent.id), now);
        let second_parent = branch("second-parent", None, now);
        let second_child = branch("second-child", Some(second_parent.id), now);
        for record in [&first_parent, &first_child, &second_parent, &second_child] {
            catalog
                .apply(CatalogMutation::CreateBranch(record.clone()))
                .await
                .unwrap();
        }
        for record in [&first_parent, &first_child, &second_child, &second_parent] {
            assert!(matches!(
                lifecycle
                    .delete_branch(BranchDeleteRequest {
                        operation_id: Uuid::new_v4(),
                        branch_id: record.id,
                        expected_revision: record.revision,
                        name: record.name.clone(),
                    })
                    .await
                    .unwrap(),
                BranchDeleteResult::Deleted(_)
            ));
        }
        let snapshot = catalog.snapshot().await.unwrap();
        assert!(snapshot.branches.is_empty());
        for record in [&first_parent, &first_child, &second_parent, &second_child] {
            let tombstone = &snapshot.tombstones[&record.id];
            assert_eq!(tombstone.name, record.name);
            assert_eq!(tombstone.parent_id, record.parent_id);
        }
        catalog.close().await.unwrap();
    }

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
