use super::{
    BranchState, Catalog, CatalogError, CatalogMutation, PrivateEpochRecord, PrivateEpochState,
    catalog_timestamp,
};
use crate::fs::store::ExtentStore;
use crate::fs::store::extent::{
    PersistedPrivateGcArtifact, PrivatePublisherIdentity, PublisherDrainReceipt,
};
use crate::segment_store::{SegmentPoolAuthority, SegmentStore, SegmentStoreError};
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateEpochRegisterRequest {
    pub epoch: u64,
    pub branch_id: Uuid,
    pub database_identity: String,
    /// Stable operation time supplied by the caller so an ambiguous catalog
    /// response can be retried as the exact same registration.
    pub registered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateEpochSealRequest {
    pub branch_id: Uuid,
    pub epoch: u64,
    pub expected_revision: u64,
    pub next_epoch: u64,
    pub sealed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateGcGuardRequest {
    pub id: Uuid,
    pub branch_id: Uuid,
    pub epoch: u64,
    pub expected_epoch_revision: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct PrivateEpochLifecycle {
    catalog: Arc<dyn Catalog>,
    segment_pool: Arc<dyn ObjectStore>,
    authority: SegmentPoolAuthority,
    publisher: Option<PrivatePublisherIdentity>,
}

impl std::fmt::Debug for PrivateEpochLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PrivateEpochLifecycle")
            .finish_non_exhaustive()
    }
}

impl PrivateEpochLifecycle {
    pub(crate) fn new(
        catalog: Arc<dyn Catalog>,
        segment_pool: Arc<dyn ObjectStore>,
        authority: SegmentPoolAuthority,
    ) -> Self {
        Self {
            catalog,
            segment_pool,
            authority,
            publisher: None,
        }
    }

    pub(crate) fn with_publisher(mut self, extent_store: &ExtentStore) -> Self {
        self.publisher = extent_store.private_publisher_identity();
        self
    }

    /// Attach one immutable, exact-writer candidate artifact to authoritative
    /// guard state. Storage ownership and catalog epoch identity are
    /// reauthenticated; the atomic guard mutation still rechecks all root
    /// blockers and the exact sealed revision.
    #[allow(dead_code)] // Invoked once mounted branches enable private collection.
    pub(crate) async fn acquire_gc_guard(
        &self,
        request: PrivateGcGuardRequest,
        artifact: &PersistedPrivateGcArtifact,
    ) -> Result<super::LocalGcGuardRecord, PrivateEpochLifecycleError> {
        if self.publisher.as_ref().is_none_or(|publisher| {
            publisher.publisher_id != artifact.publisher_id()
                || publisher.branch_id != artifact.branch_id()
                || publisher.database_identity != artifact.database_identity()
        }) || request.id != artifact.guard_id()
            || request.epoch != artifact.epoch()
            || request.branch_id != artifact.branch_id()
            || artifact.candidate_count() == 0
            || artifact.candidate_count() as usize > crate::fs::MAX_LOCAL_GC_CANDIDATES
        {
            return Err(CatalogError::OperationConflict(format!(
                "private epoch {} candidate artifact",
                request.epoch
            ))
            .into());
        }
        let proof = SegmentStore::authenticate_branch_epoch(
            Arc::clone(&self.segment_pool),
            &self.authority,
            request.epoch,
        )
        .await?;
        let epoch = self
            .catalog
            .private_epoch(request.epoch)
            .await?
            .ok_or_else(|| CatalogError::NotFound(format!("private epoch {}", request.epoch)))?;
        if !record_matches_proof(&epoch, &proof)
            || epoch.branch_id != request.branch_id
            || proof.database_identity != artifact.database_identity()
            || epoch.revision != request.expected_epoch_revision
            || epoch.state != PrivateEpochState::SealedPrivate
        {
            return Err(CatalogError::OperationConflict(format!(
                "private epoch {} guarded identity",
                request.epoch
            ))
            .into());
        }
        let guard = super::LocalGcGuardRecord {
            id: request.id,
            revision: 1,
            branch_id: request.branch_id,
            epoch: request.epoch,
            epoch_revision: request.expected_epoch_revision,
            candidate_count: artifact.candidate_count(),
            candidate_digest: artifact.candidate_digest().to_string(),
            created_at: catalog_timestamp(request.created_at),
        };
        self.catalog
            .apply(CatalogMutation::AcquireLocalGcGuard(guard.clone()))
            .await?;
        Ok(guard)
    }

    /// Register storage-authenticated branch ownership in authoritative
    /// SlateDB. Neither caller-supplied pool nor reservation identities are
    /// trusted: both are copied from the verified permanent marker.
    pub async fn register_authenticated(
        &self,
        request: PrivateEpochRegisterRequest,
    ) -> Result<PrivateEpochRecord, PrivateEpochLifecycleError> {
        let proof = SegmentStore::authenticate_branch_epoch(
            Arc::clone(&self.segment_pool),
            &self.authority,
            request.epoch,
        )
        .await?;
        if proof.branch_id != request.branch_id
            || proof.database_identity != request.database_identity
        {
            return Err(CatalogError::OperationConflict(format!(
                "authenticated private epoch {} identity",
                request.epoch
            ))
            .into());
        }
        let registered_at = catalog_timestamp(request.registered_at);
        let record = PrivateEpochRecord {
            epoch: proof.epoch,
            revision: 1,
            pool_id: proof.pool_id,
            reservation_id: proof.reservation_id,
            branch_id: proof.branch_id,
            database_identity: proof.database_identity,
            state: PrivateEpochState::Open,
            created_at: registered_at,
            updated_at: registered_at,
            sealed_at: None,
            exposed_at: None,
        };
        if let Some(existing) = self.catalog.private_epoch(record.epoch).await? {
            return reconcile_registration(existing, &record);
        }
        let branch = self
            .catalog
            .branch(proof.branch_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(proof.branch_id.to_string()))?;
        if branch.state != BranchState::Ready
            || branch.root.as_ref().map(|root| root.identity.as_str())
                != Some(record.database_identity.as_str())
        {
            return Err(CatalogError::OperationConflict(format!(
                "private epoch {} does not name its ready branch root",
                proof.epoch
            ))
            .into());
        }
        let applied = self
            .catalog
            .apply(CatalogMutation::RegisterPrivateEpoch(record.clone()))
            .await;
        if let Err(error) = applied {
            if let Some(existing) = self.catalog.private_epoch(record.epoch).await? {
                return reconcile_registration(existing, &record);
            }
            return Err(error.into());
        }
        let persisted = self
            .catalog
            .private_epoch(record.epoch)
            .await?
            .ok_or_else(|| CatalogError::NotFound(format!("private epoch {}", record.epoch)))?;
        reconcile_registration(persisted, &record)
    }

    /// Publish `sealed_private` only after the real filesystem has rotated to
    /// another authenticated/open epoch and drained every old FrameLoc
    /// publisher through its durability barriers.
    #[allow(dead_code)] // Invoked once the branch-mount writer path owns this lifecycle.
    pub(crate) async fn seal_after_rotation(
        &self,
        request: PrivateEpochSealRequest,
        receipt: &PublisherDrainReceipt,
    ) -> Result<PrivateEpochRecord, PrivateEpochLifecycleError> {
        if self.publisher.as_ref().is_none_or(|publisher| {
            publisher.publisher_id != receipt.publisher_id
                || publisher.branch_id != request.branch_id
        }) || receipt.old_epoch != request.epoch
            || receipt.next_epoch != request.next_epoch
        {
            return Err(CatalogError::OperationConflict(format!(
                "private epoch {} rotation receipt",
                request.epoch
            ))
            .into());
        }
        let old_proof = SegmentStore::authenticate_branch_epoch(
            Arc::clone(&self.segment_pool),
            &self.authority,
            request.epoch,
        )
        .await?;
        let next_proof = SegmentStore::authenticate_branch_epoch(
            Arc::clone(&self.segment_pool),
            &self.authority,
            request.next_epoch,
        )
        .await?;
        let old = self
            .catalog
            .private_epoch(request.epoch)
            .await?
            .ok_or_else(|| CatalogError::NotFound(format!("private epoch {}", request.epoch)))?;
        let next = self
            .catalog
            .private_epoch(request.next_epoch)
            .await?
            .ok_or_else(|| {
                CatalogError::NotFound(format!("private epoch {}", request.next_epoch))
            })?;
        if !record_matches_proof(&old, &old_proof)
            || !record_matches_proof(&next, &next_proof)
            || self
                .publisher
                .as_ref()
                .is_none_or(|publisher| publisher.database_identity != old_proof.database_identity)
            || old.branch_id != request.branch_id
            || next.branch_id != request.branch_id
            || old.pool_id != next.pool_id
            || old.database_identity != next.database_identity
        {
            return Err(CatalogError::OperationConflict(format!(
                "private epoch {} rotation identity",
                request.epoch
            ))
            .into());
        }
        if old.revision != request.expected_revision || old.state != PrivateEpochState::Open {
            return reconcile_seal(old, &request);
        }
        if next.state != PrivateEpochState::Open {
            return Err(CatalogError::OperationConflict(format!(
                "private epoch {} next writer is not open",
                request.next_epoch
            ))
            .into());
        }
        let sealed_at = catalog_timestamp(request.sealed_at);
        if sealed_at < old.updated_at || sealed_at < next.created_at {
            return Err(CatalogError::Invalid(
                "private epoch seal cannot precede either writer term".to_string(),
            )
            .into());
        }
        let applied = self
            .catalog
            .apply(CatalogMutation::SealPrivateEpoch {
                epoch: request.epoch,
                branch_id: request.branch_id,
                expected_revision: request.expected_revision,
                next_epoch: request.next_epoch,
                expected_next_revision: next.revision,
                sealed_at,
            })
            .await;
        if let Err(error) = applied {
            let current = self
                .catalog
                .private_epoch(request.epoch)
                .await?
                .ok_or_else(|| {
                    CatalogError::NotFound(format!("private epoch {}", request.epoch))
                })?;
            return reconcile_seal(current, &request).or(Err(error.into()));
        }
        let current = self
            .catalog
            .private_epoch(request.epoch)
            .await?
            .ok_or_else(|| CatalogError::NotFound(format!("private epoch {}", request.epoch)))?;
        reconcile_seal(current, &request)
    }
}

fn record_matches_proof(
    record: &PrivateEpochRecord,
    proof: &crate::segment_store::AuthenticatedBranchEpoch,
) -> bool {
    record.epoch == proof.epoch
        && record.pool_id == proof.pool_id
        && record.reservation_id == proof.reservation_id
        && record.branch_id == proof.branch_id
        && record.database_identity == proof.database_identity
}

fn reconcile_seal(
    current: PrivateEpochRecord,
    request: &PrivateEpochSealRequest,
) -> Result<PrivateEpochRecord, PrivateEpochLifecycleError> {
    let sealed_at = catalog_timestamp(request.sealed_at);
    if current.epoch == request.epoch
        && current.branch_id == request.branch_id
        && current.revision >= request.expected_revision.saturating_add(1)
        && current.sealed_at == Some(sealed_at)
        && matches!(
            current.state,
            PrivateEpochState::SealedPrivate | PrivateEpochState::Exposed
        )
    {
        return Ok(current);
    }
    Err(CatalogError::OperationConflict(format!("private epoch {} seal", request.epoch)).into())
}

fn reconcile_registration(
    existing: PrivateEpochRecord,
    expected: &PrivateEpochRecord,
) -> Result<PrivateEpochRecord, PrivateEpochLifecycleError> {
    if existing.epoch == expected.epoch
        && existing.pool_id == expected.pool_id
        && existing.reservation_id == expected.reservation_id
        && existing.branch_id == expected.branch_id
        && existing.database_identity == expected.database_identity
        && existing.created_at == expected.created_at
    {
        return Ok(existing);
    }
    Err(
        CatalogError::OperationConflict(format!("private epoch {} registration", expected.epoch))
            .into(),
    )
}

#[derive(Debug, thiserror::Error)]
pub enum PrivateEpochLifecycleError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    SegmentStore(#[from] SegmentStoreError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        BranchDeleteOperation, BranchDeletePhase, BranchRecord, BranchState, CatalogMutation,
        DurableRoot, LocalGcProgressRecord, SlateDbCatalog,
    };
    use crate::config::CompressionConfig;
    use crate::db::Db;
    use crate::frame_codec::FrameCodec;
    use crate::fs::key_codec::KeyCodec;
    use crate::fs::lock_manager::KeyedLockManager;
    use crate::fs::store::ExtentStore;
    use crate::segment::SEGMENT_INFO;
    use bytes::Bytes;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    fn ready_branch(id: Uuid, now: DateTime<Utc>) -> BranchRecord {
        BranchRecord {
            id,
            revision: 1,
            name: "private-owner".to_string(),
            state: BranchState::Ready,
            root: Some(DurableRoot {
                identity: format!("branches/{id}"),
                manifest_id: "checkpoint:00000000-0000-4000-8000-000000000001:1".to_string(),
            }),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn registration_uses_exact_authenticated_marker_identity() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(
                Path::from("private-registration/catalog"),
                Arc::clone(&store),
            )
            .await
            .unwrap(),
        );
        let now = catalog_timestamp(Utc::now());
        let branch = ready_branch(Uuid::new_v4(), now);
        catalog
            .apply(CatalogMutation::CreateBranch(branch.clone()))
            .await
            .unwrap();
        let pool: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority = SegmentStore::open_or_create_pool_authority(
            Arc::clone(&pool),
            &[9u8; 32],
            "volume",
            true,
        )
        .await
        .unwrap();
        let database_identity = format!("branches/{}", branch.id);
        let epoch = SegmentStore::reserve_branch_epoch(
            Arc::clone(&pool),
            &authority,
            &database_identity,
            branch.id,
        )
        .await
        .unwrap();
        let lifecycle = PrivateEpochLifecycle::new(catalog.clone(), pool, authority);
        let request = PrivateEpochRegisterRequest {
            epoch,
            branch_id: branch.id,
            database_identity: database_identity.clone(),
            registered_at: now,
        };
        let registered = lifecycle
            .register_authenticated(request.clone())
            .await
            .unwrap();
        assert_eq!(registered.branch_id, branch.id);
        assert_eq!(registered.database_identity, database_identity);
        assert!(!registered.pool_id.is_nil());
        assert!(!registered.reservation_id.is_nil());
        assert_eq!(registered.state, PrivateEpochState::Open);
        assert_eq!(
            lifecycle.register_authenticated(request).await.unwrap(),
            registered,
            "exact retry must reconcile to the same authenticated record"
        );

        let wrong_branch = PrivateEpochRegisterRequest {
            epoch,
            branch_id: Uuid::new_v4(),
            database_identity: registered.database_identity.clone(),
            registered_at: now,
        };
        assert!(matches!(
            lifecycle.register_authenticated(wrong_branch).await,
            Err(PrivateEpochLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));
        let wrong_database = PrivateEpochRegisterRequest {
            epoch,
            branch_id: branch.id,
            database_identity: "branches/wrong".to_string(),
            registered_at: now,
        };
        assert!(matches!(
            lifecycle.register_authenticated(wrong_database).await,
            Err(PrivateEpochLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));

        let other_database = "branches/other-database";
        let other_epoch = SegmentStore::reserve_branch_epoch(
            Arc::clone(&lifecycle.segment_pool),
            &lifecycle.authority,
            other_database,
            branch.id,
        )
        .await
        .unwrap();
        assert!(matches!(
            lifecycle
                .register_authenticated(PrivateEpochRegisterRequest {
                    epoch: other_epoch,
                    branch_id: branch.id,
                    database_identity: other_database.to_string(),
                    registered_at: now,
                })
                .await,
            Err(PrivateEpochLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));

        let delete_at = now + chrono::Duration::microseconds(1);
        catalog
            .apply(CatalogMutation::StartBranchDelete {
                operation: BranchDeleteOperation {
                    id: Uuid::new_v4(),
                    revision: 1,
                    branch_id: branch.id,
                    branch_name: branch.name.clone(),
                    expected_branch_revision: branch.revision,
                    root: branch.root.clone().unwrap(),
                    parent_id: branch.parent_id,
                    origin_checkpoint_id: branch.origin_checkpoint_id,
                    phase: BranchDeletePhase::Draining,
                    created_at: delete_at,
                    updated_at: delete_at,
                },
            })
            .await
            .unwrap();
        let reconciled = lifecycle
            .register_authenticated(PrivateEpochRegisterRequest {
                epoch,
                branch_id: branch.id,
                database_identity: registered.database_identity.clone(),
                registered_at: now,
            })
            .await
            .unwrap();
        assert_eq!(reconciled.state, PrivateEpochState::Exposed);
        assert_eq!(reconciled.epoch, registered.epoch);
        assert_eq!(reconciled.reservation_id, registered.reservation_id);
        catalog.close().await.unwrap();
    }

    #[tokio::test]
    async fn ownerless_reservation_cannot_be_registered_private() {
        let pool: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority = SegmentStore::open_or_create_pool_authority(
            Arc::clone(&pool),
            &[8u8; 32],
            "volume",
            true,
        )
        .await
        .unwrap();
        let epoch = SegmentStore::reserve_epoch(pool.clone(), &authority, "global")
            .await
            .unwrap();
        let error = SegmentStore::authenticate_branch_epoch(pool, &authority, epoch)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("global-only"));
    }

    #[tokio::test]
    async fn seal_requires_real_rotation_to_an_authenticated_open_successor() {
        let catalog_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(
            SlateDbCatalog::open(
                Path::from("private-seal/catalog"),
                Arc::clone(&catalog_store),
            )
            .await
            .unwrap(),
        );
        let now = catalog_timestamp(Utc::now());
        let branch = ready_branch(Uuid::new_v4(), now);
        catalog
            .apply(CatalogMutation::CreateBranch(branch.clone()))
            .await
            .unwrap();
        let database_identity = branch.root.as_ref().unwrap().identity.clone();
        let pool: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority = SegmentStore::open_or_create_pool_authority(
            Arc::clone(&pool),
            &[7u8; 32],
            "volume",
            true,
        )
        .await
        .unwrap();
        let old_epoch = SegmentStore::reserve_branch_epoch(
            Arc::clone(&pool),
            &authority,
            &database_identity,
            branch.id,
        )
        .await
        .unwrap();
        let next_epoch = SegmentStore::reserve_branch_epoch(
            Arc::clone(&pool),
            &authority,
            &database_identity,
            branch.id,
        )
        .await
        .unwrap();
        let lifecycle =
            PrivateEpochLifecycle::new(catalog.clone(), Arc::clone(&pool), authority.clone());
        for (epoch, registered_at) in [
            (old_epoch, now),
            (next_epoch, now + chrono::Duration::microseconds(1)),
        ] {
            lifecycle
                .register_authenticated(PrivateEpochRegisterRequest {
                    epoch,
                    branch_id: branch.id,
                    database_identity: database_identity.clone(),
                    registered_at,
                })
                .await
                .unwrap();
        }

        let mut other_branch = ready_branch(Uuid::new_v4(), now);
        other_branch.name = "private-other".to_string();
        catalog
            .apply(CatalogMutation::CreateBranch(other_branch.clone()))
            .await
            .unwrap();
        let other_database_identity = other_branch.root.as_ref().unwrap().identity.clone();
        let other_old_epoch = SegmentStore::reserve_branch_epoch(
            Arc::clone(&pool),
            &authority,
            &other_database_identity,
            other_branch.id,
        )
        .await
        .unwrap();
        let other_next_epoch = SegmentStore::reserve_branch_epoch(
            Arc::clone(&pool),
            &authority,
            &other_database_identity,
            other_branch.id,
        )
        .await
        .unwrap();
        for (epoch, registered_at) in [
            (other_old_epoch, now + chrono::Duration::microseconds(2)),
            (other_next_epoch, now + chrono::Duration::microseconds(3)),
        ] {
            lifecycle
                .register_authenticated(PrivateEpochRegisterRequest {
                    epoch,
                    branch_id: other_branch.id,
                    database_identity: other_database_identity.clone(),
                    registered_at,
                })
                .await
                .unwrap();
        }
        catalog
            .apply(CatalogMutation::SealPrivateEpoch {
                epoch: other_old_epoch,
                branch_id: other_branch.id,
                expected_revision: 1,
                next_epoch: other_next_epoch,
                expected_next_revision: 1,
                sealed_at: now + chrono::Duration::microseconds(4),
            })
            .await
            .unwrap();

        let metadata_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let raw = Arc::new(
            slatedb::DbBuilder::new(Path::from("private-seal/data"), metadata_store)
                .build()
                .await
                .unwrap(),
        );
        let db = Arc::new(Db::new(raw, None));
        let extent = ExtentStore::new(
            Arc::clone(&db),
            Arc::new(KeyCodec::new()),
            Arc::new(SegmentStore::new(
                Arc::clone(&pool),
                FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4),
                old_epoch,
                None,
            )),
            Arc::new(KeyedLockManager::new()),
            1024 * 1024,
        );
        extent
            .bind_private_owner(branch.id, database_identity.clone())
            .unwrap();
        let lifecycle = lifecycle.with_publisher(&extent);
        let mut first_txn = db.new_transaction().unwrap();
        let first_tail = extent
            .write(&mut first_txn, 1, 0, &Bytes::from_static(b"first"), 0)
            .await
            .unwrap();
        extent.commit_test_transaction(first_txn).await.unwrap();
        extent.apply_tail_update(1, first_tail);
        extent.seal_open().await.unwrap();
        let mut replacement_txn = db.new_transaction().unwrap();
        let replacement_tail = extent
            .write(
                &mut replacement_txn,
                1,
                0,
                &Bytes::from_static(b"replacement"),
                5,
            )
            .await
            .unwrap();
        extent
            .commit_test_transaction(replacement_txn)
            .await
            .unwrap();
        extent.apply_tail_update(1, replacement_tail);
        extent.seal_open().await.unwrap();
        let request = PrivateEpochSealRequest {
            branch_id: branch.id,
            epoch: old_epoch,
            expected_revision: 1,
            next_epoch,
            sealed_at: now + chrono::Duration::microseconds(2),
        };

        let dummy_raw = Arc::new(
            slatedb::DbBuilder::new(Path::from("private-seal/dummy"), Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let dummy_db = Arc::new(Db::new(dummy_raw, None));
        let dummy = ExtentStore::new(
            Arc::clone(&dummy_db),
            Arc::new(KeyCodec::new()),
            Arc::new(SegmentStore::new(
                Arc::clone(&pool),
                FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4),
                old_epoch,
                None,
            )),
            Arc::new(KeyedLockManager::new()),
            1024 * 1024,
        );
        let wrong_receipt = dummy.rotate_writer_epoch(next_epoch).await.unwrap();
        assert!(matches!(
            lifecycle
                .seal_after_rotation(request.clone(), &wrong_receipt)
                .await,
            Err(PrivateEpochLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));

        let receipt = extent.rotate_writer_epoch(next_epoch).await.unwrap();
        let sealed = lifecycle
            .seal_after_rotation(request.clone(), &receipt)
            .await
            .unwrap();
        assert_eq!(sealed.state, PrivateEpochState::SealedPrivate);
        assert_eq!(sealed.revision, 2);
        assert_eq!(
            lifecycle
                .seal_after_rotation(request.clone(), &receipt)
                .await
                .unwrap(),
            sealed
        );
        assert_eq!(
            catalog
                .private_epoch(next_epoch)
                .await
                .unwrap()
                .unwrap()
                .state,
            PrivateEpochState::Open
        );
        let batch = extent
            .prepare_private_gc_batch(old_epoch, crate::fs::MAX_LOCAL_GC_CANDIDATES)
            .await
            .unwrap();
        assert_eq!(batch.candidates.len(), 1);
        let guard_id = Uuid::new_v4();
        let artifact = extent
            .persist_private_gc_batch(guard_id, &batch)
            .await
            .unwrap();

        // Same-pool storage credentials are insufficient authority: a writer
        // bound to branch A cannot attach its locally prepared bytes to a
        // valid, sealed epoch owned by branch B.
        let cross_raw = Arc::new(
            slatedb::DbBuilder::new(
                Path::from("private-seal/cross-branch"),
                Arc::new(InMemory::new()),
            )
            .build()
            .await
            .unwrap(),
        );
        let cross_db = Arc::new(Db::new(cross_raw, None));
        let cross_extent = ExtentStore::new(
            Arc::clone(&cross_db),
            Arc::new(KeyCodec::new()),
            Arc::new(SegmentStore::new(
                Arc::clone(&pool),
                FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4),
                other_old_epoch,
                None,
            )),
            Arc::new(KeyedLockManager::new()),
            1024 * 1024,
        );
        cross_extent
            .bind_private_owner(branch.id, database_identity.clone())
            .unwrap();
        let mut cross_first_txn = cross_db.new_transaction().unwrap();
        let cross_first_tail = cross_extent
            .write(
                &mut cross_first_txn,
                11,
                0,
                &Bytes::from_static(b"first"),
                0,
            )
            .await
            .unwrap();
        cross_extent
            .commit_test_transaction(cross_first_txn)
            .await
            .unwrap();
        cross_extent.apply_tail_update(11, cross_first_tail);
        cross_extent.seal_open().await.unwrap();
        let mut cross_replacement_txn = cross_db.new_transaction().unwrap();
        let cross_replacement_tail = cross_extent
            .write(
                &mut cross_replacement_txn,
                11,
                0,
                &Bytes::from_static(b"replacement"),
                5,
            )
            .await
            .unwrap();
        cross_extent
            .commit_test_transaction(cross_replacement_txn)
            .await
            .unwrap();
        cross_extent.apply_tail_update(11, cross_replacement_tail);
        cross_extent.seal_open().await.unwrap();
        let _cross_receipt = cross_extent
            .rotate_writer_epoch(other_next_epoch)
            .await
            .unwrap();
        let cross_batch = cross_extent
            .prepare_private_gc_batch(other_old_epoch, crate::fs::MAX_LOCAL_GC_CANDIDATES)
            .await
            .unwrap();
        let cross_guard_id = Uuid::new_v4();
        let cross_artifact = cross_extent
            .persist_private_gc_batch(cross_guard_id, &cross_batch)
            .await
            .unwrap();
        let cross_lifecycle =
            PrivateEpochLifecycle::new(catalog.clone(), Arc::clone(&pool), authority.clone())
                .with_publisher(&cross_extent);
        assert!(matches!(
            cross_lifecycle
                .acquire_gc_guard(
                    PrivateGcGuardRequest {
                        id: cross_guard_id,
                        branch_id: other_branch.id,
                        epoch: other_old_epoch,
                        expected_epoch_revision: 2,
                        created_at: now + chrono::Duration::microseconds(5),
                    },
                    &cross_artifact,
                )
                .await,
            Err(PrivateEpochLifecycleError::Catalog(
                CatalogError::OperationConflict(_)
            ))
        ));
        let guard_request = PrivateGcGuardRequest {
            id: guard_id,
            branch_id: branch.id,
            epoch: old_epoch,
            expected_epoch_revision: sealed.revision,
            created_at: now + chrono::Duration::microseconds(3),
        };
        let guard = lifecycle
            .acquire_gc_guard(guard_request.clone(), &artifact)
            .await
            .unwrap();
        assert_eq!(guard.candidate_digest, batch.candidate_digest);
        assert_eq!(
            lifecycle
                .acquire_gc_guard(guard_request, &artifact)
                .await
                .unwrap(),
            guard
        );
        extent
            .delete_prepared_private_candidates_for_test(&batch)
            .await;
        let progress_at = guard.created_at + chrono::Duration::microseconds(1);
        let initial_progress = LocalGcProgressRecord {
            id: guard.id,
            revision: 1,
            branch_id: guard.branch_id,
            epoch: guard.epoch,
            epoch_revision: guard.epoch_revision,
            candidate_count: guard.candidate_count,
            candidate_digest: guard.candidate_digest.clone(),
            next_candidate: 0,
            deleted_objects: 0,
            deleted_bytes: 0,
            already_absent: 0,
            started_at: progress_at,
            updated_at: progress_at,
            completed_at: None,
        };
        catalog
            .apply(CatalogMutation::PublishLocalGcProgress(
                initial_progress.clone(),
            ))
            .await
            .unwrap();
        let mut completed_progress = initial_progress;
        completed_progress.revision = 2;
        completed_progress.next_candidate = 1;
        completed_progress.deleted_objects = 1;
        completed_progress.updated_at += chrono::Duration::microseconds(1);
        completed_progress.completed_at = Some(completed_progress.updated_at);
        catalog
            .apply(CatalogMutation::PublishLocalGcProgress(completed_progress))
            .await
            .unwrap();
        let delete_at = catalog_timestamp(now + chrono::Duration::microseconds(6));
        catalog
            .apply(CatalogMutation::StartBranchDelete {
                operation: BranchDeleteOperation {
                    id: Uuid::new_v4(),
                    revision: 1,
                    branch_id: branch.id,
                    branch_name: branch.name.clone(),
                    expected_branch_revision: branch.revision,
                    root: branch.root.clone().unwrap(),
                    parent_id: branch.parent_id,
                    origin_checkpoint_id: branch.origin_checkpoint_id,
                    phase: BranchDeletePhase::Draining,
                    created_at: delete_at,
                    updated_at: delete_at,
                },
            })
            .await
            .unwrap();
        let exposed = lifecycle
            .seal_after_rotation(request, &receipt)
            .await
            .unwrap();
        assert_eq!(exposed.state, PrivateEpochState::Exposed);
        assert_eq!(exposed.sealed_at, sealed.sealed_at);
        cross_db.close().await.unwrap();
        dummy_db.close().await.unwrap();
        db.close().await.unwrap();
        catalog.close().await.unwrap();
    }
}
