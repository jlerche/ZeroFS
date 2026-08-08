use super::{
    BranchState, Catalog, CatalogError, CatalogMutation, PrivateEpochRecord, PrivateEpochState,
    catalog_timestamp,
};
use crate::fs::store::ExtentStore;
use crate::fs::store::extent::PublisherDrainReceipt;
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

#[derive(Clone)]
pub struct PrivateEpochLifecycle {
    catalog: Arc<dyn Catalog>,
    segment_pool: Arc<dyn ObjectStore>,
    authority: SegmentPoolAuthority,
    publisher_id: Option<Uuid>,
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
            publisher_id: None,
        }
    }

    pub(crate) fn with_publisher(mut self, extent_store: &ExtentStore) -> Self {
        self.publisher_id = Some(extent_store.publisher_id());
        self
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
        if self.publisher_id != Some(receipt.publisher_id)
            || receipt.old_epoch != request.epoch
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
        DurableRoot, SlateDbCatalog,
    };
    use crate::config::CompressionConfig;
    use crate::db::Db;
    use crate::frame_codec::FrameCodec;
    use crate::fs::key_codec::KeyCodec;
    use crate::fs::lock_manager::KeyedLockManager;
    use crate::fs::store::ExtentStore;
    use crate::segment::SEGMENT_INFO;
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
        let lifecycle = PrivateEpochLifecycle::new(catalog.clone(), Arc::clone(&pool), authority);
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
        let lifecycle = lifecycle.with_publisher(&extent);
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
                pool,
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
        let delete_at = catalog_timestamp(now + chrono::Duration::microseconds(3));
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
        dummy_db.close().await.unwrap();
        db.close().await.unwrap();
        catalog.close().await.unwrap();
    }
}
