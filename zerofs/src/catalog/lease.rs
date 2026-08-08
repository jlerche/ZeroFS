use super::{
    Catalog, CatalogError, CatalogMutation, DurableRoot, ImmutableCheckpoint, LeaseAccessMode,
    LeaseRecord, LeaseSubjectKind, RootStoreError, SlateDbRootStore, catalog_timestamp,
};
use chrono::{DateTime, Duration, Utc};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

pub const MAX_LEASE_DURATION: Duration = Duration::minutes(5);
pub const LEASE_CLOCK_SKEW: Duration = Duration::seconds(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseAcquireRequest {
    pub lease_id: Uuid,
    pub renewal_token: Uuid,
    pub subject_id: Uuid,
    pub access_mode: LeaseAccessMode,
    pub duration: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LeaseGrant {
    pub lease: LeaseRecord,
    pub renewal_token: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRenewRequest {
    pub lease_id: Uuid,
    pub expected_revision: u64,
    pub renewal_token: Uuid,
    pub duration: Duration,
}

#[derive(Clone)]
pub struct LeaseLifecycle {
    catalog: Arc<dyn Catalog>,
    roots: SlateDbRootStore,
}

impl std::fmt::Debug for LeaseLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaseLifecycle")
            .finish_non_exhaustive()
    }
}

impl LeaseLifecycle {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, roots: SlateDbRootStore) -> Self {
        Self { catalog, roots }
    }

    pub async fn acquire_branch_by_name(
        &self,
        name: &str,
        request: LeaseAcquireRequest,
    ) -> Result<LeaseGrant, LeaseLifecycleError> {
        if let Some(existing) = self.exact_retry(&request, LeaseSubjectKind::Branch).await? {
            self.roots.verify(&existing.lease.root).await?;
            return Ok(existing);
        }
        validate_duration(request.duration)?;
        let branch = self
            .catalog
            .branch_by_name(name)
            .await?
            .filter(|branch| branch.id == request.subject_id)
            .ok_or_else(|| CatalogError::NotFound(format!("{name} ({})", request.subject_id)))?;
        let root = branch
            .root
            .clone()
            .ok_or_else(|| CatalogError::OperationConflict(branch.id.to_string()))?;
        self.roots.verify(&root).await?;
        self.acquire(request, LeaseSubjectKind::Branch, branch.revision, root)
            .await
    }

    pub async fn acquire_checkpoint_by_name(
        &self,
        branch_id: Uuid,
        name: &str,
        request: LeaseAcquireRequest,
    ) -> Result<LeaseGrant, LeaseLifecycleError> {
        if request.access_mode != LeaseAccessMode::Read {
            return Err(
                CatalogError::Invalid("checkpoint leases must be read-only".to_string()).into(),
            );
        }
        if let Some(existing) = self
            .exact_retry(&request, LeaseSubjectKind::Checkpoint)
            .await?
        {
            let checkpoint = ImmutableCheckpoint::from_durable_root(&existing.lease.root)?;
            self.roots.verify_checkpoint(&checkpoint).await?;
            return Ok(existing);
        }
        validate_duration(request.duration)?;
        let checkpoint = self
            .catalog
            .checkpoint_by_name(branch_id, name)
            .await?
            .filter(|checkpoint| checkpoint.id == request.subject_id)
            .ok_or_else(|| CatalogError::NotFound(format!("{name} ({})", request.subject_id)))?;
        let exact = ImmutableCheckpoint::from_durable_root(&checkpoint.root)?;
        if exact.checkpoint_id != checkpoint.id {
            return Err(CatalogError::Corrupt(format!(
                "checkpoint {} root encodes a different UUID",
                checkpoint.id
            ))
            .into());
        }
        self.roots.verify_checkpoint(&exact).await?;
        self.acquire(
            request,
            LeaseSubjectKind::Checkpoint,
            checkpoint.revision,
            checkpoint.root,
        )
        .await
    }

    pub async fn renew(
        &self,
        request: LeaseRenewRequest,
    ) -> Result<LeaseRecord, LeaseLifecycleError> {
        validate_duration(request.duration)?;
        if request.renewal_token.is_nil() || request.renewal_token == request.lease_id {
            return Err(CatalogError::Invalid(
                "lease and renewal token UUIDs must be distinct and non-nil".to_string(),
            )
            .into());
        }
        let renewed_at = catalog_timestamp(Utc::now());
        self.catalog
            .apply(CatalogMutation::RenewLease {
                id: request.lease_id,
                expected_revision: request.expected_revision,
                token_hash: token_hash(request.renewal_token),
                renewed_at,
                expires_at: renewed_at + request.duration,
            })
            .await?;
        self.catalog
            .lease(request.lease_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(request.lease_id.to_string()).into())
    }

    pub async fn release(
        &self,
        lease_id: Uuid,
        expected_revision: u64,
        renewal_token: Uuid,
    ) -> Result<(), LeaseLifecycleError> {
        self.end(lease_id, expected_revision, token_hash(renewal_token))
            .await_at(catalog_timestamp(Utc::now()))
            .await
    }

    pub async fn expire(
        &self,
        lease_id: Uuid,
        expected_revision: u64,
    ) -> Result<(), LeaseLifecycleError> {
        self.expire_at(lease_id, expected_revision, catalog_timestamp(Utc::now()))
            .await
    }

    pub(crate) async fn expire_at(
        &self,
        lease_id: Uuid,
        expected_revision: u64,
        observed_at: DateTime<Utc>,
    ) -> Result<(), LeaseLifecycleError> {
        self.catalog
            .apply(CatalogMutation::ExpireLease {
                id: lease_id,
                expected_revision,
                observed_at,
            })
            .await?;
        Ok(())
    }

    async fn acquire(
        &self,
        request: LeaseAcquireRequest,
        subject_kind: LeaseSubjectKind,
        expected_subject_revision: u64,
        root: DurableRoot,
    ) -> Result<LeaseGrant, LeaseLifecycleError> {
        if request.renewal_token.is_nil() || request.renewal_token == request.lease_id {
            return Err(CatalogError::Invalid(
                "lease and renewal token UUIDs must be distinct and non-nil".to_string(),
            )
            .into());
        }
        let issued_at = catalog_timestamp(Utc::now());
        let lease = LeaseRecord {
            id: request.lease_id,
            revision: 1,
            subject_kind,
            subject_id: request.subject_id,
            root,
            access_mode: request.access_mode,
            token_hash: token_hash(request.renewal_token),
            issued_at,
            updated_at: issued_at,
            expires_at: issued_at + request.duration,
        };
        self.catalog
            .apply(CatalogMutation::AcquireLease {
                expected_subject_revision,
                lease,
            })
            .await?;
        let lease = self
            .catalog
            .lease(request.lease_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(request.lease_id.to_string()))?;
        Ok(LeaseGrant {
            lease,
            renewal_token: request.renewal_token,
        })
    }

    async fn exact_retry(
        &self,
        request: &LeaseAcquireRequest,
        subject_kind: LeaseSubjectKind,
    ) -> Result<Option<LeaseGrant>, LeaseLifecycleError> {
        let Some(lease) = self.catalog.lease(request.lease_id).await? else {
            return Ok(None);
        };
        if !lease.is_unexpired(catalog_timestamp(Utc::now())) {
            return Err(CatalogError::OperationConflict(format!(
                "lease {} is expired and cannot be resurrected",
                lease.id
            ))
            .into());
        }
        if lease.subject_kind != subject_kind
            || lease.subject_id != request.subject_id
            || lease.access_mode != request.access_mode
            || lease.token_hash != token_hash(request.renewal_token)
            || lease.revision != 1
            || lease.expires_at - lease.updated_at != request.duration
        {
            return Err(CatalogError::OperationConflict(request.lease_id.to_string()).into());
        }
        Ok(Some(LeaseGrant {
            lease,
            renewal_token: request.renewal_token,
        }))
    }

    fn end(&self, lease_id: Uuid, expected_revision: u64, token_hash: String) -> EndLease<'_> {
        EndLease {
            lifecycle: self,
            lease_id,
            expected_revision,
            token_hash,
        }
    }
}

struct EndLease<'a> {
    lifecycle: &'a LeaseLifecycle,
    lease_id: Uuid,
    expected_revision: u64,
    token_hash: String,
}

impl EndLease<'_> {
    async fn await_at(self, ended_at: DateTime<Utc>) -> Result<(), LeaseLifecycleError> {
        self.lifecycle
            .catalog
            .apply(CatalogMutation::EndLease {
                id: self.lease_id,
                expected_revision: self.expected_revision,
                token_hash: self.token_hash,
                ended_at,
            })
            .await?;
        Ok(())
    }
}

fn validate_duration(duration: Duration) -> Result<(), CatalogError> {
    if duration <= Duration::zero() || duration > MAX_LEASE_DURATION {
        return Err(CatalogError::Invalid(format!(
            "lease duration must be within 1..={} seconds",
            MAX_LEASE_DURATION.num_seconds()
        )));
    }
    Ok(())
}

fn token_hash(token: Uuid) -> String {
    format!("{:x}", Sha256::digest(token.as_bytes()))
}

#[derive(Debug, thiserror::Error)]
pub enum LeaseLifecycleError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    RootStore(#[from] RootStoreError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        BranchCreateOperation, BranchRecord, BranchState, CatalogSnapshot, CheckpointRecord,
        SlateDbCatalog, TombstoneRecord,
    };
    use object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;
    use std::io;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct LostRenewalResponseCatalog {
        inner: Arc<SlateDbCatalog>,
        fail_once: AtomicBool,
    }

    #[async_trait::async_trait]
    impl Catalog for LostRenewalResponseCatalog {
        async fn snapshot(&self) -> Result<CatalogSnapshot, CatalogError> {
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
        ) -> Result<Option<crate::catalog::BranchDeleteOperation>, CatalogError> {
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
            let is_renewal = matches!(mutation, CatalogMutation::RenewLease { .. });
            let generation = self.inner.apply(mutation).await?;
            if is_renewal && self.fail_once.swap(false, Ordering::SeqCst) {
                return Err(CatalogError::Io(io::Error::other(
                    "injected lost renewal response",
                )));
            }
            Ok(generation)
        }
    }

    #[tokio::test]
    async fn renewal_reconciles_an_applied_write_with_a_lost_response() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let inner = Arc::new(
            SlateDbCatalog::open(Path::from("lost-renewal/catalog"), Arc::clone(&store))
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
                identity: "lost-renewal/root".to_string(),
                manifest_id: "checkpoint@1".to_string(),
            }),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        };
        inner
            .apply(CatalogMutation::CreateBranch(branch.clone()))
            .await
            .unwrap();
        let lease = LeaseRecord {
            id: Uuid::new_v4(),
            revision: 1,
            subject_kind: LeaseSubjectKind::Branch,
            subject_id: branch.id,
            root: branch.root.unwrap(),
            access_mode: LeaseAccessMode::Read,
            token_hash: token_hash(Uuid::from_u128(1)),
            issued_at: now,
            updated_at: now,
            expires_at: now + Duration::minutes(4),
        };
        inner
            .apply(CatalogMutation::AcquireLease {
                expected_subject_revision: branch.revision,
                lease: lease.clone(),
            })
            .await
            .unwrap();
        let catalog: Arc<dyn Catalog> = Arc::new(LostRenewalResponseCatalog {
            inner: Arc::clone(&inner),
            fail_once: AtomicBool::new(true),
        });
        let lifecycle = LeaseLifecycle::new(
            catalog,
            SlateDbRootStore::new(store, Path::from("lost-renewal/branches")),
        );
        let request = LeaseRenewRequest {
            lease_id: lease.id,
            expected_revision: lease.revision,
            renewal_token: Uuid::from_u128(1),
            duration: Duration::minutes(5),
        };
        assert!(matches!(
            lifecycle.renew(request.clone()).await,
            Err(LeaseLifecycleError::Catalog(CatalogError::Io(_)))
        ));
        let recovered = lifecycle.renew(request.clone()).await.unwrap();
        assert_eq!(recovered.revision, 2);
        assert_eq!(lifecycle.renew(request.clone()).await.unwrap(), recovered);
        assert!(matches!(
            inner
                .apply(CatalogMutation::RenewLease {
                    id: request.lease_id,
                    expected_revision: request.expected_revision,
                    token_hash: token_hash(request.renewal_token),
                    renewed_at: recovered.expires_at,
                    expires_at: recovered.expires_at + request.duration,
                })
                .await,
            Err(CatalogError::RevisionConflict { .. } | CatalogError::OperationConflict(_))
        ));
        assert_eq!(inner.snapshot().await.unwrap().generation, 3);
        inner.close().await.unwrap();
    }
}
