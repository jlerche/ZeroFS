use super::{
    CATALOG_PROJECTION_SCHEMA_VERSION, CatalogError, CatalogProjection, CatalogSnapshot,
    CustomerCatalogListRequest, CustomerCatalogPage, CustomerCatalogRecord, CustomerMetadata,
    CustomerResourceKind, TombstoneKind, validate_metadata,
};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Row, Transaction, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;
use uuid::Uuid;

const SCHEMA: &str = include_str!("../../migrations/0001_catalog_projection.sql");
const PROJECTION_BATCH_ROWS: usize = 512;

fn delta_base_matches(previous: Option<&CatalogSnapshot>, observed_generation: i64) -> bool {
    previous.is_some_and(|snapshot| {
        i64::try_from(snapshot.generation).ok() == Some(observed_generation)
    })
}

/// PostgreSQL customer-facing projection of the authoritative SlateDB catalog.
/// This schema deliberately has no durable-root or manifest columns.
pub struct PostgresCatalogProjection {
    read_client: Option<Client>,
    write_client: Arc<Mutex<Client>>,
    committed_snapshots: Arc<Mutex<BTreeMap<Uuid, Arc<CatalogSnapshot>>>>,
}

impl std::fmt::Debug for PostgresCatalogProjection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresCatalogProjection")
            .finish_non_exhaustive()
    }
}

impl PostgresCatalogProjection {
    pub async fn connect(connection_string: &str) -> Result<Self, CatalogError> {
        Self::connect_with_tls(connection_string, true).await
    }

    /// Connect with secure transport by default, with an explicit plaintext
    /// escape hatch for isolated local test databases.
    pub async fn connect_with_tls(
        connection_string: &str,
        tls: bool,
    ) -> Result<Self, CatalogError> {
        let mut config = connection_string.parse::<tokio_postgres::Config>()?;
        if !tls {
            config.ssl_mode(SslMode::Disable);
            let (read_client, read_connection) = config.connect(NoTls).await?;
            tokio::spawn(async move {
                if let Err(error) = read_connection.await {
                    tracing::error!(%error, "catalog projection read connection failed");
                }
            });
            let (write_client, write_connection) = config.connect(NoTls).await?;
            tokio::spawn(async move {
                if let Err(error) = write_connection.await {
                    tracing::error!(%error, "catalog projection write connection failed");
                }
            });
            return Ok(Self::from_clients(read_client, write_client));
        }
        config.ssl_mode(SslMode::Require);
        let (tls, certificate_errors) =
            MakeRustlsConnect::with_native_certs().map_err(|errors| {
                CatalogError::PostgresTls(format!(
                    "could not load a usable platform trust store: {errors:?}"
                ))
            })?;
        if !certificate_errors.is_empty() {
            tracing::warn!(errors = ?certificate_errors, "some PostgreSQL trust anchors failed to load");
        }
        let (read_client, read_connection) = config.connect(tls.clone()).await?;
        tokio::spawn(async move {
            if let Err(error) = read_connection.await {
                tracing::error!(%error, "catalog projection read connection failed");
            }
        });
        let (write_client, write_connection) = config.connect(tls).await?;
        tokio::spawn(async move {
            if let Err(error) = write_connection.await {
                tracing::error!(%error, "catalog projection write connection failed");
            }
        });
        Ok(Self::from_clients(read_client, write_client))
    }

    pub fn from_client(client: Client) -> Self {
        Self {
            read_client: None,
            write_client: Arc::new(Mutex::new(client)),
            committed_snapshots: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn from_clients(read_client: Client, write_client: Client) -> Self {
        Self {
            read_client: Some(read_client),
            write_client: Arc::new(Mutex::new(write_client)),
            committed_snapshots: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn migrate(&self) -> Result<(), CatalogError> {
        self.write_client.lock().await.batch_execute(SCHEMA).await?;
        Ok(())
    }
}

#[async_trait]
impl CatalogProjection for PostgresCatalogProjection {
    async fn reconcile(
        &self,
        volume_id: Uuid,
        snapshot: &CatalogSnapshot,
    ) -> Result<(), CatalogError> {
        validate_volume_id(volume_id)?;
        snapshot.validate()?;
        let generation = i64::try_from(snapshot.generation)
            .map_err(|_| CatalogError::Invalid("projection generation exceeds BIGINT".into()))?;
        let mut client = self.write_client.lock().await;
        let previous = self
            .committed_snapshots
            .lock()
            .await
            .get(&volume_id)
            .cloned();
        let transaction = client.transaction().await?;
        transaction
            .execute(
                "INSERT INTO zerofs_catalog_projection_state \
                 (volume_id, schema_version, observed_generation) VALUES ($1, $2, -1) \
                 ON CONFLICT (volume_id) DO NOTHING",
                &[&volume_id, &(CATALOG_PROJECTION_SCHEMA_VERSION as i32)],
            )
            .await?;
        let state = transaction
            .query_one(
                "SELECT schema_version, observed_generation \
                 FROM zerofs_catalog_projection_state WHERE volume_id = $1 FOR UPDATE",
                &[&volume_id],
            )
            .await?;
        let schema_version: i32 = state.get(0);
        let observed: i64 = state.get(1);
        if schema_version != CATALOG_PROJECTION_SCHEMA_VERSION as i32 {
            return Err(CatalogError::Corrupt(format!(
                "unsupported PostgreSQL projection schema {schema_version}"
            )));
        }
        let full_reconcile = !delta_base_matches(previous.as_deref(), observed);
        if generation <= observed {
            transaction.commit().await?;
            if generation == observed {
                let mut committed = self.committed_snapshots.lock().await;
                if committed
                    .get(&volume_id)
                    .is_none_or(|prior| prior.generation <= snapshot.generation)
                {
                    committed.insert(volume_id, Arc::new(snapshot.clone()));
                }
            }
            return Ok(());
        }

        let mut batch = Vec::with_capacity(PROJECTION_BATCH_ROWS);
        for record in projection_records(snapshot, previous.as_deref(), full_reconcile) {
            batch.push(record);
            if batch.len() == PROJECTION_BATCH_ROWS {
                upsert_resource_batch(&transaction, volume_id, generation, &batch).await?;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            upsert_resource_batch(&transaction, volume_id, generation, &batch).await?;
        }
        if full_reconcile {
            transaction
                .execute(
                    "UPDATE zerofs_catalog_projection_resources \
                     SET state = 'absent', observed_generation = $2 \
                     WHERE volume_id = $1 AND observed_generation <> $2",
                    &[&volume_id, &generation],
                )
                .await?;
        } else if let Some(previous) = &previous {
            let removed = previous
                .branches
                .keys()
                .chain(previous.checkpoints.keys())
                .chain(previous.tombstones.keys())
                .filter(|id| {
                    !snapshot.branches.contains_key(id)
                        && !snapshot.checkpoints.contains_key(id)
                        && !snapshot.tombstones.contains_key(id)
                })
                .copied()
                .collect::<Vec<_>>();
            for removed in removed.chunks(PROJECTION_BATCH_ROWS) {
                transaction
                    .execute(
                        "UPDATE zerofs_catalog_projection_resources \
                         SET state = 'absent', observed_generation = $3 \
                         WHERE volume_id = $1 AND resource_id = ANY($2::uuid[])",
                        &[&volume_id, &removed, &generation],
                    )
                    .await?;
            }
        }
        transaction
            .execute(
                "UPDATE zerofs_catalog_projection_state SET observed_generation = $2 \
                 WHERE volume_id = $1",
                &[&volume_id, &generation],
            )
            .await?;
        transaction.commit().await?;
        let mut committed = self.committed_snapshots.lock().await;
        if committed
            .get(&volume_id)
            .is_none_or(|prior| prior.generation <= snapshot.generation)
        {
            committed.insert(volume_id, Arc::new(snapshot.clone()));
        }
        Ok(())
    }

    async fn record(
        &self,
        volume_id: Uuid,
        resource_id: Uuid,
    ) -> Result<Option<CustomerCatalogRecord>, CatalogError> {
        validate_volume_id(volume_id)?;
        let query = "SELECT volume_id, resource_id, kind, name, state, parent_id, \
                     origin_checkpoint_id, observed_generation, created_at, updated_at, \
                     deleted_at, customer_metadata \
                     FROM zerofs_catalog_projection_resources \
                     WHERE volume_id = $1 AND resource_id = $2";
        let row = if let Some(client) = &self.read_client {
            client.query_opt(query, &[&volume_id, &resource_id]).await?
        } else {
            self.write_client
                .lock()
                .await
                .query_opt(query, &[&volume_id, &resource_id])
                .await?
        };
        row.as_ref().map(record_from_row).transpose()
    }

    async fn list(
        &self,
        volume_id: Uuid,
        request: CustomerCatalogListRequest,
    ) -> Result<CustomerCatalogPage, CatalogError> {
        validate_volume_id(volume_id)?;
        request.validate()?;
        let query = "SELECT volume_id, resource_id, kind, name, state, parent_id, \
                     origin_checkpoint_id, observed_generation, created_at, updated_at, \
                     deleted_at, customer_metadata \
                     FROM zerofs_catalog_projection_resources \
                     WHERE volume_id = $1 \
                       AND ($2::text IS NULL OR kind = $2) \
                       AND ($3::uuid IS NULL OR parent_id = $3) \
                       AND ($4::text IS NULL OR state = $4) \
                       AND ($5::uuid IS NULL OR resource_id > $5) \
                     ORDER BY resource_id LIMIT $6";
        let kind = request.kind.map(CustomerResourceKind::as_str);
        let limit = i64::try_from(request.limit + 1)
            .expect("bounded customer catalog page size fits BIGINT");
        let rows = if let Some(client) = &self.read_client {
            client
                .query(
                    query,
                    &[
                        &volume_id,
                        &kind,
                        &request.parent_id,
                        &request.state,
                        &request.after,
                        &limit,
                    ],
                )
                .await?
        } else {
            self.write_client
                .lock()
                .await
                .query(
                    query,
                    &[
                        &volume_id,
                        &kind,
                        &request.parent_id,
                        &request.state,
                        &request.after,
                        &limit,
                    ],
                )
                .await?
        };
        let mut records = rows
            .iter()
            .map(record_from_row)
            .collect::<Result<Vec<_>, _>>()?;
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
        let metadata = Value::Object(metadata);
        let updated = self
            .write_client
            .lock()
            .await
            .execute(
                "UPDATE zerofs_catalog_projection_resources SET customer_metadata = $3 \
                 WHERE volume_id = $1 AND resource_id = $2",
                &[&volume_id, &resource_id, &metadata],
            )
            .await?;
        if updated == 0 {
            return Err(CatalogError::NotFound(resource_id.to_string()));
        }
        Ok(())
    }
}

fn projection_records<'a>(
    snapshot: &'a CatalogSnapshot,
    previous: Option<&'a CatalogSnapshot>,
    full_reconcile: bool,
) -> impl Iterator<Item = Value> + 'a {
    let branches = snapshot.branches.values().filter_map(move |branch| {
        if !full_reconcile
            && previous.is_some_and(|prior| prior.branches.get(&branch.id) == Some(branch))
        {
            return None;
        }
        Some(serde_json::json!({
            "resource_id": branch.id,
            "kind": CustomerResourceKind::Branch.as_str(),
            "name": branch.name,
            "state": branch.state.as_str(),
            "parent_id": branch.parent_id,
            "origin_checkpoint_id": branch.origin_checkpoint_id,
            "created_at": branch.created_at,
            "updated_at": branch.updated_at,
            "deleted_at": null,
        }))
    });
    let checkpoints = snapshot.checkpoints.values().filter_map(move |checkpoint| {
        if !full_reconcile
            && previous
                .is_some_and(|prior| prior.checkpoints.get(&checkpoint.id) == Some(checkpoint))
        {
            return None;
        }
        Some(serde_json::json!({
            "resource_id": checkpoint.id,
            "kind": CustomerResourceKind::Checkpoint.as_str(),
            "name": checkpoint.name,
            "state": "ready",
            "parent_id": checkpoint.branch_id,
            "origin_checkpoint_id": null,
            "created_at": checkpoint.created_at,
            "updated_at": checkpoint.updated_at,
            "deleted_at": null,
        }))
    });
    let tombstones = snapshot.tombstones.values().filter_map(move |tombstone| {
        if !full_reconcile
            && previous.is_some_and(|prior| prior.tombstones.get(&tombstone.id) == Some(tombstone))
        {
            return None;
        }
        let kind = match tombstone.kind {
            TombstoneKind::Branch => CustomerResourceKind::Branch,
            TombstoneKind::Checkpoint => CustomerResourceKind::Checkpoint,
        };
        Some(serde_json::json!({
            "resource_id": tombstone.id,
            "kind": kind.as_str(),
            "name": tombstone.name,
            "state": "deleted",
            "parent_id": tombstone.parent_id,
            "origin_checkpoint_id": tombstone.origin_checkpoint_id,
            "created_at": tombstone.created_at,
            "updated_at": tombstone.deleted_at,
            "deleted_at": tombstone.deleted_at,
        }))
    });
    branches.chain(checkpoints).chain(tombstones)
}

async fn upsert_resource_batch(
    transaction: &Transaction<'_>,
    volume_id: Uuid,
    generation: i64,
    records: &[Value],
) -> Result<(), CatalogError> {
    debug_assert!(!records.is_empty() && records.len() <= PROJECTION_BATCH_ROWS);
    transaction
        .execute(
            "INSERT INTO zerofs_catalog_projection_resources \
             (volume_id, resource_id, kind, name, state, parent_id, origin_checkpoint_id, \
              observed_generation, created_at, updated_at, deleted_at) \
             SELECT $1, row.resource_id, row.kind, row.name, row.state, row.parent_id, \
                    row.origin_checkpoint_id, $3, row.created_at, row.updated_at, row.deleted_at \
             FROM jsonb_to_recordset($2::jsonb) AS row( \
                 resource_id uuid, kind text, name text, state text, parent_id uuid, \
                 origin_checkpoint_id uuid, created_at timestamptz, updated_at timestamptz, \
                 deleted_at timestamptz) \
             ON CONFLICT (volume_id, resource_id) DO UPDATE SET \
             kind=EXCLUDED.kind, name=EXCLUDED.name, state=EXCLUDED.state, \
             parent_id=EXCLUDED.parent_id, origin_checkpoint_id=EXCLUDED.origin_checkpoint_id, \
             observed_generation=EXCLUDED.observed_generation, \
             created_at=LEAST(zerofs_catalog_projection_resources.created_at, EXCLUDED.created_at), \
             updated_at=EXCLUDED.updated_at, deleted_at=EXCLUDED.deleted_at",
            &[&volume_id, &Value::Array(records.to_vec()), &generation],
        )
        .await?;
    Ok(())
}

fn record_from_row(row: &Row) -> Result<CustomerCatalogRecord, CatalogError> {
    let generation: i64 = row.get(7);
    if generation < 0 {
        return Err(CatalogError::Corrupt("negative observed generation".into()));
    }
    let metadata: Value = row.get(11);
    let customer_metadata = match metadata {
        Value::Object(metadata) => metadata,
        _ => {
            return Err(CatalogError::Corrupt(
                "customer metadata is not an object".into(),
            ));
        }
    };
    Ok(CustomerCatalogRecord {
        volume_id: row.get(0),
        resource_id: row.get(1),
        kind: match row.get::<_, &str>(2) {
            "branch" => CustomerResourceKind::Branch,
            "checkpoint" => CustomerResourceKind::Checkpoint,
            other => {
                return Err(CatalogError::Corrupt(format!(
                    "unknown resource kind {other}"
                )));
            }
        },
        name: row.get(3),
        state: row.get(4),
        parent_id: row.get(5),
        origin_checkpoint_id: row.get(6),
        observed_generation: generation as u64,
        created_at: row.get(8),
        updated_at: row.get(9),
        deleted_at: row.get(10),
        customer_metadata,
    })
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

    #[test]
    fn stale_process_cache_forces_full_reconciliation() {
        let cached = CatalogSnapshot {
            generation: 7,
            ..CatalogSnapshot::default()
        };

        assert!(delta_base_matches(Some(&cached), 7));
        assert!(!delta_base_matches(Some(&cached), 8));
        assert!(!delta_base_matches(None, 8));
        assert!(!delta_base_matches(Some(&cached), -1));
    }

    #[test]
    fn large_projection_is_partitioned_into_bounded_batches() {
        use crate::catalog::{BranchRecord, BranchState, DurableRoot};

        let mut snapshot = CatalogSnapshot {
            generation: 1,
            ..CatalogSnapshot::default()
        };
        let now = chrono::Utc::now();
        for index in 0..(PROJECTION_BATCH_ROWS * 2 + 1) {
            let id = Uuid::from_u128(index as u128 + 1);
            snapshot.branches.insert(
                id,
                BranchRecord {
                    id,
                    revision: 1,
                    name: format!("branch-{index}"),
                    state: BranchState::Ready,
                    root: Some(DurableRoot {
                        identity: format!("root-{index}"),
                        manifest_id: "checkpoint@1".to_string(),
                    }),
                    parent_id: None,
                    origin_checkpoint_id: None,
                    created_at: now,
                    updated_at: now,
                },
            );
        }

        let records = projection_records(&snapshot, None, true).collect::<Vec<_>>();
        let batches = records.chunks(PROJECTION_BATCH_ROWS).collect::<Vec<_>>();
        assert_eq!(batches.len(), 3);
        assert!(
            batches
                .iter()
                .all(|batch| !batch.is_empty() && batch.len() <= PROJECTION_BATCH_ROWS)
        );
        assert_eq!(batches.last().unwrap().len(), 1);
    }
}
