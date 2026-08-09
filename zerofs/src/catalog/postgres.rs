use super::{
    CATALOG_PROJECTION_SCHEMA_VERSION, CatalogError, CatalogProjection, CatalogSnapshot,
    CustomerCatalogListRequest, CustomerCatalogPage, CustomerCatalogRecord, CustomerMetadata,
    CustomerResourceKind, TombstoneKind, validate_metadata,
};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Row, Transaction, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;
use uuid::Uuid;

const SCHEMA: &str = include_str!("../../migrations/0001_catalog_projection.sql");

/// PostgreSQL customer-facing projection of the authoritative SlateDB catalog.
/// This schema deliberately has no durable-root or manifest columns.
pub struct PostgresCatalogProjection {
    read_client: Option<Client>,
    write_client: Arc<Mutex<Client>>,
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
        }
    }

    pub fn from_clients(read_client: Client, write_client: Client) -> Self {
        Self {
            read_client: Some(read_client),
            write_client: Arc::new(Mutex::new(write_client)),
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
        if generation <= observed {
            transaction.commit().await?;
            return Ok(());
        }

        let mut present = BTreeSet::new();
        for branch in snapshot.branches.values() {
            present.insert(branch.id);
            upsert_resource(
                &transaction,
                volume_id,
                branch.id,
                CustomerResourceKind::Branch,
                &branch.name,
                branch.state.as_str(),
                branch.parent_id,
                branch.origin_checkpoint_id,
                generation,
                branch.created_at,
                branch.updated_at,
                None,
                false,
            )
            .await?;
        }
        for checkpoint in snapshot.checkpoints.values() {
            present.insert(checkpoint.id);
            upsert_resource(
                &transaction,
                volume_id,
                checkpoint.id,
                CustomerResourceKind::Checkpoint,
                &checkpoint.name,
                "ready",
                Some(checkpoint.branch_id),
                None,
                generation,
                checkpoint.created_at,
                checkpoint.updated_at,
                None,
                false,
            )
            .await?;
        }
        for tombstone in snapshot.tombstones.values() {
            present.insert(tombstone.id);
            let kind = match tombstone.kind {
                TombstoneKind::Branch => CustomerResourceKind::Branch,
                TombstoneKind::Checkpoint => CustomerResourceKind::Checkpoint,
            };
            upsert_resource(
                &transaction,
                volume_id,
                tombstone.id,
                kind,
                &tombstone.name,
                "deleted",
                tombstone.parent_id,
                tombstone.origin_checkpoint_id,
                generation,
                tombstone.created_at,
                tombstone.deleted_at,
                Some(tombstone.deleted_at),
                false,
            )
            .await?;
        }
        let rows = transaction
            .query(
                "SELECT resource_id FROM zerofs_catalog_projection_resources WHERE volume_id = $1",
                &[&volume_id],
            )
            .await?;
        for row in rows {
            let resource_id: Uuid = row.get(0);
            if !present.contains(&resource_id) {
                transaction
                    .execute(
                        "UPDATE zerofs_catalog_projection_resources \
                         SET state = 'absent', observed_generation = $3 \
                         WHERE volume_id = $1 AND resource_id = $2",
                        &[&volume_id, &resource_id, &generation],
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

#[allow(clippy::too_many_arguments)]
async fn upsert_resource(
    transaction: &Transaction<'_>,
    volume_id: Uuid,
    resource_id: Uuid,
    kind: CustomerResourceKind,
    name: &str,
    state: &str,
    parent_id: Option<Uuid>,
    origin_checkpoint_id: Option<Uuid>,
    generation: i64,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    deleted_at: Option<chrono::DateTime<chrono::Utc>>,
    preserve_lineage: bool,
) -> Result<(), CatalogError> {
    transaction
        .execute(
            "INSERT INTO zerofs_catalog_projection_resources \
             (volume_id, resource_id, kind, name, state, parent_id, origin_checkpoint_id, \
              observed_generation, created_at, updated_at, deleted_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
             ON CONFLICT (volume_id, resource_id) DO UPDATE SET \
             kind=EXCLUDED.kind, name=EXCLUDED.name, state=EXCLUDED.state, \
             parent_id=CASE WHEN $12 THEN COALESCE(EXCLUDED.parent_id, zerofs_catalog_projection_resources.parent_id) ELSE EXCLUDED.parent_id END, \
             origin_checkpoint_id=CASE WHEN $12 THEN COALESCE(EXCLUDED.origin_checkpoint_id, zerofs_catalog_projection_resources.origin_checkpoint_id) ELSE EXCLUDED.origin_checkpoint_id END, \
             observed_generation=EXCLUDED.observed_generation, \
             created_at=LEAST(zerofs_catalog_projection_resources.created_at, EXCLUDED.created_at), \
             updated_at=EXCLUDED.updated_at, deleted_at=EXCLUDED.deleted_at",
            &[
                &volume_id,
                &resource_id,
                &kind.as_str(),
                &name,
                &state,
                &parent_id,
                &origin_checkpoint_id,
                &generation,
                &created_at,
                &updated_at,
                &deleted_at,
                &preserve_lineage,
            ],
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
