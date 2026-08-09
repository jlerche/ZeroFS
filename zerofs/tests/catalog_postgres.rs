use chrono::Utc;
use serde_json::Value;
use tokio_postgres::NoTls;
use uuid::Uuid;
use zerofs::catalog::{
    BranchRecord, BranchState, CatalogProjection, CatalogSnapshot, CustomerMetadata, DurableRoot,
    PostgresCatalogProjection, RetiredCatalogId, RetiredCatalogKind, TombstoneKind,
    TombstoneRecord, catalog_timestamp,
};

async fn connect(url: &str) -> (PostgresCatalogProjection, tokio_postgres::Client) {
    let (projection_client, projection_connection) =
        tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { projection_connection.await.unwrap() });
    let (inspection_client, inspection_connection) =
        tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { inspection_connection.await.unwrap() });
    (
        PostgresCatalogProjection::from_client(projection_client),
        inspection_client,
    )
}

#[tokio::test]
#[ignore = "requires ZEROFS_TEST_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn projection_reconciles_without_storage_secrets_and_preserves_metadata() {
    let url = std::env::var("ZEROFS_TEST_POSTGRES_URL").unwrap();
    let (projection, inspection) = connect(&url).await;
    projection.migrate().await.unwrap();
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
                identity: "storage-root-secret".to_string(),
                manifest_id: "manifest-secret".to_string(),
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
    assert_eq!(record.observed_generation, 2);

    let deleted_at = catalog_timestamp(Utc::now());
    snapshot.generation = 3;
    snapshot.branches.clear();
    snapshot.tombstones.insert(
        branch_id,
        TombstoneRecord {
            id: branch_id,
            kind: TombstoneKind::Branch,
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
        RetiredCatalogId {
            id: branch_id,
            kind: RetiredCatalogKind::Branch,
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

    let forbidden_columns = inspection
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = 'zerofs_catalog_projection_resources' \
             AND column_name IN ('root', 'root_identity', 'root_manifest_id', 'manifest_id')",
            &[],
        )
        .await
        .unwrap();
    assert!(forbidden_columns.is_empty());
    let leaked = inspection
        .query_one(
            "SELECT COUNT(*) FROM zerofs_catalog_projection_resources \
             WHERE customer_metadata::text LIKE '%storage-root-secret%' \
                OR customer_metadata::text LIKE '%manifest-secret%'",
            &[],
        )
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(leaked, 0);
}
