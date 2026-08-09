use crate::bucket_identity::BucketIdentity;
use crate::config::Settings;
use crate::key_management;
use crate::parse_object_store::parse_url_opts;
use crate::segment_store::SegmentStore;
use crate::storage_class_object_store::with_storage_class;
use anyhow::{Context, Result};
use object_store::ObjectStore;
use object_store::path::Path;
use std::path::Path as FsPath;
use std::sync::Arc;

fn paths_overlap(left: &Path, right: &Path) -> bool {
    fn contains(parent: &str, child: &str) -> bool {
        child == parent
            || child
                .strip_prefix(parent)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }
    let left = left.to_string();
    let right = right.to_string();
    contains(&left, &right) || contains(&right, &left)
}

pub async fn run(config_path: &FsPath, confirm_offline: bool) -> Result<()> {
    if !confirm_offline {
        anyhow::bail!(
            "legacy migration requires --confirm-offline after every source and target-pool reader, writer, GC, and maintenance process has stopped and will remain stopped through migration completion and the first successful root-admitting startup"
        );
    }
    let settings = Settings::from_file(config_path).context("Failed to load configuration")?;
    if settings.replication.is_some() {
        anyhow::bail!("legacy migration requires replication to be disabled while offline");
    }
    let pool = settings
        .storage
        .segment_pool_path
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("storage.segment_pool_path is required"))?;
    let (object_store, database_path) = parse_url_opts(
        &settings
            .storage
            .url
            .parse()
            .context("Failed to parse storage URL")?,
        settings.cloud_provider_env_vars(),
    )
    .context("Failed to connect to storage backend")?;
    let object_store = with_storage_class(
        Arc::from(object_store),
        settings.storage.storage_class.as_deref(),
    );
    let pool_path = Path::parse(pool).context("Invalid storage.segment_pool_path")?;
    if paths_overlap(&database_path, &pool_path) {
        anyhow::bail!("storage.segment_pool_path must be disjoint from the legacy database path");
    }
    crate::cli::password::validate_password(&settings.storage.encryption_password)
        .context("Password validation failed")?;
    let database_instance_id = BucketIdentity::get_or_create(&object_store, database_path.as_ref())
        .await
        .context("Failed to resolve the immutable legacy database identity")?
        .id();
    let pool_store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        Arc::clone(&object_store),
        pool_path.clone(),
    ));
    SegmentStore::mark_legacy_pool_bootstrap(
        &pool_store,
        database_path.as_ref(),
        database_instance_id,
    )
    .await
    .context("Failed to publish the fail-closed legacy migration bootstrap")?;
    let (master_key, wrapped_key_digest) = key_management::prepare_legacy_shared_key(
        &object_store,
        &database_path,
        &pool_path,
        &settings.storage.encryption_password,
    )
    .await
    .context("Failed to preserve the exact legacy volume key")?;
    let authority = SegmentStore::open_or_create_legacy_pool_authority(
        Arc::clone(&pool_store),
        &master_key,
        database_path.as_ref(),
        database_instance_id,
        wrapped_key_digest,
    )
    .await
    .context("Failed to establish authenticated shared-pool authority")?;
    let report = SegmentStore::migrate_legacy_segments(
        Arc::clone(&object_store),
        &authority,
        &database_path,
        &pool_path,
        database_instance_id,
        wrapped_key_digest,
    )
    .await
    .context("Legacy segment migration failed")?;
    SegmentStore::validate_epoch_reservations(pool_store, &authority)
        .await
        .context("Imported pool contains an unreserved epoch")?;
    let fingerprint = report
        .inventory_fingerprint
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    println!(
        "Legacy migration {} completed: {} segments, {} bytes, inventory {}",
        report.migration_id, report.segment_count, report.total_bytes, fingerprint
    );
    println!(
        "The legacy segment objects were retained as an offline rollback copy; configured serving now authenticates the shared-pool completion."
    );
    println!(
        "Keep all target-pool serving, GC, and maintenance stopped until this database completes its first normal root-admitting startup."
    );
    Ok(())
}
