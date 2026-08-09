use super::init::open_catalog_runtime;
use super::server::{DatabaseMode, parse_wal_object_store};
use crate::config::Settings;
use crate::parse_object_store::parse_url_opts;
use crate::storage_class_object_store::with_storage_class;
use anyhow::{Context, Result};
use serde_json::json;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

/// Run the terminal shadow-GC pipeline while the volume is offline.
///
/// This entry point intentionally exposes only capture, mark, and report. It
/// cannot quarantine, revalidate, or physically delete an object.
pub async fn report(config_path: &Path, run_id: Option<Uuid>, confirm_offline: bool) -> Result<()> {
    if !confirm_offline {
        anyhow::bail!(
            "global GC reporting requires --confirm-offline after every volume reader, writer, GC, and maintenance process has stopped"
        );
    }

    let settings = Settings::from_file(config_path).context("Failed to load configuration")?;
    let catalog = settings
        .catalog
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("global GC reporting requires [catalog]"))?;
    let segment_pool_path = settings
        .storage
        .segment_pool_path
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("global GC reporting requires storage.segment_pool_path"))?;

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
    let wal_object_store = settings
        .wal
        .as_ref()
        .map(parse_wal_object_store)
        .transpose()
        .context("Failed to connect to WAL storage backend")?;
    let runtime = open_catalog_runtime(
        Some(catalog),
        object_store,
        wal_object_store,
        database_path.as_ref(),
        Some(segment_pool_path),
        DatabaseMode::ReadWrite,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("global GC reporting requires an authoritative catalog"))?;

    let run_id = run_id.unwrap_or_else(Uuid::new_v4);
    println!(
        "{}",
        json!({
            "event": "global_gc_report_started",
            "run_id": run_id,
            "mode": "offline_mark_only",
            "physical_deletion_capability": false,
        })
    );

    let result = runtime.global_gc_report(run_id).await;
    let close_result = runtime.close().await;
    let run = match (result, close_result) {
        (Ok(run), Ok(())) => run,
        (Err(error), Ok(())) => return Err(error),
        (Ok(_), Err(error)) => return Err(error),
        (Err(error), Err(close_error)) => {
            return Err(error.context(format!("catalog close also failed: {close_error:#}")));
        }
    };
    let mark = run
        .mark_stats
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("reported GC run is missing mark statistics"))?;
    let inventory = run
        .inventory_stats
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("reported GC run is missing inventory statistics"))?;

    println!(
        "{}",
        json!({
            "event": "global_gc_report_completed",
            "run_id": run.id,
            "revision": run.revision,
            "catalog_generation": run.catalog_generation,
            "inventory_cutoff": run.inventory_cutoff,
            "root_digest": run.root_digest,
            "roots_enumerated": mark.roots_enumerated,
            "references_enumerated": mark.references_enumerated,
            "unique_segments": mark.unique_segments,
            "objects_seen": inventory.objects_seen,
            "objects_newer_than_cutoff": inventory.objects_newer_than_cutoff,
            "reachable_objects": inventory.reachable_objects,
            "candidate_objects": inventory.candidate_objects,
            "candidate_bytes": inventory.candidate_bytes,
            "mode": "offline_mark_only",
            "physical_deletion_capability": false,
        })
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn offline_confirmation_is_required_before_config_is_read() {
        let missing = Path::new("this-global-gc-config-must-not-exist.toml");
        let error = report(missing, None, false).await.unwrap_err().to_string();
        assert!(error.contains("--confirm-offline"), "{error}");
        assert!(!error.contains("load configuration"), "{error}");
    }
}
