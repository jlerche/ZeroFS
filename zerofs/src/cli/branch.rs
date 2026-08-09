use super::init::open_catalog_runtime;
use super::server::{DatabaseMode, parse_wal_object_store};
use crate::cli::connect_rpc_client;
use crate::config::Settings;
use crate::parse_object_store::parse_url_opts;
use crate::storage_class_object_store::with_storage_class;
use anyhow::{Context, Result, bail};
use slatedb::admin::AdminBuilder;
use slatedb::object_store::path::Path as SlatePath;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;
use zerofs::catalog::{CustomerCatalogRecord, ImmutableCheckpoint, InitialBranchCreateRequest};

pub async fn bootstrap_branch(
    config_path: &Path,
    name: &str,
    source_checkpoint: &str,
    branch_id: Option<Uuid>,
    operation_id: Option<Uuid>,
    confirm_offline: bool,
) -> Result<()> {
    if !confirm_offline {
        bail!(
            "initial branch bootstrap requires --confirm-offline after the source volume and every catalog, GC, and maintenance process has stopped"
        );
    }
    let branch_id = branch_id.unwrap_or_else(Uuid::new_v4);
    let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
    println!("Exact retry: --id {branch_id} --operation-id {operation_id}");
    let settings = Settings::from_file(config_path).context("Failed to load configuration")?;
    let catalog = settings
        .catalog
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("initial branch bootstrap requires [catalog]"))?;
    if catalog.mount.is_some() {
        bail!("initial branch bootstrap configuration cannot contain [catalog.mount]");
    }
    let segment_pool_path = settings
        .storage
        .segment_pool_path
        .as_deref()
        .ok_or_else(|| {
            anyhow::anyhow!("initial branch bootstrap requires storage.segment_pool_path")
        })?;
    zerofs::catalog::validate_resource_name(name)
        .map_err(|error| anyhow::anyhow!("Invalid branch name: {error}"))?;
    zerofs::catalog::validate_resource_name(source_checkpoint)
        .map_err(|error| anyhow::anyhow!("Invalid checkpoint name: {error}"))?;

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
        Arc::clone(&object_store),
        wal_object_store.clone(),
        database_path.as_ref(),
        Some(segment_pool_path),
        DatabaseMode::ReadWrite,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("initial branch bootstrap requires an authoritative catalog"))?;
    match runtime
        .resume_initial_branch(operation_id, branch_id, name)
        .await
    {
        Ok(Some(branch)) => {
            runtime.close().await?;
            println!(
                "Branch '{}' ({}) is ready (operation {})",
                branch.name, branch.id, operation_id
            );
            return Ok(());
        }
        Ok(None) => {}
        Err(error) => {
            let close_result = runtime.close().await;
            return match close_result {
                Ok(()) => Err(error),
                Err(close_error) => {
                    Err(error.context(format!("catalog close also failed: {close_error:#}")))
                }
            };
        }
    }

    let result = async {
        let source_path = SlatePath::from(database_path.to_string());
        let mut admin = AdminBuilder::new(source_path.clone(), Arc::clone(&object_store));
        if let Some(wal) = &wal_object_store {
            admin = admin.with_wal_object_store(Arc::clone(wal));
        }
        let checkpoints = admin
            .build()
            .list_checkpoints(Some(source_checkpoint))
            .await
            .context("Failed to resolve physical source checkpoint")?;
        let checkpoint = match checkpoints.as_slice() {
            [checkpoint] if checkpoint.expire_time.is_none() => checkpoint,
            [checkpoint] => bail!(
                "physical source checkpoint {} is expiring and cannot bootstrap a branch",
                checkpoint.id
            ),
            [] => bail!("physical source checkpoint '{source_checkpoint}' was not found"),
            _ => bail!("multiple physical checkpoints use name '{source_checkpoint}'"),
        };
        runtime
            .create_initial_branch(InitialBranchCreateRequest {
                operation_id,
                destination_id: branch_id,
                destination_name: name.to_string(),
                source: ImmutableCheckpoint {
                    database_path: source_path,
                    checkpoint_id: checkpoint.id,
                    manifest_id: checkpoint.manifest_id,
                },
                created_at: zerofs::catalog::catalog_timestamp(checkpoint.create_time),
            })
            .await
    }
    .await;
    let close_result = runtime.close().await;
    let branch = match (result, close_result) {
        (Ok(branch), Ok(())) => branch,
        (Err(error), Ok(())) => return Err(error),
        (Ok(_), Err(error)) => return Err(error),
        (Err(error), Err(close_error)) => {
            return Err(error.context(format!("catalog close also failed: {close_error:#}")));
        }
    };
    println!(
        "Branch '{}' ({}) is ready (operation {})",
        branch.name, branch.id, operation_id
    );
    Ok(())
}

pub async fn create_branch(
    config_path: &Path,
    name: &str,
    source_branch_id: Uuid,
    source_checkpoint: &str,
    branch_id: Option<Uuid>,
    operation_id: Option<Uuid>,
) -> Result<()> {
    let branch_id = branch_id.unwrap_or_else(Uuid::new_v4);
    let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
    println!("Exact retry: --id {branch_id} --operation-id {operation_id}");
    let result = connect_rpc_client(config_path)
        .await?
        .create_branch(
            operation_id,
            branch_id,
            name,
            source_branch_id,
            source_checkpoint,
        )
        .await?;
    println!(
        "Branch '{}' ({}) is {} (operation {})",
        result.name, result.branch_id, result.state, result.operation_id
    );
    Ok(())
}

pub async fn delete_branch(
    config_path: &Path,
    branch_id: Uuid,
    name: &str,
    operation_id: Option<Uuid>,
) -> Result<()> {
    let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
    println!("Exact retry: --operation-id {operation_id}");
    let result = connect_rpc_client(config_path)
        .await?
        .delete_branch(operation_id, branch_id, name)
        .await?;
    println!(
        "Branch '{}' ({}) is {} (operation {})",
        result.name, result.branch_id, result.state, result.operation_id
    );
    Ok(())
}

pub async fn list_branches(config_path: &Path, after: Option<Uuid>, limit: usize) -> Result<()> {
    let page = connect_rpc_client(config_path)
        .await?
        .list_branches(after, limit)
        .await?;
    if page.records.is_empty() {
        println!("No branch records found.");
    } else {
        for branch in page.records {
            print_branch(&branch);
            println!();
        }
    }
    if let Some(next_after) = page.next_after {
        println!("Next cursor: {next_after}");
    }
    Ok(())
}

pub async fn get_branch_info(config_path: &Path, id: Uuid) -> Result<()> {
    let Some(branch) = connect_rpc_client(config_path)
        .await?
        .get_branch_info(id)
        .await?
    else {
        bail!("Branch {id} not found");
    };
    print_branch(&branch);
    Ok(())
}

fn print_branch(branch: &CustomerCatalogRecord) {
    println!("Name: {}", branch.name);
    println!("ID: {}", branch.resource_id);
    println!("Volume: {}", branch.volume_id);
    println!("State: {}", branch.state);
    println!(
        "Parent: {}",
        branch
            .parent_id
            .map_or_else(|| "-".to_string(), |id| id.to_string())
    );
    println!(
        "Origin checkpoint: {}",
        branch
            .origin_checkpoint_id
            .map_or_else(|| "-".to_string(), |id| id.to_string())
    );
    println!("Observed generation: {}", branch.observed_generation);
    println!("Created: {}", branch.created_at.to_rfc3339());
    println!("Updated: {}", branch.updated_at.to_rfc3339());
    if let Some(deleted_at) = branch.deleted_at {
        println!("Deleted: {}", deleted_at.to_rfc3339());
    }
    println!(
        "Metadata: {}",
        serde_json::Value::Object(branch.customer_metadata.clone())
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bootstrap_requires_offline_confirmation_before_reading_config() {
        let missing = Path::new("this-bootstrap-config-must-not-exist.toml");
        let error = bootstrap_branch(missing, "main", "bootstrap-source", None, None, false)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("--confirm-offline"), "{error}");
        assert!(!error.contains("load configuration"), "{error}");
    }
}
