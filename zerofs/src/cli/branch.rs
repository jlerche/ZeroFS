use crate::cli::connect_rpc_client;
use anyhow::{Result, bail};
use std::path::Path;
use uuid::Uuid;
use zerofs::catalog::CustomerCatalogRecord;

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
