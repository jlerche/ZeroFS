use super::connect_rpc_client;
use crate::rpc::client::CheckpointView;
use anyhow::Result;
use comfy_table::{Table, presets::UTF8_FULL};
use std::path::Path;

pub async fn create_checkpoint(config_path: &Path, name: &str) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    let checkpoint = client.create_checkpoint(name).await?;

    println!("✓ Checkpoint created successfully!");
    println!("  Name: {}", checkpoint.name);
    println!("  ID: {}", checkpoint.id);
    println!("  Created at: {}", format_timestamp(checkpoint.created_at));

    Ok(())
}

pub async fn list_checkpoints(
    config_path: &Path,
    after: Option<uuid::Uuid>,
    limit: usize,
) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    let page = client.list_checkpoints(after, limit).await?;

    if page.records.is_empty() {
        println!("No checkpoints found.");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(vec!["Name", "ID", "State", "Created At", "Metadata"]);

    for checkpoint in page.records {
        match checkpoint {
            CheckpointView::Legacy(checkpoint) => {
                table.add_row(vec![
                    checkpoint.name,
                    checkpoint.id.to_string(),
                    "-".to_string(),
                    format_timestamp(checkpoint.created_at),
                    "-".to_string(),
                ]);
            }
            CheckpointView::Catalog(checkpoint) => {
                table.add_row(vec![
                    checkpoint.name,
                    checkpoint.resource_id.to_string(),
                    checkpoint.state,
                    checkpoint.created_at.to_rfc3339(),
                    serde_json::Value::Object(checkpoint.customer_metadata).to_string(),
                ]);
            }
        }
    }

    println!("{table}");
    if let Some(next_after) = page.next_after {
        println!("Next cursor: {next_after}");
    }
    Ok(())
}

pub async fn delete_checkpoint(
    config_path: &Path,
    name: &str,
    checkpoint_id: Option<uuid::Uuid>,
) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    let checkpoint_id = match checkpoint_id {
        Some(checkpoint_id) => checkpoint_id,
        None => client
            .get_checkpoint_info(name, None)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Checkpoint '{name}' not found"))?
            .id(),
    };
    client
        .delete_checkpoint(checkpoint_id, name)
        .await
        .map_err(|error| {
            error.context(format!(
                "checkpoint UUID {checkpoint_id}; retry with --id {checkpoint_id}"
            ))
        })?;

    println!(
        "✓ Checkpoint '{}' ({}) deleted successfully!",
        name, checkpoint_id
    );
    Ok(())
}

pub async fn get_checkpoint_info(
    config_path: &Path,
    name: &str,
    checkpoint_id: Option<uuid::Uuid>,
) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    let checkpoint = client.get_checkpoint_info(name, checkpoint_id).await?;

    match checkpoint {
        Some(CheckpointView::Legacy(info)) => {
            println!("Checkpoint Information:");
            println!("  Name: {}", info.name);
            println!("  ID: {}", info.id);
            println!("  Created at: {}", format_timestamp(info.created_at));
        }
        Some(CheckpointView::Catalog(info)) => {
            println!("Checkpoint Information:");
            println!("  Name: {}", info.name);
            println!("  ID: {}", info.resource_id);
            println!("  Volume: {}", info.volume_id);
            println!("  State: {}", info.state);
            println!(
                "  Branch: {}",
                info.parent_id
                    .map_or_else(|| "-".to_string(), |id| id.to_string())
            );
            println!("  Observed generation: {}", info.observed_generation);
            println!("  Created: {}", info.created_at.to_rfc3339());
            println!("  Updated: {}", info.updated_at.to_rfc3339());
            println!(
                "  Metadata: {}",
                serde_json::Value::Object(info.customer_metadata)
            );
        }
        None => {
            println!("Checkpoint '{}' not found.", name);
        }
    }

    Ok(())
}

fn format_timestamp(timestamp: u64) -> String {
    use std::time::{Duration, UNIX_EPOCH};

    let time = UNIX_EPOCH + Duration::from_secs(timestamp);
    let datetime: chrono::DateTime<chrono::Local> = time.into();
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}
