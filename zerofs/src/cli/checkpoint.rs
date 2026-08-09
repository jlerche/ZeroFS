use super::connect_rpc_client;
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

pub async fn list_checkpoints(config_path: &Path) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    let checkpoints = client.list_checkpoints().await?;

    if checkpoints.is_empty() {
        println!("No checkpoints found.");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(vec!["Name", "ID", "Created At"]);

    for checkpoint in checkpoints {
        table.add_row(vec![
            checkpoint.name,
            checkpoint.id.to_string(),
            format_timestamp(checkpoint.created_at),
        ]);
    }

    println!("{table}");
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
        None => {
            client
                .get_checkpoint_info(name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Checkpoint '{name}' not found"))?
                .id
        }
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

pub async fn get_checkpoint_info(config_path: &Path, name: &str) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    let checkpoint = client.get_checkpoint_info(name).await?;

    match checkpoint {
        Some(info) => {
            println!("Checkpoint Information:");
            println!("  Name: {}", info.name);
            println!("  ID: {}", info.id);
            println!("  Created at: {}", format_timestamp(info.created_at));
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
