use std::path::Path;
use std::str::FromStr;

use pancake_db_idl::dml::PartitionFieldValue;
use pancake_db_idl::schema::PartitionMeta;
use tokio::fs;
use uuid::Uuid;

use crate::errors::{ServerResult, ServerError, Contextable};
use crate::types::PartitionKey;
use crate::utils::{common, dirs};

pub async fn list_segment_ids(
  dir: &Path,
  partition_key: &PartitionKey,
) -> ServerResult<Vec<Uuid>> {
  let partition_dir = dirs::partition_dir(dir, partition_key);
  let mut res = Vec::new();
  let mut read_dir = fs::read_dir(&partition_dir).await
    .map_err(|e| ServerError::from(e).with_context(format!(
      "while reading directory {:?} for listing segments",
      partition_dir
    )))?;
  while let Ok(Some(entry)) = read_dir.next_entry().await {
    if !entry.file_type().await.unwrap().is_dir() {
      continue;
    }

    let fname = entry.file_name();
    let parts = fname
      .to_str()
      .unwrap()
      .split('_')
      .collect::<Vec<&str>>();

    if parts.len() != 2 {
      continue;
    }
    if parts[0] != "s" {
      continue;
    }

    res.push(Uuid::from_str(parts[1])
      .map_err(
        |e| ServerError::from(e).with_context(format!(
          "while parsing segment id {} found in directory for {}",
          parts[1],
          partition_key,
        ))
      )?);
  }
  Ok(res)
}

pub async fn list_subpartitions(
  dir: &Path,
  name: &str,
  meta: &PartitionMeta,
) -> ServerResult<Vec<PartitionFieldValue>> {
  let mut res = Vec::new();
  let mut read_dir = fs::read_dir(&dir).await
    .map_err(|e| ServerError::from(e).with_context(format!(
      "while reading directory {:?} for listing subpartitions",
      dir
    )))?;
  while let Ok(Some(entry)) = read_dir.next_entry().await {
    if !entry.file_type().await.unwrap().is_dir() {
      continue;
    }

    let fname = entry.file_name();
    let parts = fname
      .to_str()
      .unwrap()
      .split('=')
      .collect::<Vec<&str>>();

    if parts.len() != 2 {
      continue;
    }
    if parts[0] != name {
      continue;
    }
    let parsed = common::partition_field_value_from_string(
      parts[1],
      meta.dtype.unwrap(),
    ).with_context(|| format!(
      "while parsing partition value {} in {:?}",
      parts[1],
      dir,
    ))?;
    res.push(parsed);
  }
  Ok(res)
}

pub async fn create_segment_dirs(segment_dir: &Path) -> ServerResult<()> {
  common::create_if_new(segment_dir).await?;
  common::create_if_new(segment_dir.join("v0")).await?;
  Ok(())
}
