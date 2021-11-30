use std::io;
use std::path::Path;

use pancake_db_idl::dml::PartitionField;
use pancake_db_idl::schema::PartitionMeta;
use tokio::fs;

use crate::errors::ServerResult;
use crate::utils::{common, dirs};
use std::str::FromStr;
use uuid::Uuid;
use crate::types::PartitionKey;

pub async fn list_segment_ids(
  dir: &Path,
  partition_key: &PartitionKey,
) -> ServerResult<Vec<Uuid>> {
  let partition_dir = dirs::partition_dir(dir, partition_key);
  let mut res = Vec::new();
  let mut read_dir = fs::read_dir(partition_dir).await?;
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

    res.push(Uuid::from_str(parts[1])?);
  }
  Ok(res)
}

pub async fn list_subpartitions(
  dir: &Path,
  meta: &PartitionMeta,
) -> ServerResult<Vec<PartitionField>> {
  let mut res = Vec::new();
  let mut read_dir = fs::read_dir(&dir).await?;
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
    if *parts[0] != meta.name {
      continue;
    }
    let parsed = common::partition_field_from_string(
      &meta.name,
      parts[1],
      meta.dtype.unwrap(),
    )?;
    res.push(parsed);
  }
  Ok(res)
}

pub async fn create_segment_dirs(segment_dir: &Path) -> io::Result<()> {
  fs::create_dir(segment_dir).await?;
  fs::create_dir(segment_dir.join("v0")).await?;
  Ok(())
}
