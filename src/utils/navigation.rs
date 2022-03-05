use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use async_stream::try_stream;
use futures::Stream;
use pancake_db_idl::dml::{PartitionFieldValue, PartitionFilter};
use pancake_db_idl::schema::PartitionMeta;
use tokio::fs;
use tokio::fs::DirEntry;
use uuid::Uuid;

use crate::errors::{Contextable, ServerError, ServerResult};
use crate::types::{NormalizedPartition, PartitionKey};
use crate::utils::{common, dirs};

pub async fn partitions_for_table(
  dir: &Path,
  table_name: &str,
  partitioning: &HashMap<String, PartitionMeta>,
  filters: &[PartitionFilter],
) -> ServerResult<Vec<HashMap<String, PartitionFieldValue>>> {
  let mut partitions: Vec<HashMap<String, PartitionFieldValue>> = vec![HashMap::new()];
  let mut partition_names: Vec<_> = partitioning.keys().cloned().collect();
  partition_names.sort();
  for partition_name in &partition_names {
    let meta = partitioning[partition_name].clone();
    let mut new_partitions: Vec<HashMap<String, PartitionFieldValue>> = Vec::new();
    for partition in &partitions {
      let subdir = dirs::partition_dir(
        dir,
        &PartitionKey {
          table_name: table_name.to_string(),
          partition: NormalizedPartition::from_raw_fields(partition)?
        }
      );
      let subpartitions = subpartitions(
        &subdir,
        partition_name,
        &meta
      )
        .await?;

      for leaf in subpartitions {
        let mut new_partition = partition.clone();
        new_partition.insert(partition_name.to_string(), leaf);
        if common::satisfies_filters(&new_partition, filters)? {
          new_partitions.push(new_partition);
        }
      }
    }
    partitions = new_partitions;
  }
  Ok(partitions)
}

async fn subpartitions(
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

fn split_segment_id(entry: &DirEntry) -> Option<String> {
  let fname = entry.file_name();
  let parts = fname
    .to_str()
    .unwrap()
    .split('_')
    .collect::<Vec<&str>>();

  if parts.len() != 2 || parts[0] != "s" {
    return None;
  }

  Some(parts[1].to_string())
}

pub fn stream_segment_ids_for_partition(
  dir: &Path,
  partition_key: PartitionKey,
) -> impl Stream<Item=ServerResult<Uuid>> {
  let partition_dir = dirs::partition_dir(dir, &partition_key);
  try_stream! {
    let mut read_dir = fs::read_dir(&partition_dir).await
      .map_err(|e| ServerError::from(e).with_context(format!(
        "while reading directory {:?} for listing segments",
        partition_dir
      )))?;

    while let Some(entry) = read_dir.next_entry().await? {
      if !entry.file_type().await.unwrap().is_dir() {
        continue;
      }

      if let Some(segment_id_str) = split_segment_id(&entry) {
        let uuid = Uuid::from_str(&segment_id_str).map_err(
          |e| ServerError::from(e).with_context(format!(
            "while parsing segment id {} found in directory for {}",
            segment_id_str,
            partition_key,
          ))
        )?;
        yield uuid;
      }
    }
  }
}

pub async fn create_segment_dirs(segment_dir: &Path) -> ServerResult<()> {
  common::create_if_new(segment_dir).await?;
  common::create_if_new(segment_dir.join("v0")).await?;
  Ok(())
}
