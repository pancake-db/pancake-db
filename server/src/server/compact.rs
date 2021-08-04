use std::collections::HashMap;

use pancake_db_idl::schema::{ColumnMeta, Schema};
use tokio::fs;

use pancake_db_core::compression;
use pancake_db_core::compression::Compressor;
use pancake_db_core::errors::PancakeResult;

use crate::dirs;
use crate::storage::compaction::{Compaction, CompressionParams};
use crate::storage::flush::FlushMetadata;
use crate::types::SegmentKey;
use crate::utils;

use super::Server;
use chrono::{Utc, Duration};

impl Server {
  pub async fn compact_if_needed(&self, segment_key: &SegmentKey) -> PancakeResult<()> {
    let table_name_string = segment_key.table_name.to_string();
    let metadata = self.flush_metadata_cache.get(&segment_key).await;

    if Utc::now() - metadata.read_version_since > Duration::seconds(self.opts.delete_stale_compaction_seconds) {
      self.delete_old_versions(segment_key, metadata.read_version).await?;
    }

    let schema = self.schema_cache.get_result(&table_name_string).await?;

    if metadata.write_versions.len() > 1  || metadata.n < self.opts.min_rows_for_compaction {
      //already compacting
      return Ok(());
    }

    let existing_compaction = self.compaction_cache
      .get(segment_key.compaction_key(metadata.read_version))
      .await;
    if existing_compaction.compacted_n * 2 > metadata.n {
      return Ok(());
    }

    return self.compact(segment_key, schema, metadata).await;
  }

  async fn delete_old_versions(
    &self,
    segment_key: &SegmentKey,
    current_read_version: u64,
  ) -> PancakeResult<()> {
    let dir = dirs::segment_dir(&self.opts.dir, segment_key);
    let mut read_dir = fs::read_dir(&dir).await?;
    while let Ok(Some(entry)) = read_dir.next_entry().await {
      if !entry.file_type().await.unwrap().is_dir() {
        continue;
      }

      let fname = entry.file_name();
      let parts: Vec<&str> = fname
        .to_str()
        .unwrap()
        .split('v')
        .collect::<Vec<&str>>();

      if parts.len() != 2 {
        continue;
      }
      if *parts[0] != *"" {
        continue;
      }
      let parsed: u64 = parts[1].parse::<u64>()?;
      if parsed < current_read_version {
        let full_path = dir.join(fname);
        fs::remove_dir_all(full_path).await?;
      }
    }
    Ok(())
  }

  fn plan_compaction(&self, schema: &Schema, metadata: &FlushMetadata) -> Compaction {
    let mut col_compression_params = HashMap::new();

    for col in &schema.columns {
      col_compression_params.insert(
        col.name.clone(),
        compression::choose_compression_params(col.dtype.unwrap())
      );
    }

    Compaction {
      compacted_n: metadata.n,
      col_compression_params,
    }
  }

  async fn execute_col_compaction(
    &self,
    segment_key: &SegmentKey,
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    old_compression_params: Option<&CompressionParams>,
    compressor: &dyn Compressor,
    new_version: u64,
  ) -> PancakeResult<()> {
    let values = self.read_col(
      segment_key,
      col,
      metadata,
      old_compression_params,
      metadata.n,
    ).await?;
    let bytes = compressor.compress(&values, col)?;
    utils::append_to_file(
      &dirs::compact_col_file(&self.opts.dir, &segment_key.compaction_key(new_version), &col.name),
      bytes.as_slice(),
    ).await?;
    Ok(())
  }

  async fn execute_compaction(
    &self,
    segment_key: &SegmentKey,
    schema: &Schema,
    metadata: &FlushMetadata,
    old_compaction: &Compaction,
    compaction: &Compaction,
    new_version: u64,
  ) -> PancakeResult<()> {
    for col in &schema.columns {
      let old_compression_params = old_compaction.col_compression_params
        .get(&col.name);
      let compressor = compression::get_compressor(
        col.dtype.unwrap(),
        compaction.col_compression_params.get(&col.name).unwrap()
      )?;
      self.execute_col_compaction(
        segment_key,
        col,
        metadata,
        old_compression_params,
        &*compressor,
        new_version,
      ).await?;
    }
    Ok(())
  }

  async fn compact(
    &self,
    segment_key: &SegmentKey,
    schema: Schema,
    metadata: FlushMetadata
  ) -> PancakeResult<()> {
    let new_version = metadata.read_version + 1;

    let compaction: Compaction = self.plan_compaction(&schema, &metadata);

    let old_compaction_key = segment_key.compaction_key(metadata.read_version);
    let old_compaction = self.compaction_cache.get(old_compaction_key).await;

    let compaction_key = segment_key.compaction_key(new_version);
    utils::create_if_new(dirs::version_dir(&self.opts.dir, &compaction_key)).await?;

    self.compaction_cache.save(compaction_key, compaction.clone()).await?;
    self.flush_metadata_cache.add_write_version(&segment_key, new_version).await?;

    self.execute_compaction(
      segment_key,
      &schema,
      &metadata,
      &old_compaction,
      &compaction,
      new_version
    ).await?;

    self.flush_metadata_cache.update_read_version(&segment_key, new_version).await?;
    Ok(())
  }
}
