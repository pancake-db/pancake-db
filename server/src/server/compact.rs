use std::collections::HashMap;

use pancake_db_idl::schema::{ColumnMeta, Schema};
use tokio::fs;

use pancake_db_core::compression;
use pancake_db_core::compression::ValueCodec;
use crate::errors::ServerResult;

use crate::dirs;
use crate::storage::compaction::Compaction;
use crate::storage::flush::FlushMetadata;
use crate::types::SegmentKey;
use crate::utils;

use super::Server;
use chrono::{Utc, Duration};

impl Server {
  // returns true if the segment will no longer need compaction (until more flushes happen),
  // false if it might still need compaction in the future
  pub async fn compact_if_needed(&self, segment_key: &SegmentKey) -> ServerResult<bool> {
    let table_name_string = segment_key.table_name.to_string();
    let metadata = self.flush_metadata_cache.get(&segment_key).await;

    if Utc::now() - metadata.read_version_since > Duration::seconds(self.opts.delete_stale_compaction_seconds) {
      self.delete_old_versions(segment_key, metadata.read_version).await?;
    }

    let schema = self.schema_cache.get_or_err(&table_name_string).await?;

    if metadata.write_versions.len() > 1 || metadata.n < self.opts.min_rows_for_compaction {
      //already compacting or too few rows to warrant compaction
      return Ok(true);
    }
    let current_time = Utc::now();
    if current_time - metadata.read_version_since < Duration::seconds(self.opts.min_compaction_intermission_seconds) {
      return Ok(false);
    }

    let existing_compaction = self.compaction_cache
      .get(segment_key.compaction_key(metadata.read_version))
      .await;

    let n_rows_has_increased = metadata.n > existing_compaction.compacted_n;
    let n_rows_has_doubled = metadata.n >= 2 * existing_compaction.compacted_n;
    let n_rows_has_been_constant = current_time - metadata.last_flush_at > Duration::seconds(self.opts.compact_as_constant_seconds);
    if n_rows_has_doubled || (n_rows_has_increased && n_rows_has_been_constant) {
      self.compact(segment_key, schema, metadata).await?;
      Ok(true)
    } else {
      Ok(false)
    }
  }

  async fn delete_old_versions(
    &self,
    segment_key: &SegmentKey,
    current_read_version: u64,
  ) -> ServerResult<()> {
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
    let mut col_codecs = HashMap::new();

    for col in &schema.columns {
      col_codecs.insert(
        col.name.clone(),
        compression::choose_codec(col.dtype.unwrap())
      );
    }

    Compaction {
      compacted_n: metadata.n,
      col_codecs,
    }
  }

  async fn execute_col_compaction(
    &self,
    segment_key: &SegmentKey,
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    old_compression_params: Option<&String>,
    compressor: &dyn ValueCodec,
    new_version: u64,
  ) -> ServerResult<()> {
    let values = self.read_col(
      segment_key,
      col,
      metadata,
      old_compression_params,
      metadata.n,
    ).await?;
    let bytes = compressor.compress(&values, col.nested_list_depth as u8)?;
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
  ) -> ServerResult<()> {
    log::info!(
      "starting compaction for {} version {} ({} rows)",
      segment_key,
      new_version,
      metadata.n
    );
    for col in &schema.columns {
      let old_compression_params = old_compaction.col_codecs
        .get(&col.name);
      let compressor = compression::new_codec(
        col.dtype.unwrap(),
        compaction.col_codecs.get(&col.name).unwrap()
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
    log::info!("finished compaction for {} version {}", segment_key, new_version);
    Ok(())
  }

  async fn compact(
    &self,
    segment_key: &SegmentKey,
    schema: Schema,
    metadata: FlushMetadata
  ) -> ServerResult<()> {
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
