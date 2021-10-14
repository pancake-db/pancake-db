use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use pancake_db_idl::schema::{ColumnMeta, Schema};
use tokio::fs;

use pancake_db_core::compression;
use pancake_db_core::compression::ValueCodec;

use crate::dirs;
use crate::errors::{ServerError, ServerResult};
use crate::locks::table::TableReadLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::compaction::Compaction;
use crate::storage::Metadata;
use crate::storage::segment::SegmentMetadata;
use crate::types::SegmentKey;
use crate::utils;

struct CompactionAssessment {
  pub remove_from_candidates: bool,
  pub do_compaction: bool,
  pub old_version: u64,
  pub new_version: u64,
  pub n_to_compact: usize,
}

pub struct CompactionOp {
  pub key: SegmentKey,
}

impl CompactionOp {
  async fn delete_old_versions(
    &self,
    dir: &Path,
    current_read_version: u64,
  ) -> ServerResult<()> {
    let dir = dirs::segment_dir(dir, &self.key);
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

  async fn assess_compaction<'a>(
    &self,
    server: &Server,
    segment_meta: &'a mut SegmentMetadata,
  ) -> ServerResult<CompactionAssessment> {
    let opts = &server.opts;
    let current_time = Utc::now();
    if current_time - segment_meta.read_version_since > Duration::seconds(opts.delete_stale_compaction_seconds) {
      self.delete_old_versions(&opts.dir, segment_meta.read_version).await?;
    }

    let n_flushed_rows = segment_meta.all_time_n as usize - segment_meta.staged_n -
      (segment_meta.all_time_n_deleted as usize - segment_meta.staged_n_deleted);
    let new_version = segment_meta.read_version + 1;
    let mut res = CompactionAssessment {
      remove_from_candidates: false,
      do_compaction: false,
      old_version: segment_meta.read_version,
      new_version,
      n_to_compact: n_flushed_rows
    };
    if segment_meta.write_versions.len() > 1 || n_flushed_rows < opts.min_rows_for_compaction {
      //already compacting or too few rows to warrant compaction
      res.remove_from_candidates = true;
      return Ok(res);
    }

    if current_time - segment_meta.read_version_since < Duration::seconds(opts.min_compaction_intermission_seconds) {
      return Ok(res);
    }

    // compaction meta never changes, so it's fine to drop the lock immediately
    let existing_compaction = server.compaction_cache
      .get_lock(&self.key.compaction_key(segment_meta.read_version))
      .await?
      .read()
      .await
      .clone()
      .unwrap_or_default();

    let n_rows_has_increased = n_flushed_rows > existing_compaction.compacted_n;
    let n_rows_has_doubled = n_flushed_rows >= 2 * existing_compaction.compacted_n;
    let n_rows_has_been_constant = current_time - segment_meta.last_flush_at > Duration::seconds(opts.compact_as_constant_seconds);
    if n_rows_has_doubled || (n_rows_has_increased && n_rows_has_been_constant) {
      // create a new directory and start flushing to the new version as well
      let compaction_key = self.key.compaction_key(new_version);
      utils::create_if_new(dirs::version_dir(&opts.dir, &compaction_key)).await?;
      segment_meta.write_versions = vec![segment_meta.read_version, new_version];
      segment_meta.overwrite(&opts.dir, &self.key).await?;

      res.remove_from_candidates = true;
      res.do_compaction = true;
      Ok(res)
    } else {
      // we won't do a compaction now, but we will later
      Ok(res)
    }
  }

  fn plan_compaction(&self, schema: &Schema, assessment: &CompactionAssessment) -> Compaction {
    let mut col_codecs = HashMap::new();

    for col in &schema.columns {
      col_codecs.insert(
        col.name.clone(),
        compression::choose_codec(col.dtype.unwrap())
      );
    }

    Compaction {
      compacted_n: assessment.n_to_compact as usize,
      col_codecs,
    }
  }

  async fn execute_col_compaction(
    &self,
    server: &Server,
    col: &ColumnMeta,
    assessment: &CompactionAssessment,
    old_compression_params: Option<&String>,
    compressor: &dyn ValueCodec,
  ) -> ServerResult<()> {
    let values = server.read_col(
      &self.key,
      col,
      assessment.old_version,
      old_compression_params,
      assessment.n_to_compact,
    ).await?;
    let bytes = compressor.compress(&values, col.nested_list_depth as u8)?;
    let compaction_key = self.key.compaction_key(assessment.new_version);
    utils::append_to_file(
      &dirs::compact_col_file(&server.opts.dir, &compaction_key, &col.name),
      bytes.as_slice(),
    ).await?;
    Ok(())
  }

  async fn compact(
    &self,
    server: &Server,
    schema: &Schema,
    assessment: &CompactionAssessment
  ) -> ServerResult<()> {
    let opts = &server.opts;
    let compaction = self.plan_compaction(schema, assessment);
    {
      let new_compaction_key = self.key.compaction_key(assessment.new_version);
      let new_compaction_lock = server.compaction_cache
        .get_lock(&new_compaction_key)
        .await?;
      let mut new_compaction_guard = new_compaction_lock.write().await;
      *new_compaction_guard = Some(compaction.clone());
      compaction.overwrite(&opts.dir, &new_compaction_key).await?;
    }
    let old_compaction = {
      let old_compaction_key = self.key.compaction_key(assessment.old_version);
      let old_compaction_lock = server.compaction_cache
        .get_lock(&old_compaction_key)
        .await?;
      let new_compaction_guard = old_compaction_lock.read().await;
      new_compaction_guard.clone().unwrap_or_default()
    };

    log::info!(
      "starting compaction for {} version {} ({} rows)",
      self.key,
      assessment.new_version,
      assessment.n_to_compact,
    );
    for col in &schema.columns {
      let old_compression_params = old_compaction.col_codecs
        .get(&col.name);
      let compressor = compression::new_codec(
        col.dtype.unwrap(),
        compaction.col_codecs.get(&col.name).unwrap()
      )?;
      self.execute_col_compaction(
        server,
        col,
        assessment,
        old_compression_params,
        &*compressor,
      ).await?;
    }
    log::info!("finished compaction for {} version {}", self.key, assessment.new_version);

    Ok(())
  }
}

#[async_trait]
impl ServerOp<TableReadLocks> for CompactionOp {
  type Response = bool;

  fn get_key(&self) -> ServerResult<String> {
    Ok(self.key.table_name.clone())
  }

  // 1. obtain write lock on segment meta, check if we need compaction
  // 2. create version directory and compaction meta, update segment meta, release segment meta lock
  // 3. do compaction
  // 4. obtain write lock on segment meta, update it, release it
  // Obtaining and releasing segment meta is safe because only 1 compaction can happen at a time.
  // Return whether to remove this segment key from the set of compaction candidates
  async fn execute_with_locks(&self, server: &Server, locks: TableReadLocks) -> ServerResult<Self::Response> {
    let TableReadLocks {
      table_meta,
    } = locks;
    let opts = &server.opts;

    let segment_lock = server.segment_metadata_cache.get_lock(&self.key)
      .await?;
    let assessment = {
      let mut segment_guard = segment_lock.write().await;
      let maybe_segment_meta = &mut *segment_guard;
      if maybe_segment_meta.is_none() {
        return Err(ServerError::does_not_exist("segment", &self.key.to_string()));
      }
      let segment_meta = maybe_segment_meta.as_mut().unwrap();

      self.assess_compaction(server, segment_meta).await?
    };

    if assessment.do_compaction {
      // important that segment meta is not locked during compaction
      // otherwise writes would be blocked
      self.compact(server, &table_meta.schema, &assessment).await?;

      let mut segment_guard = segment_lock.write().await;
      let maybe_segment_meta = &mut *segment_guard;
      if maybe_segment_meta.is_none() {
        return Err(ServerError::does_not_exist("segment", &self.key.to_string()));
      }
      let segment_meta = maybe_segment_meta.as_mut().unwrap();
      segment_meta.read_version = assessment.new_version;
      segment_meta.write_versions = vec![assessment.new_version];
      segment_meta.read_version_since = Utc::now();
      segment_meta.overwrite(&opts.dir, &self.key).await?;
    }

    Ok(assessment.remove_from_candidates)
  }
}
