use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use pancake_db_core::{compression, deletion};
use pancake_db_core::compression::ValueCodec;
use pancake_db_idl::schema::{ColumnMeta, Schema};
use tokio::fs;
use tokio::sync::OwnedRwLockWriteGuard;

use crate::errors::{ServerError, ServerResult};
use crate::locks::table::TableReadLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::compaction::Compaction;
use crate::storage::deletion::DeletionMetadata;
use crate::storage::Metadata;
use crate::storage::segment::SegmentMetadata;
use crate::types::{CompactionKey, SegmentKey};
use crate::utils::common;
use crate::utils::dirs;

struct CompactionAssessment {
  pub remove_from_candidates: bool,
  pub do_compaction: bool,
  pub old_version: u64,
  pub new_version: u64,
  pub compacted_n: usize,
  pub omitted_n: u32,
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
        log::info!(
          "deleting {} version {}",
          self.key,
          parsed,
        );
        let full_path = dir.join(fname);
        fs::remove_dir_all(full_path).await?;
      }
    }
    Ok(())
  }

  async fn assess_compaction(
    &self,
    server: &Server,
    segment_meta: &SegmentMetadata,
  ) -> ServerResult<CompactionAssessment> {
    let opts = &server.opts;
    let current_time = Utc::now();
    if current_time - segment_meta.read_version_since > Duration::seconds(opts.delete_stale_compaction_seconds) {
      log::info!(
        "checking for old versions for {} version {} (current as of {:?}",
        self.key,
        segment_meta.read_version,
        segment_meta.read_version_since,
      );
      self.delete_old_versions(&opts.dir, segment_meta.read_version).await?;
    }

    let omitted_n = segment_meta.all_time_deleted_n - segment_meta.staged_deleted_n as u32;
    let compacted_n = (segment_meta.all_time_n - segment_meta.staged_n as u32 -
      omitted_n) as usize;
    let new_version = segment_meta.read_version + 1;
    let mut res = CompactionAssessment {
      remove_from_candidates: false,
      do_compaction: false,
      old_version: segment_meta.read_version,
      new_version,
      compacted_n,
      omitted_n,
    };
    if segment_meta.write_versions.len() > 1 || compacted_n < opts.min_rows_for_compaction {
      res.remove_from_candidates = true;
      log::debug!(
        "will not compact {}; already compacting or too few rows",
        self.key,
      );
      return Ok(res);
    }

    if current_time - segment_meta.read_version_since < Duration::seconds(opts.min_compaction_intermission_seconds) {
      log::debug!(
        "will not compact {}; was compacted too recently",
        self.key,
      );
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

    let n_rows_has_increased = compacted_n > existing_compaction.compacted_n;
    let n_rows_has_doubled = compacted_n >= 2 * existing_compaction.compacted_n;
    let n_rows_has_been_constant = current_time - segment_meta.last_flush_at > Duration::seconds(opts.compact_as_constant_seconds);
    if n_rows_has_doubled || (n_rows_has_increased && n_rows_has_been_constant) {
      log::debug!(
        "will compact {}",
        self.key,
      );
      res.remove_from_candidates = true;
      res.do_compaction = true;
    } else {
      log::debug!(
        "will not compact {}; recently active without enough new rows",
        self.key,
      );
    }
    Ok(res)
  }

  fn plan_compaction(
    &self,
    augmented_cols: &HashMap<String, ColumnMeta>,
    assessment: &CompactionAssessment,
  ) -> Compaction {
    let mut col_codecs = HashMap::new();

    for (col_name, col_meta) in augmented_cols {
      col_codecs.insert(
        col_name.to_string(),
        compression::choose_codec(col_meta.dtype.unwrap())
      );
    }

    Compaction {
      compacted_n: assessment.compacted_n,
      omitted_n: assessment.omitted_n,
      col_codecs,
    }
  }

  async fn execute_col_compaction(
    &self,
    server: &Server,
    col_name: &str,
    col_meta: &ColumnMeta,
    assessment: &CompactionAssessment,
    old_compression_params: Option<&String>,
    compressor: &dyn ValueCodec,
  ) -> ServerResult<()> {
    let values = server.read_col(
      &self.key,
      col_name,
      col_meta,
      assessment.old_version,
      old_compression_params,
      assessment.compacted_n,
    ).await?;
    let bytes = compressor.compress(&values, col_meta.nested_list_depth as u8)?;
    let compaction_key = self.key.compaction_key(assessment.new_version);
    common::append_to_file(
      &dirs::compact_col_file(&server.opts.dir, &compaction_key, col_name),
      bytes.as_slice(),
    ).await?;
    Ok(())
  }

  async fn execute_deletion_compaction(
    &self,
    server: &Server,
    old_compaction_key: &CompactionKey,
    new_compaction_key: &CompactionKey,
  ) -> ServerResult<()> {
    let old_pre_compaction_deletions = server.read_pre_compaction_deletions(
      &old_compaction_key
    ).await?;
    let old_post_compaction_deletions = server.read_post_compaction_deletions(
      &old_compaction_key
    ).await?;

    let n_pre = old_pre_compaction_deletions.len();
    let n_post = old_post_compaction_deletions.len();
    if n_pre == 0 && n_post == 0 {
      return Ok(());
    }

    let mut new_pre_compaction_deletions = Vec::new();
    let mut pre_i = 0;
    let mut post_i = 0;
    while pre_i < n_pre && post_i < n_post {
      let pre_deleted = pre_i < n_pre && old_pre_compaction_deletions[pre_i];
      let post_deleted = post_i < n_post && old_post_compaction_deletions[post_i];
      new_pre_compaction_deletions.push(pre_deleted || post_deleted);
      if !pre_deleted {
        post_i += 1
      }
      pre_i += 1
    }

    let deletion_bytes = deletion::compress_deletions(new_pre_compaction_deletions)?;

    fs::write(
      dirs::pre_compaction_deletions_path(&server.opts.dir, new_compaction_key),
      &deletion_bytes,
    ).await?;
    Ok(())
  }

  async fn compact(
    &self,
    server: &Server,
    schema: &Schema,
    assessment: &CompactionAssessment,
    deletion_meta_guard: OwnedRwLockWriteGuard<Option<DeletionMetadata>>,
  ) -> ServerResult<()> {
    let dir = &server.opts.dir;
    let augmented_cols = common::augmented_columns(&schema);
    let compaction = self.plan_compaction(&augmented_cols, assessment);
    let new_compaction_key = self.key.compaction_key(assessment.new_version);
    {
      let new_compaction_lock = server.compaction_cache
        .get_lock(&new_compaction_key)
        .await?;
      let mut new_compaction_guard = new_compaction_lock.write().await;
      *new_compaction_guard = Some(compaction.clone());
      compaction.overwrite(dir, &new_compaction_key).await?;
    }
    let old_compaction_key = self.key.compaction_key(assessment.old_version);
    let old_compaction = {
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
      assessment.compacted_n,
    );

    // First compact deletion information, since deletions are locked.
    // We'll compact each column later, since flushes aren't locked.
    self.execute_deletion_compaction(server, &old_compaction_key, &new_compaction_key).await?;
    drop(deletion_meta_guard);

    // Now we compact each column.
    for (col_name, col_meta) in &augmented_cols {
      let old_compression_params = old_compaction.col_codecs
        .get(col_name);
      let compressor = compression::new_codec(
        col_meta.dtype.unwrap(),
        compaction.col_codecs.get(col_name).unwrap()
      )?;
      self.execute_col_compaction(
        server,
        col_name,
        col_meta,
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

    let deletion_lock = server.deletion_metadata_cache.get_lock(&self.key)
      .await?;
    let segment_lock = server.segment_metadata_cache.get_lock(&self.key)
      .await?;

    // we'll manually drop the deletion lock when we don't need it
    let deletion_meta_guard = deletion_lock.write_owned().await;

    // we put this code in a block to scope the first write lock on segment meta
    let assessment = {
      let mut segment_guard = segment_lock.write().await;
      let maybe_segment_meta = &mut *segment_guard;
      if maybe_segment_meta.is_none() {
        return Err(ServerError::does_not_exist("segment", &self.key.to_string()));
      }
      let segment_meta = maybe_segment_meta.as_mut().unwrap();

      let assessment = self.assess_compaction(server, segment_meta).await?;

      if assessment.do_compaction {
        // create a new directory and start flushing to the new version as well
        let compaction_key = self.key.compaction_key(assessment.new_version);
        common::create_if_new(dirs::version_dir(&opts.dir, &compaction_key)).await?;
        segment_meta.write_versions = vec![segment_meta.read_version, assessment.new_version];
        segment_meta.overwrite(&opts.dir, &self.key).await?;
      }
      assessment
    };

    if assessment.do_compaction {
      // important that segment meta is not locked during compaction
      // otherwise writes would be blocked
      self.compact(server, &table_meta.schema, &assessment, deletion_meta_guard).await?;

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

impl CompactionOp {
  pub async fn recover(
    server: &Server,
    segment_key: &SegmentKey,
    segment_meta: &mut SegmentMetadata,
  ) -> ServerResult<()> {
    let dir = &server.opts.dir;
    if segment_meta.write_versions.len() > 1 {
      log::debug!(
        "identified incomplete compaction for {}; removing files",
        segment_key,
      );
      for version in &segment_meta.write_versions {
        if *version != segment_meta.read_version {
          let compaction_key = segment_key.compaction_key(*version);
          fs::remove_dir_all(dirs::version_dir(dir, &compaction_key)).await?;
        }
      }

      segment_meta.write_versions = vec![segment_meta.read_version];
      segment_meta.overwrite(dir, segment_key).await?;
    }

    Ok(())
  }
}
