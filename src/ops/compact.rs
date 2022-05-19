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
use crate::metadata::compaction::Compaction;
use crate::metadata::deletion::DeletionMetadata;
use crate::metadata::PersistentMetadata;
use crate::metadata::segment::SegmentMetadata;
use crate::types::{CompactionKey, SegmentKey};
use crate::utils::{common, object_storage};
use crate::utils::dirs;

struct CompactionAssessment {
  pub do_compaction: bool,
  pub old_version: u64,
  pub new_version: u64,
  pub all_time_n_to_compact: u32,
  pub deletion_id: u64,
  pub warrants_object_storage: bool,
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
        .split('_')
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
      log::debug!(
        "checking for old versions for {} version {} (current as of {:?}",
        self.key,
        segment_meta.read_version,
        segment_meta.read_version_since,
      );
      self.delete_old_versions(&opts.dir, segment_meta.read_version).await?;
    }

    let all_time_n_to_compact = segment_meta.all_time_n - segment_meta.staged_n as u32;
    let new_version = segment_meta.read_version + 1;
    let mut res = CompactionAssessment {
      do_compaction: false,
      old_version: segment_meta.read_version,
      new_version,
      all_time_n_to_compact,
      deletion_id: segment_meta.deletion_id,
      warrants_object_storage: false,
    };
    if segment_meta.write_versions.len() > 1 || all_time_n_to_compact < opts.min_rows_for_compaction {
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

    let n_rows_has_increased = all_time_n_to_compact > existing_compaction.all_time_compacted_n;
    let n_rows_has_doubled = all_time_n_to_compact >= 2 * existing_compaction.all_time_compacted_n;
    let n_rows_has_been_constant = current_time - segment_meta.last_flush_at > Duration::seconds(opts.compact_as_constant_seconds);
    if n_rows_has_doubled || (n_rows_has_increased && n_rows_has_been_constant) {
      log::debug!(
        "will compact {}",
        self.key,
      );
      res.do_compaction = true;
      res.warrants_object_storage = segment_meta.is_cold;
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
    all_time_omitted_n: u32,
  ) -> Compaction {
    let mut col_codecs = HashMap::new();

    for (col_name, col_meta) in augmented_cols {
      col_codecs.insert(
        col_name.to_string(),
        compression::choose_codec(col_meta.dtype())
      );
    }

    Compaction {
      all_time_compacted_n: assessment.all_time_n_to_compact,
      all_time_omitted_n,
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
      assessment.all_time_n_to_compact as usize,
    ).await?;
    let bytes = compressor.compress(&values, col_meta.nested_list_depth as u8)?;
    let compaction_key = self.key.compaction_key(assessment.new_version);
    let version_coords = object_storage::version_coords(
      &server.opts,
      &compaction_key,
      assessment.warrants_object_storage,
    );
    object_storage::write_col(
      server,
      &bytes,
      &version_coords,
      col_name,
    ).await?;
    Ok(())
  }

  // returns all time omitted n
  async fn execute_deletion_compaction(
    &self,
    server: &Server,
    old_compaction_key: &CompactionKey,
    new_compaction_key: &CompactionKey,
    assessment: &CompactionAssessment,
  ) -> ServerResult<u32> {
    let old_pre_compaction_deletions = server.read_pre_compaction_deletions(
      old_compaction_key
    ).await?;
    let old_post_compaction_deletions = server.read_post_compaction_deletions(
      old_compaction_key,
      assessment.deletion_id,
    ).await?;

    let n_pre = old_pre_compaction_deletions.len();
    let n_post = old_post_compaction_deletions.len();
    if n_pre == 0 && n_post == 0 {
      return Ok(0);
    }

    let mut new_pre_compaction_deletions = Vec::new();
    let mut row_id = 0;
    let mut post_i = 0;
    let mut all_time_omitted_n = 0;
    while row_id < n_pre || post_i < n_post {
      let pre_deleted = row_id < n_pre && old_pre_compaction_deletions[row_id];
      let post_deleted = post_i < n_post && old_post_compaction_deletions[post_i];
      let is_deleted = pre_deleted || post_deleted;
      new_pre_compaction_deletions.push(is_deleted);
      if is_deleted && row_id < assessment.all_time_n_to_compact as usize {
        all_time_omitted_n += 1;
      }
      if !pre_deleted {
        post_i += 1
      }
      row_id += 1
    }

    let deletion_bytes = deletion::compress_deletions(&new_pre_compaction_deletions)?;

    common::overwrite_file(
      dirs::pre_compaction_deletions_path(
        &server.opts.dir,
        new_compaction_key,
      ),
      &deletion_bytes,
    ).await?;
    Ok(all_time_omitted_n)
  }

  async fn compact(
    &self,
    server: &Server,
    schema: &Schema,
    assessment: &CompactionAssessment,
    deletion_meta_guard: OwnedRwLockWriteGuard<Option<DeletionMetadata>>,
  ) -> ServerResult<()> {
    let dir = &server.opts.dir;
    let augmented_cols = common::augmented_columns(schema);
    let old_compaction_key = self.key.compaction_key(assessment.old_version);
    let old_compaction = {
      let old_compaction_lock = server.compaction_cache
        .get_lock(&old_compaction_key)
        .await?;
      let new_compaction_guard = old_compaction_lock.read().await;
      new_compaction_guard.clone().unwrap_or_default()
    };
    let new_compaction_key = self.key.compaction_key(assessment.new_version);

    log::info!(
      "starting compaction for {} ({} rows)",
      new_compaction_key,
      assessment.all_time_n_to_compact,
    );

    // First compact deletion information, since deletions are locked.
    // Along the way we'll be able to count the number of rows omitted
    // We'll compact each column later, since flushes aren't locked.
    let all_time_omitted_n = self.execute_deletion_compaction(
      server,
      &old_compaction_key,
      &new_compaction_key,
      assessment,
    ).await?;
    drop(deletion_meta_guard);
    log::debug!(
      "wrote deletion information for {} deletion id {} all_time_omitted_n {}",
      new_compaction_key,
      assessment.deletion_id,
      all_time_omitted_n,
    );

    // Write the compaction metadata
    let compaction = self.plan_compaction(&augmented_cols, assessment, all_time_omitted_n);
    {
      let new_compaction_lock = server.compaction_cache
        .get_lock(&new_compaction_key)
        .await?;
      let mut new_compaction_guard = new_compaction_lock.write().await;
      *new_compaction_guard = Some(compaction.clone());
      compaction.overwrite(dir, &new_compaction_key).await?;
    }
    log::debug!(
      "wrote compaction metadata for {}",
      new_compaction_key,
    );

    // Now we compact each column.
    for (col_name, col_meta) in &augmented_cols {
      let old_compression_params = old_compaction.col_codecs
        .get(col_name);
      let compressor = compression::new_codec(
        common::unwrap_dtype(col_meta.dtype)?,
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
      log::debug!(
        "wrote compacted column {} for {}",
        col_name,
        new_compaction_key,
      );
    }
    log::info!("finished compaction for {}", new_compaction_key);

    Ok(())
  }
}

#[async_trait]
impl ServerOp for CompactionOp {
  type Locks = TableReadLocks;
  type Response = ();

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

    let deletion_lock = server.deletion_metadata_cache.get_lock(
      &self.key
    ).await?;
    let segment_lock = server.segment_metadata_cache.get_lock(
      &self.key
    ).await?;

    // we'll manually drop the deletion lock when we don't need it
    let deletion_meta_guard = deletion_lock.write_owned().await;

    // we put this code in a block to scope the first write lock on segment meta
    let assessment = {
      let mut segment_guard = segment_lock.write().await;
      let maybe_segment_meta = &mut *segment_guard;
      if maybe_segment_meta.is_none() {
        return Err(ServerError::does_not_exist("segment", &self.key));
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
      self.compact(server, &table_meta.schema(), &assessment, deletion_meta_guard).await?;

      let mut segment_guard = segment_lock.write().await;
      let maybe_segment_meta = &mut *segment_guard;
      if maybe_segment_meta.is_none() {
        return Err(ServerError::does_not_exist("segment", &self.key));
      }
      let segment_meta = maybe_segment_meta.as_mut().unwrap();
      segment_meta.read_version = assessment.new_version;
      segment_meta.write_versions = vec![assessment.new_version];
      segment_meta.read_version_since = Utc::now();
      segment_meta.read_coords = object_storage::version_coords(
        opts,
        &self.key.compaction_key(assessment.new_version),
        assessment.warrants_object_storage,
      );
      segment_meta.overwrite(&opts.dir, &self.key).await?;
    }

    Ok(())
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
