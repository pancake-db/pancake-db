use std::collections::HashSet;
use std::str::FromStr;

use async_trait::async_trait;
use pancake_db_core::{deletion};
use pancake_db_idl::dml::{DeleteFromSegmentRequest, DeleteFromSegmentResponse};

use crate::errors::{ServerError, ServerResult};
use crate::locks::deletion::DeletionWriteLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::metadata::PersistentMetadata;
use crate::types::{SegmentKey, NormalizedPartition};
use crate::utils::common;
use crate::utils::dirs;
use uuid::Uuid;

pub struct DeleteFromSegmentOp {
  pub req: DeleteFromSegmentRequest
}

#[async_trait]
impl ServerOp<DeletionWriteLocks> for DeleteFromSegmentOp {
  type Response = DeleteFromSegmentResponse;

  fn get_key(&self) -> ServerResult<SegmentKey> {
    let partition = NormalizedPartition::from_raw_fields(&self.req.partition)?;
    Ok(SegmentKey {
      table_name: self.req.table_name.clone(),
      partition,
      segment_id: Uuid::from_str(&self.req.segment_id)?,
    })
  }

  async fn execute_with_locks(
    &self,
    server: &Server,
    locks: DeletionWriteLocks,
  ) -> ServerResult<DeleteFromSegmentResponse> {
    common::validate_entity_name_for_write("table name", &self.req.table_name)?;

    let dir = &server.opts.dir;

    let DeletionWriteLocks {
      table_meta: _,
      deletion_guard: _,
      segment_key,
    } = locks;

    let max_row_id = match self.req.row_ids.iter().max() {
      Some(id) => Ok(*id),
      None => Err(ServerError::invalid("no row ids provided to delete"))
    }?;
    let row_ids: HashSet<_> = self.req.row_ids.iter().cloned().collect();

    // briefly get a read lock on segment metadata so we know what version to
    let segment_meta_lock = server.segment_metadata_cache.get_lock(&segment_key).await?;
    let segment_meta = {
      common::unwrap_metadata(
        &segment_key,
        &segment_meta_lock.read().await.clone()
      )?
    };

    if max_row_id >= segment_meta.all_time_n {
      return Err(ServerError::invalid(format!(
        "requested to deleted row id of {} when max row id in segment is {}",
        max_row_id,
        segment_meta.all_time_n - 1,
      )))
    }

    let old_deletion_id = segment_meta.deletion_id;
    let new_deletion_id = old_deletion_id + 1;

    let mut maybe_n_deleted = None;
    for &version in &segment_meta.write_versions {
      let compaction_key = segment_key.compaction_key(version);
      let pre_compaction_deletions = server.read_pre_compaction_deletions(
        &compaction_key
      ).await?;
      let n_pre = pre_compaction_deletions.len();
      let mut post_compaction_deletions = server.read_post_compaction_deletions(
        &compaction_key,
        old_deletion_id,
      ).await?;

      let mut version_n_deleted = 0;
      let mut post_i = 0;
      for row_id in 0..(max_row_id + 1) as usize {
        let pre_deleted = row_id < n_pre && pre_compaction_deletions[row_id];
        if post_i >= post_compaction_deletions.len() {
          post_compaction_deletions.push(false);
        }

        if !pre_deleted {
          if row_ids.contains(&(row_id as u32)) && !post_compaction_deletions[post_i]{
            post_compaction_deletions[post_i] = true;
            version_n_deleted += 1;
          }

          post_i += 1
        }
      }

      match maybe_n_deleted {
        Some(n) if n == version_n_deleted => Ok(()),
        Some(n) => Err(ServerError::internal(format!(
          "number of rows deleted did not agree among versions; {} vs {} for {}",
          n,
          version_n_deleted,
          version,
        ))),
        None => {
          maybe_n_deleted = Some(version_n_deleted);
          Ok(())
        }
      }?;

      let new_deletions_path = dirs::post_compaction_deletions_path(
        dir,
        &compaction_key,
        new_deletion_id,
      );
      log::debug!("writing new deletion {} for {}", new_deletion_id, compaction_key);
      common::overwrite_file(
        new_deletions_path,
        deletion::compress_deletions(
          &post_compaction_deletions
        )?,
      ).await?;
    }

    let n_deleted = maybe_n_deleted.unwrap();
    // briefly grab segment meta and write the new deletion id
    // and # of deletions to it
    {
      let mut segment_meta_guard = segment_meta_lock.write().await;
      if segment_meta_guard.is_none() {
        return Err(ServerError::internal("segment metadata unexpectedly disappeared during deletion"));
      }

      let segment_meta = segment_meta_guard.as_mut().unwrap();
      segment_meta.deletion_id = new_deletion_id;
      segment_meta.all_time_deleted_n += n_deleted;
      segment_meta.overwrite(dir, &segment_key).await?;
    }

    Ok(DeleteFromSegmentResponse {
      n_deleted,
      ..Default::default()
    })
  }
}
