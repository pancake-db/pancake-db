use async_trait::async_trait;
use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse};

use crate::dirs;
use crate::errors::ServerResult;
use crate::locks::partition::{PartitionWriteLocks, PartitionLockKey, PartitionReadLocks};
use crate::locks::segment::SegmentLockKey;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::Metadata;
use crate::utils;
use uuid::Uuid;
use tokio::fs;
use crate::types::SegmentKey;

pub struct WriteToPartitionOp {
  req: WriteToPartitionRequest,
}

#[async_trait]
impl<'a> ServerOp<PartitionWriteLocks<'a>> for WriteToPartitionOp {
  type Response = WriteToPartitionResponse;

  fn get_key(&self) -> PartitionLockKey {
    PartitionLockKey {
      table_name: self.req.table_name.clone(),
      partition: self.req.partition.clone(),
    }
  }

  async fn execute_with_locks(&self, server: &Server, locks: PartitionWriteLocks<'a>) -> ServerResult<WriteToPartitionResponse> {
    utils::validate_entity_name_for_read("table name", &self.req.table_name)?;

    let dir = &server.opts.dir;
    let PartitionWriteLocks {
      schema,
      partition_meta,
      segment_meta,
      segment_key,
    } = locks;

    utils::validate_rows(&schema, &self.req.rows)?;

    let segment_key = if segment_meta.all_time_n >= server.opts.default_rows_per_segment + segment_meta.all_time_n_deleted {
      let new_segment_id = Uuid::new_v4().to_string();
      partition_meta.write_segment_id = new_segment_id.clone();
      partition_meta.segment_ids.push(new_segment_id.clone());
      let key = SegmentKey {
        table_name: segment_key.table_name.clone(),
        partition: segment_key.partition.clone(),
        segment_id: new_segment_id,
      };
      fs::create_dir(dirs::segment_dir(dir, &key)).await?;
      key
    } else {
      segment_key
    };

    let staged_bytes = utils::rows_to_staged_bytes(&self.req.rows)?;
    utils::append_to_file(
      dirs::staged_rows_path(&server.opts.dir, &segment_key),
      &staged_bytes,
    ).await?;

    let n_rows = self.req.rows.len();
    segment_meta.all_time_n += n_rows as u64;
    segment_meta.currently_staged_n += n_rows;
    segment_meta.overwrite(dir, &segment_key).await?;

    server.add_flush_candidate(SegmentLockKey {
      table_name: self.req.table_name.clone(),
      partition: self.req.partition.clone(),
      segment_id: segment_key.segment_id.clone(),
    }).await;

    Ok(WriteToPartitionResponse {..Default::default()})
  }
}