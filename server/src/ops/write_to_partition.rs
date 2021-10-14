use async_trait::async_trait;
use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse};
use uuid::Uuid;

use crate::dirs;
use crate::errors::ServerResult;
use crate::locks::partition::PartitionWriteLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::Metadata;
use crate::types::{NormalizedPartition, PartitionKey, SegmentKey};
use crate::utils;

pub struct WriteToPartitionOp {
  pub req: WriteToPartitionRequest,
}

#[async_trait]
impl ServerOp<PartitionWriteLocks> for WriteToPartitionOp {
  type Response = WriteToPartitionResponse;

  fn get_key(&self) -> ServerResult<PartitionKey> {
    let partition = NormalizedPartition::from_raw_fields(&self.req.partition)?;
    Ok(PartitionKey {
      table_name: self.req.table_name.clone(),
      partition,
    })
  }

  async fn execute_with_locks(&self, server: &Server, locks: PartitionWriteLocks) -> ServerResult<WriteToPartitionResponse> {
    utils::validate_entity_name_for_read("table name", &self.req.table_name)?;

    let dir = &server.opts.dir;
    let PartitionWriteLocks {
      table_meta,
      mut definitely_partition_guard,
      mut definitely_segment_guard,
      segment_key,
    } = locks;
    let partition_meta = definitely_partition_guard.as_mut().unwrap();
    let segment_meta = definitely_segment_guard.as_mut().unwrap();

    utils::validate_rows(&table_meta.schema, &self.req.rows)?;

    let segment_key = if segment_meta.all_time_n >= server.opts.default_rows_per_segment + segment_meta.all_time_n_deleted {
      let new_segment_id = Uuid::new_v4().to_string();
      let key = SegmentKey {
        table_name: segment_key.table_name.clone(),
        partition: segment_key.partition.clone(),
        segment_id: new_segment_id.clone(),
      };
      utils::create_segment_dirs(&dirs::segment_dir(dir, &key)).await?;
      partition_meta.write_segment_id = new_segment_id.clone();
      partition_meta.segment_ids.push(new_segment_id);
      partition_meta.overwrite(dir, &segment_key.partition_key()).await?;
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
    segment_meta.staged_n += n_rows;
    segment_meta.overwrite(dir, &segment_key).await?;

    server.add_flush_candidate(segment_key).await;

    Ok(WriteToPartitionResponse {..Default::default()})
  }
}