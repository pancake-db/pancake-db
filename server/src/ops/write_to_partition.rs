use async_trait::async_trait;
use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse};
use uuid::Uuid;
use tokio::fs;

use crate::utils::dirs;
use crate::errors::{ServerResult, ServerError};
use crate::locks::partition::PartitionWriteLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::Metadata;
use crate::types::{NormalizedPartition, PartitionKey, SegmentKey};
use crate::utils::common;
use crate::storage::segment::SegmentMetadata;
use std::path::Path;

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
    common::validate_entity_name_for_read("table name", &self.req.table_name)?;

    let dir = &server.opts.dir;
    let PartitionWriteLocks {
      table_meta,
      mut definitely_partition_guard,
      mut definitely_segment_guard,
      mut segment_key,
    } = locks;
    let partition_meta = definitely_partition_guard.as_mut().unwrap();
    let mut segment_meta = definitely_segment_guard.as_mut().unwrap();

    common::validate_rows(&table_meta.schema, &self.req.rows)?;

    let mut default_segment_meta = SegmentMetadata::default();
    if segment_meta.all_time_n >= server.opts.default_rows_per_segment + segment_meta.all_time_deleted_n {
      let new_segment_id = Uuid::new_v4().to_string();
      let key = SegmentKey {
        table_name: segment_key.table_name.clone(),
        partition: segment_key.partition.clone(),
        segment_id: new_segment_id.clone(),
      };
      common::create_segment_dirs(&dirs::segment_dir(dir, &key)).await?;
      partition_meta.write_segment_id = new_segment_id.clone();
      partition_meta.segment_ids.push(new_segment_id);
      partition_meta.overwrite(dir, &segment_key.partition_key()).await?;

      segment_key = key;
      segment_meta = &mut default_segment_meta;
    }

    let staged_bytes = common::rows_to_staged_bytes(&self.req.rows)?;
    common::append_to_file(
      dirs::staged_rows_path(&server.opts.dir, &segment_key),
      &staged_bytes,
    ).await?;

    let n_rows = self.req.rows.len();
    Self::increment_segment_meta_n_rows(n_rows, segment_meta, dir, &segment_key).await?;

    server.add_flush_candidate(segment_key).await;

    Ok(WriteToPartitionResponse {..Default::default()})
  }
}

impl WriteToPartitionOp {
  async fn increment_segment_meta_n_rows(
    n_rows: usize,
    segment_meta: &mut SegmentMetadata,
    dir: &Path,
    segment_key: &SegmentKey
  ) -> ServerResult<()> {
    if n_rows > 0 {
      segment_meta.all_time_n += n_rows as u64;
      segment_meta.staged_n += n_rows;
      segment_meta.overwrite(dir, &segment_key).await
    } else {
      Ok(())
    }
  }

  pub async fn recover(
    server: &Server,
    segment_key: &SegmentKey,
    segment_meta: &mut SegmentMetadata,
  ) -> ServerResult<()> {
    let dir = &server.opts.dir;
    let staged_rows_path = dirs::staged_rows_path(dir, &segment_key);
    let staged_rows = common::staged_bytes_to_rows(&fs::read(&staged_rows_path).await?)?;
    if staged_rows.len() < segment_meta.staged_n {
      return Err(ServerError::internal(&format!(
        "segment {} is in an impossible state with fewer rows ({}) in staged file than in metadata ({})",
        segment_key,
        staged_rows.len(),
        segment_meta.staged_n,
      )))
    }
    let n_rows = staged_rows.len() - segment_meta.staged_n;
    if n_rows > 0 {
      log::debug!(
        "identified missing staged rows in segment {} metadata; filling them in",
        segment_key,
      )
    }
    Self::increment_segment_meta_n_rows(n_rows, segment_meta, dir, &segment_key).await
  }
}