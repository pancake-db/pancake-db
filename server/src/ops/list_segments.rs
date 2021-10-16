use async_trait::async_trait;
use pancake_db_idl::dml::{ListSegmentsRequest, ListSegmentsResponse, Segment};

use crate::errors::ServerResult;
use crate::locks::table::TableReadLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::types::{NormalizedPartition, PartitionKey};
use crate::utils::common;

pub struct ListSegmentsOp {
  pub req: ListSegmentsRequest,
}

#[async_trait]
impl ServerOp<TableReadLocks> for ListSegmentsOp {
  type Response = ListSegmentsResponse;

  fn get_key(&self) -> ServerResult<String> {
    Ok(self.req.table_name.clone())
  }

  async fn execute_with_locks(&self, server: &Server, locks: TableReadLocks) -> ServerResult<ListSegmentsResponse> {
    let req = &self.req;
    let table_name = &req.table_name;
    common::validate_entity_name_for_read("table name", &table_name)?;

    let TableReadLocks { table_meta } = locks;

    let partitioning = table_meta.schema.partitioning.clone();
    let partitions = server.list_partitions(
      table_name,
      partitioning,
      &req.partition_filter,
    ).await?;

    let mut segments = Vec::new();
    for partition in &partitions {
      let normalized_partition = NormalizedPartition::from_raw_fields(partition)?;
      let partition_key = PartitionKey {
        table_name: table_name.clone(),
        partition: normalized_partition.clone(),
      };

      let partition_meta_lock = server.partition_metadata_cache
        .get_lock(&partition_key)
        .await?;
      let partition_meta_guard = partition_meta_lock.read().await;
      let maybe_partition_meta = &*partition_meta_guard;

      if maybe_partition_meta.is_none() {
        log::warn!("missing segments metadata for table {} partition {}", table_name, normalized_partition);
        continue;
      }

      let partition_meta = maybe_partition_meta.as_ref().unwrap();
      for segment_id in &partition_meta.segment_ids {
        let count = if req.include_counts {
          let segment_key = partition_key.segment_key(segment_id.clone());
          let segment_lock = server.segment_metadata_cache
            .get_lock(&segment_key)
            .await?;
          let segment_guard = segment_lock.read().await;
          let maybe_segment_meta = segment_guard.clone();
          maybe_segment_meta.map(|meta| (meta.all_time_n - meta.all_time_deleted_n) as u32).unwrap_or(0)
        } else {
          0_u32
        };
        segments.push(Segment {
          partition: partition.clone(),
          segment_id: segment_id.clone(),
          count,
          ..Default::default()
        });
      }
    }

    Ok(ListSegmentsResponse {
      segments,
      continuation_token: "".to_string(),
      ..Default::default()
    })
  }
}