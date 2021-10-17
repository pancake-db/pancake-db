use async_trait::async_trait;
use pancake_db_idl::dml::{ListSegmentsRequest, ListSegmentsResponse, Segment};
use pancake_db_idl::dml::SegmentMetadata as PbSegmentMetadata;
use protobuf::MessageField;

use crate::errors::ServerResult;
use crate::locks::table::TableReadLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::segment::SegmentMetadata;
use crate::types::{NormalizedPartition, PartitionKey};
use crate::utils::common;

pub struct ListSegmentsOp {
  pub req: ListSegmentsRequest,
}

impl ListSegmentsOp {
  fn pb_segment_meta_from_option(maybe_segment_meta: Option<SegmentMetadata>) -> MessageField<PbSegmentMetadata> {
    MessageField::from_option(maybe_segment_meta.map(|meta| {
      let count = (meta.all_time_n - meta.all_time_deleted_n) as u32;
      PbSegmentMetadata {
        latest_version: meta.read_version,
        count,
        ..Default::default()
      }
    }))
  }
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
        let metadata = if req.include_metadata {
          let segment_key = partition_key.segment_key(segment_id.clone());
          let segment_lock = server.segment_metadata_cache
            .get_lock(&segment_key)
            .await?;
          let segment_guard = segment_lock.read().await;
          let maybe_segment_meta = segment_guard.clone();
          Self::pb_segment_meta_from_option(maybe_segment_meta)
        } else {
          MessageField::none()
        };
        segments.push(Segment {
          partition: partition.clone(),
          segment_id: segment_id.clone(),
          metadata,
          ..Default::default()
        });
      }
    }

    Ok(ListSegmentsResponse {
      segments,
      ..Default::default()
    })
  }
}