use std::collections::{HashSet, HashMap};

use async_trait::async_trait;
use futures::{pin_mut, StreamExt};
use pancake_db_idl::dml::{ListSegmentsRequest, ListSegmentsResponse, PartitionFieldValue, Segment};
use pancake_db_idl::dml::SegmentMetadata as PbSegmentMetadata;

use crate::errors::ServerResult;
use crate::locks::table::GlobalTableReadLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::metadata::segment::SegmentMetadata;
use crate::types::{NormalizedPartition, PartitionKey};
use crate::utils::{common, navigation, sharding};

pub struct ListSegmentsOp {
  pub req: ListSegmentsRequest,
}

impl ListSegmentsOp {
  fn pb_segment_meta_from_option(maybe_segment_meta: Option<SegmentMetadata>) -> Option<PbSegmentMetadata> {
    maybe_segment_meta.map(|meta| {
      let row_count = (meta.all_time_n - meta.all_time_deleted_n) as u32;
      PbSegmentMetadata {
        row_count,
        ..Default::default()
      }
    })
  }

  async fn list_shards_segments(
    &self,
    server: &Server,
    table_name: &str,
    partitions: &[HashMap<String, PartitionFieldValue>],
    n_shards_log: u32,
    shards: HashSet<u64>,
    include_metadata: bool,
  ) -> ServerResult<Vec<Segment>> {
    let mut segments = Vec::new();
    for partition in partitions {
      let normalized_partition = NormalizedPartition::from_raw_fields(partition)?;
      let partition_key = PartitionKey {
        table_name: table_name.to_string(),
        partition: normalized_partition,
      };

      let segment_id_stream = navigation::stream_segment_ids_for_partition(
        &server.opts.dir,
        partition_key.clone(),
      );
      pin_mut!(segment_id_stream);
      while let Some(segment_id_result) = segment_id_stream.next().await {
        let segment_id = segment_id_result?;

        if !shards.contains(&sharding::segment_id_to_shard(n_shards_log, segment_id)) {
          continue;
        }

        let metadata = if include_metadata {
          let segment_key = partition_key.segment_key(segment_id);
          let segment_lock = server.segment_metadata_cache
            .get_lock(&segment_key)
            .await?;
          let segment_guard = segment_lock.read().await;
          let maybe_segment_meta = segment_guard.clone();
          Self::pb_segment_meta_from_option(maybe_segment_meta)
        } else {
          None
        };
        segments.push(Segment {
          partition: partition.clone(),
          segment_id: segment_id.to_string(),
          metadata,
          ..Default::default()
        });
      }
    }
    Ok(segments)
  }
}

#[async_trait]
impl ServerOp for ListSegmentsOp {
  type Locks = GlobalTableReadLocks;
  type Response = ListSegmentsResponse;

  fn get_key(&self) -> ServerResult<String> {
    Ok(self.req.table_name.clone())
  }

  async fn execute_with_locks(&self, server: &Server, locks: GlobalTableReadLocks) -> ServerResult<ListSegmentsResponse> {
    let req = &self.req;
    let table_name = &req.table_name;
    common::validate_entity_name_for_read("table name", table_name)?;

    let GlobalTableReadLocks {
      global_meta,
      table_meta,
    } = locks;

    let partitioning = table_meta.schema().partitioning.clone();
    let partitions = navigation::partitions_for_table(
      &server.opts.dir,
      table_name,
      &partitioning,
      &req.partition_filter,
    ).await?;

    let n_shards = 1_u64 << global_meta.n_shards_log;
    let mut all_shards = HashSet::new();
    for shard in 0..n_shards {
      // seems silly for now, but will make sense when we have distributed sharding
      all_shards.insert(shard);
    }
    let segments = self.list_shards_segments(
      server,
      table_name,
      &partitions,
      global_meta.n_shards_log,
      all_shards,
      req.include_metadata,
    ).await?;

    Ok(ListSegmentsResponse {
      segments,
      ..Default::default()
    })
  }
}