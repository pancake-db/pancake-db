use async_std::stream::Stream;
use async_stream::try_stream;
use futures::pin_mut;
use futures::StreamExt;
use tokio::fs;

use crate::{Server, ServerResult};
use crate::types::{InternalTableInfo, NormalizedPartition, PartitionKey, SegmentKey};
use crate::utils::navigation;

impl Server {
  pub async fn internal_list_tables(&self) -> ServerResult<Vec<InternalTableInfo>> {
    let mut tables = Vec::new();
    let mut read_dir = fs::read_dir(&self.opts.dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
      if !entry.file_type().await?.is_dir() {
        continue;
      }

      if let Some(possible_table_name) = entry.file_name().to_str() {
        let lock_res = self.table_metadata_cache.get_lock(&possible_table_name.to_string()).await;
        if let Err(err) = lock_res {
          log::error!("failed to read metadata when listing table {}: {}", possible_table_name, err);
          continue;
        }

        let lock = lock_res.unwrap();
        let guard = lock.read().await;
        if let Some(meta) = &*guard {
          tables.push(InternalTableInfo {
            name: possible_table_name.to_string(),
            meta: meta.clone(),
          });
        }
      }
    }
    Ok(tables)
  }

  pub fn stream_all_segment_keys(&self) -> impl Stream<Item=ServerResult<SegmentKey>> + '_ {
    try_stream! {
      let tables = self.internal_list_tables().await?;
      for table in &tables {
        let schema = table.meta.schema();
        let partitions = navigation::partitions_for_table(
          &self.opts.dir,
          &table.name,
          &schema.partitioning,
          &Vec::new(),
        ).await?;
        for partition in &partitions {
          let partition_key = PartitionKey {
            table_name: table.name.clone(),
            partition: NormalizedPartition::from_raw_fields(partition)?
          };

          let segment_id_stream = navigation::stream_segment_ids_for_partition(
            &self.opts.dir,
            partition_key.clone(),
          );
          pin_mut!(segment_id_stream);
          while let Some(segment_id_result) = segment_id_stream.next().await {
            let segment_id = segment_id_result?;
            yield partition_key.segment_key(segment_id);
          }
        }
      }
    }
  }
}