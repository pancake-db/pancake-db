use std::collections::HashSet;

use tokio::fs;
use uuid::Uuid;

use crate::errors::ServerResult;
use crate::ops::compact::CompactionOp;
use crate::ops::drop_table::DropTableOp;
use crate::ops::flush::FlushOp;
use crate::ops::write_to_partition::WriteToPartitionOp;
use crate::server::Server;
use crate::metadata::compaction::Compaction;
use crate::metadata::PersistentMetadata;
use crate::metadata::partition::PartitionMetadata;
use crate::metadata::segment::SegmentMetadata;
use crate::types::{InternalTableInfo, NormalizedPartition, PartitionKey};
use crate::utils::{common, navigation};

impl Server {
  pub async fn internal_list_tables(&self) -> ServerResult<Vec<InternalTableInfo>> {
    let mut tables = Vec::new();
    let mut read_dir = fs::read_dir(&self.opts.dir).await?;
    while let Ok(Some(entry)) = read_dir.next_entry().await {
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

  pub async fn recover(&self) -> ServerResult<()> {
    log::info!("recovering to clean state");

    for table_info in self.internal_list_tables().await? {
      self.recover_table(table_info).await?;
    }
    Ok(())
  }

  async fn recover_table(&self, table_info: InternalTableInfo) -> ServerResult<()> {
    log::debug!("recovering table {}", table_info.name);
    // 1. Dropped tables
    let InternalTableInfo {
      name: table_name,
      meta: table_meta
    } = table_info;

    if table_meta.dropped {
      DropTableOp::recover(self, &table_name, &table_meta).await?;
      return Ok(());
    }

    let dir = &self.opts.dir;
    for partition in &self.list_partitions(
      &table_name,
      &table_meta.schema.partitioning,
      &Vec::new(),
    ).await? {
      let normalized = NormalizedPartition::from_raw_fields(partition)?;
      let partition_key = PartitionKey {
        table_name: table_name.clone(),
        partition: normalized
      };
      let maybe_partition_meta = PartitionMetadata::load(dir, &partition_key)
        .await?;

      if maybe_partition_meta.is_none() {
        continue;
      }
      let partition_meta = maybe_partition_meta.unwrap();
      let all_segment_ids = navigation::list_segment_ids(dir, &partition_key).await?;
      let active_segment_ids: HashSet<Uuid> = partition_meta.active_segment_ids.into_iter().collect();

      for segment_id in &all_segment_ids {
        let segment_key = partition_key.segment_key(*segment_id);
        let mut maybe_segment_meta = SegmentMetadata::load(dir, &segment_key).await?;

        if maybe_segment_meta.is_none() {
          continue;
        }
        let segment_meta = maybe_segment_meta.as_mut().unwrap();

        // 2. Compactions
        CompactionOp::recover(self, &segment_key, segment_meta).await?;

        // 3. Flushes
        FlushOp::recover(self, &table_meta, &segment_key, segment_meta).await?;

        if active_segment_ids.contains(segment_id) {
          // 4. Writes
          WriteToPartitionOp::recover(self, &segment_key, segment_meta).await?;
        }

        // 5. background state
        // 5a. flush candidates
        if segment_meta.staged_n > 0 {
          log::debug!("adding segment {} as flush candidate", segment_key);
          self.background.add_flush_candidate(segment_key.clone()).await;
        }
        // 5b. compaction candidates
        let compaction_key = segment_key.compaction_key(segment_meta.read_version);
        let compaction = Compaction::load(dir, &compaction_key)
          .await?
          .unwrap_or_default();
        let flush_only_n = common::flush_only_n(segment_meta, &compaction);
        if flush_only_n > 0 {
          log::debug!("adding segment {} as compaction candidate", segment_key);
          self.background.add_compaction_candidate(segment_key).await;
        }
      }
    }

    Ok(())
  }
}