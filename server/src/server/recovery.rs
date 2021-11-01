use tokio::fs;

use crate::constants::TABLE_METADATA_FILENAME;
use crate::errors::ServerResult;
use crate::ops::drop_table::DropTableOp;
use crate::server::Server;
use crate::storage::Metadata;
use crate::storage::table::TableMetadata;
use crate::utils::common;
use crate::types::{NormalizedPartition, PartitionKey};
use crate::storage::partition::PartitionMetadata;
use crate::storage::segment::SegmentMetadata;
use crate::ops::compact::CompactionOp;
use crate::ops::write_to_partition::WriteToPartitionOp;
use crate::ops::flush::FlushOp;
use crate::storage::compaction::Compaction;

struct TableInfo {
  pub name: String,
  pub meta: TableMetadata,
}

impl Server {
  async fn list_tables(&self) -> ServerResult<Vec<TableInfo>> {
    let mut res = Vec::new();
    let mut read_dir = fs::read_dir(&self.opts.dir).await?;
    while let Ok(Some(entry)) = read_dir.next_entry().await {
      if !entry.file_type().await?.is_dir() {
        continue;
      }
      let path = entry.path();
      let possible_meta_path = path.join(TABLE_METADATA_FILENAME);
      if common::file_exists(&possible_meta_path).await? {
        let path_str = path.file_name().and_then(|s| s.to_str());
        if let Some(name) = path_str {
          let name = name.to_string();
          let meta = TableMetadata::load(
            &self.opts.dir,
            &name,
          ).await?.unwrap();
          res.push(TableInfo {
            name,
            meta,
          })
        }
      }
    }
    Ok(res)
  }

  pub async fn recover(&self) -> ServerResult<()> {
    log::info!("recovering to clean state");

    for table_info in self.list_tables().await? {
      self.recover_table(table_info).await?;
    }
    Ok(())
  }

  async fn recover_table(&self, table_info: TableInfo) -> ServerResult<()> {
    log::debug!("recovering table {}", table_info.name);
    // 1. Dropped tables
    let TableInfo {
      name: table_name,
      meta: table_meta
    } = table_info;

    if table_meta.dropped {
      DropTableOp::recover(&self, &table_name, &table_meta).await?;
      return Ok(());
    }

    let dir = &self.opts.dir;
    for partition in &self.list_partitions(
      &table_name,
      table_meta.schema.partitioning.clone(),
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

      for segment_id in &partition_meta.segment_ids {
        let segment_key = partition_key.segment_key(segment_id.clone());
        let mut maybe_segment_meta = SegmentMetadata::load(dir, &segment_key).await?;

        if maybe_segment_meta.is_none() {
          continue;
        }
        let segment_meta = maybe_segment_meta.as_mut().unwrap();

        // 2. Compactions
        CompactionOp::recover(&self, &segment_key, segment_meta).await?;

        // 3. Flushes
        FlushOp::recover(&self, &table_meta, &segment_key, segment_meta).await?;

        if segment_id == &partition_meta.write_segment_id {
          // 4. Writes
          WriteToPartitionOp::recover(&self, &segment_key, segment_meta).await?;
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
        let flush_only_n = common::flush_only_n(&segment_meta, &compaction);
        if flush_only_n > 0 {
          log::debug!("adding segment {} as compaction candidate", segment_key);
          self.background.add_compaction_candidate(segment_key).await;
        }
      }
    }

    Ok(())
  }
}