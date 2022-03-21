use std::collections::HashSet;
use futures::pin_mut;

use uuid::Uuid;
use futures::StreamExt;

use crate::errors::Contextable;
use crate::errors::ServerResult;
use crate::ops::compact::CompactionOp;
use crate::ops::drop_table::DropTableOp;
use crate::ops::flush::FlushOp;
use crate::ops::write_to_partition::WriteToPartitionOp;
use crate::server::Server;
use crate::metadata::PersistentMetadata;
use crate::metadata::partition::PartitionMetadata;
use crate::metadata::segment::SegmentMetadata;
use crate::types::{InternalTableInfo, NormalizedPartition, PartitionKey};
use crate::utils::navigation;

impl Server {
  pub async fn recover(&self) -> ServerResult<()> {
    log::info!("recovering to clean state");

    let table_infos = self.internal_list_tables()
      .await
      .with_context(|| "while listing tables")?;
    for table_info in &table_infos {
      self.recover_table(table_info.clone())
        .await
        .with_context(|| format!("while recovering table {}", table_info.name))?;
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
      DropTableOp::recover(self, &table_name, &table_meta)
        .await
        .with_context(|| "while completing drop table op")?;
      return Ok(());
    }

    let dir = &self.opts.dir;
    for partition in navigation::partitions_for_table(
      dir,
      &table_name,
      &table_meta.schema().partitioning,
      &Vec::new(),
    ).await.with_context(|| "while listing partitions")? {
      let normalized = NormalizedPartition::from_raw_fields(&partition)
        .with_context(|| format!("while normalizing partition {:?}", &partition))?;
      let partition_key = PartitionKey {
        table_name: table_name.clone(),
        partition: normalized.clone(),
      };
      let maybe_partition_meta = PartitionMetadata::load(dir, &partition_key)
        .await
        .with_context(|| format!("while loading partition meta {}", normalized))?;

      if maybe_partition_meta.is_none() {
        continue;
      }
      let partition_meta = maybe_partition_meta.unwrap();
      let active_segment_ids: HashSet<Uuid> = partition_meta.active_segment_ids
        .into_iter()
        .collect();

      let segment_id_stream = navigation::stream_segment_ids_for_partition(
        dir,
        partition_key.clone(),
      );
      pin_mut!(segment_id_stream);
      while let Some(segment_id_result) = segment_id_stream.next().await {
        let segment_id = segment_id_result
          .with_context(|| format!("while listing segment ids for {}", normalized))?;
        let segment_key = partition_key.segment_key(segment_id);
        let mut maybe_segment_meta = SegmentMetadata::load(dir, &segment_key).await
          .with_context(|| format!("while loading segment metadata for {}", segment_key))?;

        if maybe_segment_meta.is_none() {
          continue;
        }
        let segment_meta = maybe_segment_meta.as_mut().unwrap();

        // 2. Compactions
        CompactionOp::recover(self, &segment_key, segment_meta).await?;

        // 3. Flushes
        FlushOp::recover(self, &table_meta, &segment_key, segment_meta).await?;

        if active_segment_ids.contains(&segment_id) {
          // 4. Writes
          WriteToPartitionOp::recover(self, &segment_key, segment_meta).await?;
        }

        // 5. background state
        // 5a. flush candidates
        if segment_meta.staged_n > 0 {
          log::debug!("adding segment {} as flush candidate", segment_key);
          self.background.add_flush_candidate(segment_key.clone()).await;
        }
      }
    }

    Ok(())
  }
}
