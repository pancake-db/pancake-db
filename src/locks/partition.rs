use async_trait::async_trait;
use tokio::sync::OwnedRwLockWriteGuard;
use crate::Contextable;

use crate::errors::ServerResult;
use crate::locks::table::GlobalTableReadLocks;
use crate::locks::traits::ServerOpLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::metadata::global::GlobalMetadata;
use crate::metadata::PersistentMetadata;
use crate::metadata::partition::PartitionMetadata;
use crate::metadata::segment::SegmentMetadata;
use crate::metadata::table::TableMetadata;
use crate::types::{PartitionKey, SegmentKey, ShardId};
use crate::utils::{common, navigation};
use crate::utils::dirs;

pub struct PartitionWriteLocks {
  pub global_meta: GlobalMetadata,
  pub table_meta: TableMetadata,
  pub definitely_partition_guard: OwnedRwLockWriteGuard<Option<PartitionMetadata>>,
  pub definitely_segment_guard: OwnedRwLockWriteGuard<Option<SegmentMetadata>>,
  pub shard_id: ShardId,
  pub segment_key: SegmentKey,
}

#[async_trait]
impl ServerOpLocks for PartitionWriteLocks {
  type Key = PartitionKey;

  async fn execute<Op: ServerOp<Locks=Self>>(
    server: &Server,
    op: &Op
  ) -> ServerResult<Op::Response> {
    let key = op.get_key()?;
    let table_locks = GlobalTableReadLocks::obtain(server, &key.table_name).await?;

    let locks = Self::from_table_read(table_locks, &key, server).await?;

    op.execute_with_locks(server, locks).await
  }
}

impl PartitionWriteLocks {
  pub async fn from_table_read(
    table_read_locks: GlobalTableReadLocks,
    key: &PartitionKey,
    server: &Server,
  ) -> ServerResult<Self> {
    let GlobalTableReadLocks { global_meta, table_meta } = table_read_locks;
    let dir = &server.opts.dir;

    key.partition.check_against_schema(&table_meta.schema())?;

    // partition lock
    // Ideally we'd check the read lock, return it if Some, and otherwise acquire drop that read
    // lock, acquire a write lock, overwrite, drop the write lock, and acquire a read lock again.
    // But for now just get a write lock for simplicity at the cost of some contention.
    let partition_lock = server.partition_metadata_cache.get_lock(key)
      .await?;
    let mut partition_guard = partition_lock.write_owned().await;
    let partition_meta = match &mut *partition_guard {
      Some(meta) => meta,
      None => {
        let partition_meta = PartitionMetadata::new(global_meta.n_shards_log);
        common::create_if_new(&dirs::partition_dir(dir, key)).await?;
        partition_meta.overwrite(dir, key).await?;
        *partition_guard = Some(partition_meta.clone());
        partition_guard.as_mut().unwrap()
      },
    };

    let shard_id = ShardId::randomly_select(
      global_meta.n_shards_log,
      1, // TODO use table_meta
      key,
      partition_meta.sharding_denominator_log,
    );
    let maybe_active_segment_id = partition_meta.get_active_segment_id(&shard_id);

    // segment lock
    let segment_id = match maybe_active_segment_id {
      Some(id) => id,
      None => {
        let segment_id = shard_id.generate_segment_id();
        partition_meta.active_segment_ids.push(segment_id);
        partition_meta.overwrite(dir, key).await?;
        segment_id
      }
    };
    let segment_key = key.segment_key(segment_id);

    let segment_lock = server.segment_metadata_cache.get_lock(&segment_key).await?;
    let mut segment_guard = segment_lock.write_owned().await;
    let maybe_segment_meta = &mut *segment_guard;
    if maybe_segment_meta.is_none() {
      navigation::create_segment_dirs(&dirs::segment_dir(&server.opts.dir, &segment_key)).await
        .with_context(|| format!(
          "while creating new segment dirs for {}",
          segment_key,
        ))?;
      let segment_meta = SegmentMetadata::new_from_schema(&table_meta.schema());
      *maybe_segment_meta = Some(segment_meta);
    }

    Ok(PartitionWriteLocks {
      global_meta,
      table_meta,
      definitely_partition_guard: partition_guard,
      definitely_segment_guard: segment_guard,
      shard_id,
      segment_key,
    })
  }
}
