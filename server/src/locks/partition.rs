use async_trait::async_trait;
use tokio::sync::OwnedRwLockWriteGuard;

use crate::errors::{ServerError, ServerResult};
use crate::locks::traits::ServerOpLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::global::GlobalMetadata;
use crate::storage::Metadata;
use crate::storage::partition::PartitionMetadata;
use crate::storage::segment::SegmentMetadata;
use crate::storage::table::TableMetadata;
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

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op
  ) -> ServerResult<Op::Response> {
    let dir = &server.opts.dir;

    // global lock
    let global_lock = server.global_metadata_cache.get_lock(&()).await?;
    let global_guard = global_lock.read().await;
    let global_meta = common::unwrap_metadata(&(), &*global_guard)?;

    // table lock
    let key: PartitionKey = op.get_key()?;
    let table_name = &key.table_name;
    let table_lock = server.table_metadata_cache.get_lock(table_name).await?;
    let table_guard = table_lock.read().await;
    let maybe_table = table_guard.clone();
    if maybe_table.is_none() {
      return Err(ServerError::does_not_exist("table", table_name));
    }
    let table_meta = maybe_table.unwrap();

    key.partition.check_against_schema(&table_meta.schema)?;

    // partition lock
    // Ideally we'd check the read lock, return it if Some, and otherwise acquire drop that read
    // lock, acquire a write lock, overwrite, drop the write lock, and acquire a read lock again.
    // But for now just get a write lock for simplicity at the cost of some contention.
    let partition_lock = server.partition_metadata_cache.get_lock(&key)
      .await?;
    let mut partition_guard = partition_lock.write_owned().await;
    let partition_meta = match &mut *partition_guard {
      Some(meta) => meta,
      None => {
        let partition_meta = PartitionMetadata::new(global_meta.n_shards_log);
        common::create_if_new(&dirs::partition_dir(dir, &key)).await?;
        partition_meta.overwrite(dir, &key).await?;
        *partition_guard = Some(partition_meta.clone());
        partition_guard.as_mut().unwrap()
      },
    };

    let shard_id = ShardId::randomly_select(
      global_meta.n_shards_log,
      1, // TODO use table_meta
      &key,
      partition_meta.sharding_denominator_log,
    );
    let maybe_active_segment_id = partition_meta.get_active_segment_id(&shard_id);

    // segment lock
    let segment_id = match maybe_active_segment_id {
      Some(id) => id,
      None => {
        let segment_id = shard_id.generate_segment_id();
        partition_meta.active_segment_ids.push(segment_id);
        partition_meta.overwrite(dir, &key).await?;
        segment_id
      }
    };
    let segment_key = key.segment_key(segment_id);

    let segment_lock = server.segment_metadata_cache.get_lock(&segment_key).await?;
    let mut segment_guard = segment_lock.write_owned().await;
    let maybe_segment_meta = &mut *segment_guard;
    if maybe_segment_meta.is_none() {
      navigation::create_segment_dirs(&dirs::segment_dir(&server.opts.dir, &segment_key)).await?;
      let segment_meta = SegmentMetadata::default();
      *maybe_segment_meta = Some(segment_meta);
    }

    let locks = PartitionWriteLocks {
      global_meta,
      table_meta,
      definitely_partition_guard: partition_guard,
      definitely_segment_guard: segment_guard,
      shard_id,
      segment_key,
    };

    op.execute_with_locks(server, locks).await
  }
}
