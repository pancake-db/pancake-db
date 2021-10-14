use async_trait::async_trait;
use tokio::sync::OwnedRwLockWriteGuard;
use uuid::Uuid;

use crate::{dirs, utils};
use crate::errors::{ServerError, ServerResult};
use crate::locks::traits::ServerOpLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::Metadata;
use crate::storage::partition::PartitionMetadata;
use crate::storage::segment::SegmentMetadata;
use crate::storage::table::TableMetadata;
use crate::types::{PartitionKey, SegmentKey};

pub struct PartitionWriteLocks {
  pub table_meta: TableMetadata,
  pub definitely_partition_guard: OwnedRwLockWriteGuard<Option<PartitionMetadata>>,
  pub definitely_segment_guard: OwnedRwLockWriteGuard<Option<SegmentMetadata>>,
  pub segment_key: SegmentKey,
}

#[async_trait]
impl ServerOpLocks for PartitionWriteLocks {
  type Key = PartitionKey;

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op
  ) -> ServerResult<Op::Response> {
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
    let partition_lock = server.partition_metadata_cache.get_lock(&key)
      .await?;
    let mut partition_guard = partition_lock.write_owned().await;
    let maybe_partition_meta = &mut *partition_guard;

    if maybe_partition_meta.is_none() {
      let id = Uuid::new_v4().to_string();
      let partition_meta = PartitionMetadata::new(&id);
      utils::create_if_new(&dirs::partition_dir(&server.opts.dir, &key)).await?;
      partition_meta.overwrite(&server.opts.dir, &key).await?;
      *maybe_partition_meta = Some(partition_meta.clone());
    };

    let partition_meta = maybe_partition_meta.as_mut().unwrap();
    let segment_id = &partition_meta.write_segment_id;
    let segment_key = key.segment_key(segment_id.to_string());

    let segment_lock = server.segment_metadata_cache.get_lock(&segment_key).await?;
    let mut segment_guard = segment_lock.write_owned().await;
    let maybe_segment_meta = &mut *segment_guard;
    if maybe_segment_meta.is_none() {
      utils::create_segment_dirs(&dirs::segment_dir(&server.opts.dir, &segment_key)).await?;
      let segment_meta = SegmentMetadata::default();
      *maybe_segment_meta = Some(segment_meta);
    }

    let locks = PartitionWriteLocks {
      table_meta,
      definitely_partition_guard: partition_guard,
      definitely_segment_guard: segment_guard,
      segment_key,
    };

    op.execute_with_locks(server, locks).await
  }
}
