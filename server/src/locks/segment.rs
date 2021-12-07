use async_trait::async_trait;

use crate::errors::{ServerError, ServerResult};
use crate::locks::traits::ServerOpLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::segment::SegmentMetadata;
use crate::types::SegmentKey;
use tokio::sync::OwnedRwLockWriteGuard;
use crate::storage::table::TableMetadata;

pub struct SegmentWriteLocks {
  pub table_meta: TableMetadata,
  pub definitely_segment_guard: OwnedRwLockWriteGuard<Option<SegmentMetadata>>,
  pub segment_key: SegmentKey,
}

pub struct SegmentReadLocks {
  pub table_meta: TableMetadata,
  pub segment_meta: SegmentMetadata,
  pub segment_key: SegmentKey,
}

#[async_trait]
impl ServerOpLocks for SegmentWriteLocks {
  type Key = SegmentKey;

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> {
    let key: SegmentKey = op.get_key()?;
    let table_name = &key.table_name;
    let table_lock = server.table_metadata_cache.get_lock(table_name).await?;
    let table_guard = table_lock.read().await;
    let maybe_table = table_guard.clone();
    if maybe_table.is_none() {
      return Err(ServerError::does_not_exist("table", table_name));
    }

    let table_meta = maybe_table.unwrap();
    key.partition.check_against_schema(&table_meta.schema)?;

    let segment_meta_lock = server.segment_metadata_cache.get_lock(&key).await?;
    let mut segment_guard = segment_meta_lock.write_owned().await;
    let maybe_segment_meta = &mut *segment_guard;
    if maybe_segment_meta.is_none() {
      *maybe_segment_meta = Some(SegmentMetadata::new_from_schema(&table_meta.schema));
    }

    let locks = SegmentWriteLocks {
      table_meta,
      definitely_segment_guard: segment_guard,
      segment_key: key,
    };

    op.execute_with_locks(server, locks).await
  }
}

#[async_trait]
impl ServerOpLocks for SegmentReadLocks {
  type Key = SegmentKey;

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> {
    let key: SegmentKey = op.get_key()?;
    let table_name = &key.table_name;
    let table_lock = server.table_metadata_cache.get_lock(table_name).await?;
    let table_guard = table_lock.read().await;
    let maybe_table = table_guard.clone();
    if maybe_table.is_none() {
      return Err(ServerError::does_not_exist("table", table_name));
    }

    let table_meta = maybe_table.unwrap();
    key.partition.check_against_schema(&table_meta.schema)?;

    let segment_meta_lock = server.segment_metadata_cache.get_lock(&key).await?;
    let segment_guard = segment_meta_lock.read().await;
    let maybe_segment_meta = segment_guard.clone();

    if maybe_segment_meta.is_none() {
      return Err(ServerError::does_not_exist("segment", &key.segment_id.to_string()));
    }

    let segment_meta = maybe_segment_meta.unwrap();
    let locks = SegmentReadLocks {
      table_meta,
      segment_meta,
      segment_key: key,
    };

    op.execute_with_locks(server, locks).await
  }
}
