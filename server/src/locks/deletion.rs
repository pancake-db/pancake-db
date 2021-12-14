use async_trait::async_trait;
use tokio::sync::OwnedRwLockWriteGuard;

use crate::errors::{ServerResult, ServerError};
use crate::locks::traits::ServerOpLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::deletion::DeletionMetadata;
use crate::storage::table::TableMetadata;
use crate::types::SegmentKey;
use crate::utils::common;
use crate::storage::segment::SegmentMetadata;

pub struct DeletionWriteLocks {
  pub table_meta: TableMetadata,
  pub deletion_guard: OwnedRwLockWriteGuard<Option<DeletionMetadata>>,
  pub segment_key: SegmentKey,
}

pub struct DeletionReadLocks {
  pub table_meta: TableMetadata,
  pub maybe_deletion_meta: Option<DeletionMetadata>,
  pub segment_meta: SegmentMetadata,
  pub segment_key: SegmentKey,
}

#[async_trait]
impl ServerOpLocks for DeletionWriteLocks {
  type Key = SegmentKey;

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> {
    let key: SegmentKey = op.get_key()?;
    let table_name = &key.table_name;
    let table_lock = server.table_metadata_cache.get_lock(table_name).await?;
    let table_guard = table_lock.read().await;
    let table_meta = common::unwrap_metadata(table_name, &*table_guard)?;

    key.partition.check_against_schema(&table_meta.schema)?;

    let deletion_lock = server.deletion_metadata_cache.get_lock(&key).await?;
    let deletion_guard = deletion_lock.write_owned().await;

    let locks = DeletionWriteLocks {
      table_meta,
      deletion_guard,
      segment_key: key,
    };

    op.execute_with_locks(server, locks).await
  }
}

#[async_trait]
impl ServerOpLocks for DeletionReadLocks {
  type Key = SegmentKey;

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> {
    let key: SegmentKey = op.get_key()?;
    let table_name = &key.table_name;
    let table_lock = server.table_metadata_cache.get_lock(table_name).await?;
    let table_guard = table_lock.read().await;
    let table_meta = common::unwrap_metadata(table_name, &*table_guard)?;

    key.partition.check_against_schema(&table_meta.schema)?;

    let deletion_lock = server.deletion_metadata_cache.get_lock(&key).await?;
    let deletion_guard = deletion_lock.read().await;

    let segment_meta_lock = server.segment_metadata_cache.get_lock(&key).await?;
    let segment_guard = segment_meta_lock.read().await;
    let maybe_segment_meta = segment_guard.clone();

    if maybe_segment_meta.is_none() {
      return Err(ServerError::does_not_exist("segment", &key.segment_id.to_string()));
    }

    let segment_meta = maybe_segment_meta.unwrap();

    let locks = DeletionReadLocks {
      table_meta,
      maybe_deletion_meta: deletion_guard.clone(),
      segment_meta,
      segment_key: key,
    };

    op.execute_with_locks(server, locks).await
  }
}
