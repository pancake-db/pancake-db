use async_trait::async_trait;
use pancake_db_idl::schema::Schema;

use crate::errors::{ServerError, ServerResult};
use crate::locks::traits::ServerOpLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::segment::SegmentMetadata;
use crate::types::SegmentKey;
use tokio::sync::OwnedRwLockWriteGuard;

pub struct SegmentWriteLocks {
  pub schema: Schema,
  pub definitely_segment_guard: OwnedRwLockWriteGuard<Option<SegmentMetadata>>,
  pub segment_key: SegmentKey,
}

pub struct SegmentReadLocks {
  pub schema: Schema,
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
    let schema_lock = server.schema_cache.get_lock(table_name).await?;
    let schema_guard = schema_lock.read().await;
    let maybe_schema = schema_guard.clone();
    if maybe_schema.is_none() {
      return Err(ServerError::does_not_exist("schema", table_name));
    }

    let schema = maybe_schema.unwrap();
    key.partition.check_against_schema(&schema)?;

    let segment_meta_lock = server.segment_metadata_cache.get_lock(&key).await?;
    let mut segment_guard = segment_meta_lock.write_owned().await;
    let maybe_segment_meta = &mut *segment_guard;
    if maybe_segment_meta.is_none() {
      *maybe_segment_meta = Some(SegmentMetadata::default());
    }

    let locks = SegmentWriteLocks {
      schema,
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
    let schema_lock = server.schema_cache.get_lock(table_name).await?;
    let schema_guard = schema_lock.read().await;
    let maybe_schema = schema_guard.clone();
    if maybe_schema.is_none() {
      return Err(ServerError::does_not_exist("schema", table_name));
    }

    let schema = maybe_schema.unwrap();
    key.partition.check_against_schema(&schema)?;

    let segment_meta_lock = server.segment_metadata_cache.get_lock(&key).await?;
    let segment_guard = segment_meta_lock.read().await;
    let maybe_segment_meta = segment_guard.clone();

    if maybe_segment_meta.is_none() {
      return Err(ServerError::does_not_exist("segment", &key.segment_id));
    }

    let segment_meta = maybe_segment_meta.unwrap();
    let locks = SegmentReadLocks {
      schema,
      segment_meta,
      segment_key: key,
    };

    op.execute_with_locks(server, locks).await
  }
}
