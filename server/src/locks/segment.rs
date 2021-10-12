use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use pancake_db_idl::dml::PartitionField;
use pancake_db_idl::schema::Schema;

use crate::errors::{ServerError, ServerResult};
use crate::locks::traits::{ServerOpLocks, ServerWriteOpLocks};
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::segment::SegmentMetadata;
use crate::storage::staged::StagedMetadata;
use crate::types::{NormalizedPartition, SegmentKey};

#[derive(Clone, Hash)]
pub struct SegmentLockKey {
  pub table_name: String,
  pub partition: Vec<PartitionField>,
  pub segment_id: String,
}

impl Display for SegmentLockKey {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}/{:?} segment {}",
      self.table_name,
      self.partition,
      self.segment_id
    )
  }
}

pub struct SegmentWriteLocks<'a> {
  segment_key: SegmentKey,
  pub schema: Schema,
  pub segment_meta: &'a mut SegmentMetadata,
}

pub struct SegmentReadLocks {
  segment_key: SegmentKey,
  pub schema: Schema,
  pub segment_meta: SegmentMetadata,
}

#[async_trait]
impl<'a> ServerOpLocks for SegmentWriteLocks<'a> {
  type Key = SegmentLockKey;

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> {
    let key: SegmentLockKey = op.get_key();
    let table_name = &key.table_name;
    let schema_lock = server.schema_cache.get_lock(table_name).await?;
    let schema_guard = schema_lock.read().await;
    let maybe_schema = schema_guard.clone();
    if maybe_schema.is_none() {
      return Err(ServerError::does_not_exist("schema", table_name));
    }

    let schema = maybe_schema.unwrap();
    let segment_key = SegmentKey {
      table_name: table_name.to_string(),
      partition: NormalizedPartition::full(&schema, &key.partition)?,
      segment_id: key.segment_id.clone()
    };

    let segment_meta_lock = server.segment_metadata_cache.get_lock(&segment_key).await?;
    let mut segment_guard = segment_meta_lock.write().await;
    let maybe_segment_meta = &mut *segment_guard;
    if maybe_segment_meta.is_none() {
      *maybe_segment_meta = Some(SegmentMetadata::default());
    }

    let locks = SegmentWriteLocks {
      schema,
      segment_meta: maybe_segment_meta.as_mut().unwrap(),
      segment_key,
    };

    op.execute_with_locks(server, locks).await
  }
}

#[async_trait]
impl ServerOpLocks for SegmentReadLocks {
  type Key = SegmentLockKey;

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> {
    let key: SegmentLockKey = op.get_key();
    let table_name = &key.table_name;
    let schema_lock = server.schema_cache.get_lock(table_name).await?;
    let schema_guard = schema_lock.read().await;
    let maybe_schema = schema_guard.clone();
    if maybe_schema.is_none() {
      return Err(ServerError::does_not_exist("schema", table_name));
    }

    let schema = maybe_schema.unwrap();
    let segment_key = SegmentKey {
      table_name: table_name.to_string(),
      partition: NormalizedPartition::full(&schema, &key.partition)?,
      segment_id: key.segment_id.clone()
    };

    let segment_meta_lock = server.segment_metadata_cache.get_lock(&segment_key).await?;
    let mut segment_guard = segment_meta_lock.read().await;
    let maybe_segment_meta = segment_guard.clone();

    if maybe_segment_meta.is_none() {
      return Err(ServerError::does_not_exist("segment", &key.segment_id));
    }

    let segment_meta = maybe_segment_meta.unwrap();
    let locks = SegmentReadLocks {
      schema,
      segment_meta,
      segment_key,
    };

    op.execute_with_locks(server, locks).await
  }
}
