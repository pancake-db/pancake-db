use async_trait::async_trait;
use pancake_db_idl::dml::PartitionField;
use pancake_db_idl::schema::Schema;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};
use uuid::Uuid;

use crate::dirs;
use crate::errors::{ServerError, ServerResult};
use crate::locks::traits::{ServerOpLocks, ServerWriteOpLocks};
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::Metadata;
use crate::storage::partition::PartitionMetadata;
use crate::storage::segment::SegmentMetadata;
use crate::storage::staged::StagedMetadata;
use crate::types::{NormalizedPartition, PartitionKey, SegmentKey};
use crate::utils;

pub struct PartitionLockKey {
  pub table_name: String,
  pub partition: Vec<PartitionField>,
}

pub struct PartitionWriteLocks<'a> {
  pub schema: Schema,
  pub partition_meta: &'a mut PartitionMetadata,
  pub segment_meta: &'a mut SegmentMetadata,
  pub segment_key: SegmentKey,
}

#[async_trait]
impl<'a> ServerOpLocks for PartitionWriteLocks<'a> {
  type Key = PartitionLockKey;

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op
  ) -> ServerResult<Op::Response> {
    let key: PartitionLockKey = op.get_key();
    let table_name = &key.table_name;
    let schema_lock = server.schema_cache.get_lock(table_name).await?;
    let schema_guard: RwLockReadGuard<'_, Option<Schema>> = schema_lock.read().await;
    let maybe_schema = schema_guard.clone();
    if maybe_schema.is_none() {
      return Err(ServerError::does_not_exist("schema", table_name));
    }

    let schema = maybe_schema.unwrap();
    let partition_key = PartitionKey {
      table_name: table_name.to_string(),
      partition: NormalizedPartition::full(&schema, &key.partition)?
    };
    let partition_lock = server.partition_metadata_cache.get_lock(&partition_key)
      .await?;
    let mut partition_guard: RwLockWriteGuard<'_, Option<PartitionMetadata>> = partition_lock.write().await;
    let maybe_partition_meta = &mut *partition_guard;

    if maybe_partition_meta.is_none() {
      let id = Uuid::new_v4().to_string();
      let partition_meta = PartitionMetadata::new(&id);
      utils::create_if_new(&dirs::partition_dir(&server.opts.dir, &partition_key)).await?;
      partition_meta.overwrite(&server.opts.dir, &partition_key).await?;
      *maybe_partition_meta = Some(partition_meta.clone());
    };

    let partition_meta = maybe_partition_meta.as_mut().unwrap();
    let segment_id = &partition_meta.write_segment_id;
    let segment_key = partition_key.segment_key(segment_id.to_string());

    let segment_lock = server.segment_metadata_cache.get_lock(&segment_key).await?;
    let mut segment_guard = segment_lock.write().await;
    let maybe_segment_meta = &mut *segment_guard;
    if maybe_segment_meta.is_none() {
      utils::create_if_new(&dirs::segment_dir(&server.opts.dir, &segment_key)).await?;
      let segment_meta = SegmentMetadata::default();
      *maybe_segment_meta = Some(segment_meta);
    }

    let locks = PartitionWriteLocks {
      schema,
      partition_meta,
      segment_meta: maybe_segment_meta.as_mut().unwrap(),
      segment_key,
    };

    op.execute_with_locks(server, locks).await
  }
}
