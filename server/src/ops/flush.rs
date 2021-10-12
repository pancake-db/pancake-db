use async_trait::async_trait;
use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse, Row, FieldValue};
use pancake_db_idl::schema::Schema;
use protobuf::MessageField;
use uuid::Uuid;

use crate::dirs;
use crate::errors::{ServerError, ServerResult};
use crate::locks::partition::{PartitionLockKey, PartitionReadLocks, PartitionWriteLocks};
use crate::locks::segment::{SegmentWriteLocks, SegmentLockKey};
use crate::locks::table::TableReadLocks;
use crate::ops::traits::ServerOp;
use crate::opt::Opt;
use crate::server::Server;
use crate::storage::Metadata;
use crate::storage::partition::PartitionMetadata;
use crate::types::SegmentKey;
use crate::utils;
use chrono::Utc;
use tokio::fs;
use pancake_db_core::encoding;

pub struct FlushOp {
  segment_lock_key: SegmentLockKey,
}

#[async_trait]
impl<'a> ServerOp<SegmentWriteLocks<'a>> for FlushOp {
  type Response = ();

  fn get_key(&self) -> SegmentLockKey {
    self.segment_lock_key.clone()
  }

  async fn execute_with_locks(&self, server: &Server, locks: SegmentWriteLocks<'a>) -> ServerResult<()> {
    utils::validate_entity_name_for_read("table name", &self.segment_lock_key.table_name)?;

    let dir = &server.opts.dir;
    let SegmentWriteLocks {
      schema: _s,
      segment_meta,
      segment_key,
    } = locks;

    let n_rows = segment_meta.currently_staged_n;
    log::debug!(
      "flushing {} rows for segment {}",
      n_rows,
      segment_key,
    );

    let staged_bytes = fs::read(dirs::staged_rows_path(dir, &segment_key)).await?;
    let rows = utils::staged_bytes_to_rows(&staged_bytes)?;

    let mut field_maps = Vec::new();
    for row in &rows {
      field_maps.push(row.field_map());
    }

    for version in &segment_meta.write_versions {
      let compaction_key = segment_key.compaction_key(*version);
      for col in &schema.columns {
        let field_values = field_maps
          .iter()
          .map(|m| m.get(&col.name).map(|f| f.value.clone().unwrap()).unwrap_or_default())
          .collect::<Vec<FieldValue>>();
        let dtype = utils::unwrap_dtype(col.dtype)?;
        let encoder = encoding::new_encoder(dtype, col.nested_list_depth as u8);
        let bytes = encoder.encode(&field_values)?;
        utils::append_to_file(
          &dirs::flush_col_file(dir, &compaction_key, &col.name),
          &bytes,
        ).await?;
      }
    }

    segment_meta.last_flush_at = Utc::now();
    segment_meta.currently_staged_n = 0;
    segment_meta.current_staged_n_deleted = 0;
    segment_meta.overwrite(&dir, &segment_key).await?;

    Ok(())
  }
}