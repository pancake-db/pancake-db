use async_trait::async_trait;
use chrono::Utc;
use pancake_db_idl::dml::FieldValue;
use tokio::fs;

use pancake_db_core::encoding;

use crate::dirs;
use crate::errors::ServerResult;
use crate::locks::segment::SegmentWriteLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::Metadata;
use crate::utils;
use crate::types::SegmentKey;

pub struct FlushOp {
  pub segment_key: SegmentKey,
}

#[async_trait]
impl ServerOp<SegmentWriteLocks> for FlushOp {
  type Response = ();

  fn get_key(&self) -> ServerResult<SegmentKey> {
    Ok(self.segment_key.clone())
  }

  async fn execute_with_locks(&self, server: &Server, locks: SegmentWriteLocks) -> ServerResult<()> {
    utils::validate_entity_name_for_read("table name", &self.segment_key.table_name)?;

    let dir = &server.opts.dir;
    let SegmentWriteLocks {
      schema,
      mut definitely_segment_guard,
      segment_key,
    } = locks;
    let segment_meta = definitely_segment_guard.as_mut().unwrap();

    let n_rows = segment_meta.currently_staged_n;
    log::debug!(
      "flushing {} rows for segment {}",
      n_rows,
      segment_key,
    );

    let staged_rows_path = dirs::staged_rows_path(dir, &segment_key);
    let staged_bytes = fs::read(&staged_rows_path).await?;
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
        let bytes = encoding::new_encoder(dtype, col.nested_list_depth as u8)
          .encode(&field_values)?;
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

    log::debug!("truncating staged rows path {:?}", staged_rows_path);
    fs::OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .open(staged_rows_path)
      .await?;

    Ok(())
  }
}