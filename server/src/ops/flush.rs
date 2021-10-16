use async_trait::async_trait;
use chrono::Utc;
use pancake_db_idl::dml::FieldValue;
use tokio::fs;

use pancake_db_core::encoding;

use crate::utils::dirs;
use crate::errors::ServerResult;
use crate::locks::segment::SegmentWriteLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::Metadata;
use crate::utils::common;
use crate::types::{SegmentKey, CompactionKey};
use crate::storage::segment::SegmentMetadata;
use std::path::PathBuf;
use crate::storage::table::TableMetadata;
use crate::storage::compaction::Compaction;
use crate::utils::decoding_seek;
use tokio::fs::OpenOptions;

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
    common::validate_entity_name_for_read("table name", &self.segment_key.table_name)?;

    let dir = &server.opts.dir;
    let SegmentWriteLocks {
      table_meta,
      mut definitely_segment_guard,
      segment_key,
    } = locks;
    let segment_meta = definitely_segment_guard.as_mut().unwrap();

    let n_rows = segment_meta.staged_n;
    log::debug!(
      "flushing {} rows for segment {}",
      n_rows,
      segment_key,
    );

    let staged_rows_path = dirs::staged_rows_path(dir, &segment_key);
    let staged_bytes = fs::read(&staged_rows_path).await?;
    let rows = common::staged_bytes_to_rows(&staged_bytes)?;

    let mut field_maps = Vec::new();
    for row in &rows {
      field_maps.push(row.field_map());
    }

    // before we do anything destructive, mark this segment as flushing
    // for recovery purposes
    segment_meta.flushing = true;
    segment_meta.overwrite(&dir, &segment_key).await?;

    for version in &segment_meta.write_versions {
      let compaction_key = segment_key.compaction_key(*version);
      for col in &table_meta.schema.columns {
        let field_values = field_maps
          .iter()
          .map(|m| m.get(&col.name).map(|f| f.value.clone().unwrap()).unwrap_or_default())
          .collect::<Vec<FieldValue>>();
        let dtype = common::unwrap_dtype(col.dtype)?;
        let bytes = encoding::new_encoder(dtype, col.nested_list_depth as u8)
          .encode(&field_values)?;
        common::append_to_file(
          &dirs::flush_col_file(dir, &compaction_key, &col.name),
          &bytes,
        ).await?;
      }
    }

    segment_meta.last_flush_at = Utc::now();
    segment_meta.staged_n = 0;
    segment_meta.staged_deleted_n = 0;
    segment_meta.overwrite(&dir, &segment_key).await?;

    log::debug!("truncating staged rows path {:?}", staged_rows_path);
    Self::truncate_staged_rows(staged_rows_path).await?;

    segment_meta.flushing = false;
    segment_meta.overwrite(&dir, &segment_key).await?;

    Ok(())
  }
}

impl FlushOp {
  async fn truncate_staged_rows(staged_rows_path: PathBuf) -> ServerResult<()> {
    fs::OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .open(staged_rows_path)
      .await?;
    Ok(())
  }

  async fn trim_flush_files(
    server: &Server,
    table_meta: &TableMetadata,
    segment_meta: &SegmentMetadata,
    compaction_key: &CompactionKey,
  ) -> ServerResult<()> {
    // make sure there are exactly the right number of fields in each
    // column; if there are more, trim them off
    let dir = &server.opts.dir;
    let compaction = Compaction::load(dir, compaction_key)
      .await?
      .unwrap_or_default();

    let trim_idx = common::flush_only_n(&segment_meta, &compaction);
    for col_meta in &table_meta.schema.columns {
      let flush_file = dirs::flush_col_file(dir, compaction_key, &col_meta.name);
      let bytes = fs::read(&flush_file).await?;
      let trim_byte_idx = decoding_seek::byte_idx_for_row_idx(
        col_meta.dtype.enum_value_or_default(),
        col_meta.nested_list_depth as u8,
        &bytes,
        trim_idx,
      )?;

      if trim_byte_idx != bytes.len() {
        log::debug!("trimming {:?} from {} to {} bytes", flush_file, bytes.len(), trim_byte_idx);
        let file = OpenOptions::new()
          .write(true)
          .open(flush_file)
          .await?;
        file.set_len(trim_byte_idx as u64).await?;
      }
    }
    Ok(())
  }

  pub async fn recover(
    server: &Server,
    table_meta: &TableMetadata,
    segment_key: &SegmentKey,
    segment_meta: &mut SegmentMetadata,
  ) -> ServerResult<()> {
    let dir = &server.opts.dir;
    if !segment_meta.flushing {
      return Ok(())
    }

    if segment_meta.staged_n == 0 {
      log::debug!(
        "identified incomplete flush in segment {}; recovering by truncating staged rows",
        segment_key,
      );
      let staged_rows_path = dirs::staged_rows_path(
        dir,
        segment_key,
      );
      Self::truncate_staged_rows(staged_rows_path).await?;
    } else {
      log::debug!(
        "identified incomplete flush in segment {}; recovering by trimming flush columns",
        segment_key,
      );
      for version in &segment_meta.write_versions {
        let compaction_key = segment_key.compaction_key(*version);
        Self::trim_flush_files(
          server,
          table_meta,
          segment_meta,
          &compaction_key
        ).await?;
      }
    }

    segment_meta.flushing = false;
    segment_meta.overwrite(dir, segment_key).await?;

    Ok(())
  }
}