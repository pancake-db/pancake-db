use std::io::ErrorKind;
use std::path::Path;

use pancake_db_core::{compression, deletion, encoding};
use pancake_db_idl::dml::{FieldValue, ListSegmentsRequest, ListSegmentsResponse, ReadSegmentColumnRequest, ReadSegmentColumnResponse, ReadSegmentDeletionsRequest, ReadSegmentDeletionsResponse};
use pancake_db_idl::schema::ColumnMeta;
use tokio::fs;

use crate::errors::ServerResult;
use crate::ops::list_segments::ListSegmentsOp;
use crate::ops::read_segment_column::ReadSegmentColumnOp;
use crate::ops::read_segment_deletions::ReadSegmentDeletionsOp;
use crate::ops::traits::ServerOp;
use crate::types::{CompactionKey, SegmentKey};
use crate::utils::common;
use crate::utils::dirs;

use super::Server;

impl Server {
  pub async fn read_compact_col(
    &self,
    segment_key: &SegmentKey,
    col_name: &str,
    col_meta: &ColumnMeta,
    read_version: u64,
    codec: &str,
    limit: usize,
  ) -> ServerResult<Vec<FieldValue>> {
    let compaction_key = segment_key.compaction_key(read_version);
    let path = dirs::compact_col_file(&self.opts.dir, &compaction_key, col_name);
    let bytes = common::read_or_empty(&path).await?;
    if bytes.is_empty() {
      Ok(Vec::new())
    } else {
      let decompressor = compression::new_codec(
        common::unwrap_dtype(col_meta.dtype)?,
        codec,
      )?;
      let decoded = decompressor.decompress(&bytes, col_meta.nested_list_depth as u8)?;
      let limited= if limit < decoded.len() {
        Vec::from(&decoded[0..limit])
      } else {
        decoded
      };
      Ok(limited)
    }
  }

  pub async fn read_flush_col(
    &self,
    segment_key: &SegmentKey,
    col_name: &str,
    col_meta: &ColumnMeta,
    read_version: u64,
    limit: usize,
  ) -> ServerResult<Vec<FieldValue>> {
    let compaction_key = segment_key.compaction_key(read_version);
    let path = dirs::flush_col_file(&self.opts.dir, &compaction_key, col_name);
    let bytes = common::read_or_empty(&path).await?;
    if bytes.is_empty() {
      Ok(Vec::new())
    } else {
      let dtype = common::unwrap_dtype(col_meta.dtype)?;
      let decoder = encoding::new_field_value_decoder(dtype, col_meta.nested_list_depth as u8);
      Ok(decoder.decode_limited(&bytes, limit)?)
    }
  }

  // Includes only compacted and flushed data, omitting staged rows
  pub async fn read_col(
    &self,
    segment_key: &SegmentKey,
    col_name: &str,
    col_meta: &ColumnMeta,
    read_version: u64,
    maybe_compression_params: Option<&String>,
    limit: usize,
  ) -> ServerResult<Vec<FieldValue>> {
    let mut values = Vec::new();
    if let Some(compression_params) = maybe_compression_params {
      values.extend(
        self.read_compact_col(
          segment_key,
          col_name,
          col_meta,
          read_version,
          compression_params,
          limit
        ).await?
      );
    }
    if values.len() < limit {
      values.extend(self.read_flush_col(
        segment_key,
        col_name,
        col_meta,
        read_version,
        limit - values.len()
      ).await?);
    }
    Ok(values)
  }

  pub async fn read_pre_compaction_deletions(
    &self,
    compaction_key: &CompactionKey,
  ) -> ServerResult<Vec<bool>> {
    self.read_deletions(
      &dirs::pre_compaction_deletions_path(
        &self.opts.dir,
        compaction_key
      ),
    ).await
  }

  pub async fn read_post_compaction_deletions(
    &self,
    compaction_key: &CompactionKey,
    deletion_id: u64,
  ) -> ServerResult<Vec<bool>> {
    self.read_deletions(
      &dirs::post_compaction_deletions_path(
        &self.opts.dir,
        compaction_key,
        deletion_id,
      ),
    ).await
  }

  async fn read_deletions(
    &self,
    path: &Path,
  ) -> ServerResult<Vec<bool>> {
    match fs::read(path).await {
      Ok(bytes) => {
        Ok(deletion::decompress_deletions(&bytes)?)
      },
      Err(e) if matches!(e.kind(), ErrorKind::NotFound) => {
        Ok(Vec::new())
      },
      Err(e) => Err(e.into()),
    }
  }

  async fn list_segments(&self, req: ListSegmentsRequest) -> ServerResult<ListSegmentsResponse> {
    ListSegmentsOp { req }.execute(self).await
  }

  async fn read_segment_deletions(&self, req: ReadSegmentDeletionsRequest) -> ServerResult<ReadSegmentDeletionsResponse> {
    ReadSegmentDeletionsOp { req }.execute(self).await
  }

  async fn read_segment_column(&self, req: ReadSegmentColumnRequest) -> ServerResult<ReadSegmentColumnResponse> {
    ReadSegmentColumnOp { req }.execute(self).await
  }
}