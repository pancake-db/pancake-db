use std::str::FromStr;

use async_trait::async_trait;
use pancake_db_core::encoding;
use pancake_db_idl::dml::{FieldValue, ReadSegmentColumnRequest, ReadSegmentColumnResponse};
use pancake_db_idl::dtype::DataType;
use tokio::fs;
use uuid::Uuid;

use crate::errors::{ServerError, ServerResult};
use crate::locks::segment::SegmentReadLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::types::{NormalizedPartition, SegmentKey};
use crate::utils::common;
use crate::utils::dirs;

#[derive(Clone, Copy, Debug)]
enum FileType {
  Flush,
  Compact,
}

#[derive(Clone, Debug)]
pub struct SegmentColumnContinuation {
  version: u64,
  file_type: FileType,
  offset: u64,
}

impl SegmentColumnContinuation {
  fn new(file_type: FileType, version: u64) -> Self {
    SegmentColumnContinuation {
      version,
      file_type,
      offset: 0,
    }
  }
}

pub struct ContinuedReadSegmentColumnResponse {
  pub resp: ReadSegmentColumnResponse,
  pub continuation: Option<SegmentColumnContinuation>,
}

pub struct ReadSegmentColumnOp {
  pub req: ReadSegmentColumnRequest,
  pub continuation: Option<SegmentColumnContinuation>,
}

#[async_trait]
impl ServerOp for ReadSegmentColumnOp {
  type Locks = SegmentReadLocks;
  type Response = ContinuedReadSegmentColumnResponse;

  fn get_key(&self) -> ServerResult<SegmentKey> {
    let partition = NormalizedPartition::from_raw_fields(&self.req.partition)?;
    Ok(SegmentKey {
      table_name: self.req.table_name.clone(),
      partition,
      segment_id: Uuid::from_str(&self.req.segment_id)?,
    })
  }

  async fn execute_with_locks(&self, server: &Server, locks: SegmentReadLocks) -> ServerResult<Self::Response> {
    let req = &self.req;
    common::validate_entity_name_for_read("table name", &req.table_name)?;
    common::validate_segment_id(&req.segment_id)?;
    common::validate_entity_name_for_read("column name", &req.column_name)?;
    if self.req.correlation_id.is_empty() && self.continuation.is_none() {
      return Err(ServerError::invalid("must provide either a correlation id or a continuation token"))
    }

    let SegmentReadLocks {
      table_meta,
      segment_meta,
      segment_key,
    } = locks;
    let col_name = req.column_name.clone();

    let augmented_columns = common::augmented_columns(&table_meta.schema());
    let maybe_col_meta = augmented_columns
      .get(&col_name);
    if maybe_col_meta.is_none() {
      return Err(ServerError::does_not_exist("column", &col_name));
    }
    let col_meta = maybe_col_meta.unwrap();

    let is_explicit_column = segment_meta.explicit_columns.contains(&col_name);
    let continuation = if self.continuation.is_none() {
      let version = server.correlation_metadata_cache.get_correlated_read_version(
        &req.correlation_id,
        &segment_key,
        segment_meta.read_version
      ).await?;

      // If the segment explicitly contains this column and version > 0,
      // it probably has compacted data.
      // Otherwise it definitely doesn't, and we can skip to flushed+staged data.
      if is_explicit_column && version > 0 {
        SegmentColumnContinuation::new(FileType::Compact, version)
      } else {
        SegmentColumnContinuation::new(FileType::Flush, version)
      }
    } else {
      self.continuation.clone().unwrap()
    };

    let compaction_key = segment_key.compaction_key(continuation.version);
    // compaction metadata does not change after writing so it is safe to clone it and drop the lock
    let compaction_lock = server.compaction_cache
      .get_lock(&compaction_key)
      .await?;
    let compaction = compaction_lock.read()
      .await
      .clone()
      .unwrap_or_default();

    let opts = &server.opts;
    let dir = &opts.dir;
    let row_count = (segment_meta.all_time_n - segment_meta.all_time_deleted_n) as u32;
    let deletion_count = segment_meta.all_time_deleted_n - compaction.all_time_omitted_n;
    let implicit_nulls_count = if is_explicit_column {
      0
    } else {
      segment_meta.all_time_n - segment_meta.staged_n as u32
    };
    let mut resp = ReadSegmentColumnResponse {
      row_count,
      deletion_count,
      implicit_nulls_count,
      ..Default::default()
    };
    let mut new_continuation = None;
    match continuation.file_type {
      FileType::Compact => {
        let compressed_filename = dirs::compact_col_file(
          dir,
          &compaction_key,
          &col_name,
        );
        let codec = compaction.col_codecs
          .get(&col_name)
          .map(|c| c.clone() as String)
          .unwrap_or_default();

        let compressed_data = common::read_with_offset(
          compressed_filename,
          continuation.offset,
          opts.read_page_byte_size,
        ).await?;

        if compressed_data.len() < opts.read_page_byte_size {
          let has_flushed_data_future = common::file_exists(dirs::flush_col_file(
            dir,
            &compaction_key,
            &col_name
          ));
          let has_staged_data_future = common::file_nonempty(dirs::staged_rows_path(
            dir,
            &segment_key,
          ));
          let (has_flushed_data, has_staged_data) = tokio::join!(has_flushed_data_future, has_staged_data_future);
          if has_flushed_data? || has_staged_data? {
            new_continuation = Some(SegmentColumnContinuation {
              version: continuation.version,
              file_type: FileType::Flush,
              offset: 0,
            });
          }
        } else {
          new_continuation = Some(SegmentColumnContinuation {
            version: continuation.version,
            file_type: FileType::Compact,
            offset: continuation.offset + compressed_data.len() as u64,
          });
        };

        resp.codec = codec;
        resp.data = compressed_data;
      },
      FileType::Flush => {
        let uncompressed_filename = dirs::flush_col_file(
          dir,
          &compaction_key,
          &col_name,
        );
        resp.data = common::read_with_offset(
          uncompressed_filename,
          continuation.offset,
          opts.read_page_byte_size,
        ).await?;

        if resp.data.len() < opts.read_page_byte_size {
          // we have reached the end of flushed data
          // encode staged data on the fly and append it
          let staged_rows_path = dirs::staged_rows_path(dir, &segment_key);
          let staged_bytes = fs::read(&staged_rows_path).await?;
          let staged_rows = common::staged_bytes_to_rows(&staged_bytes)?;
          let staged_values = staged_rows.iter()
            .map(|row| row.fields.get(&col_name).cloned().unwrap_or_default())
            .collect::<Vec<FieldValue>>();
          let encoder = encoding::new_encoder(
            DataType::from_i32(col_meta.dtype).ok_or(ServerError::internal("unknown dtype"))?,
            col_meta.nested_list_depth as u8
          );
          let staged_bytes = encoder.encode(&staged_values)?;
          resp.data.extend(staged_bytes);
        } else {
          new_continuation = Some(SegmentColumnContinuation {
            version: continuation.version,
            file_type: FileType::Flush,
            offset: continuation.offset + resp.data.len() as u64
          });
        }
      }
    }

    Ok(ContinuedReadSegmentColumnResponse {
      resp,
      continuation: new_continuation,
    })
  }
}
