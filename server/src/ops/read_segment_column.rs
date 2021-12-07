use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use async_trait::async_trait;
use pancake_db_core::encoding;
use pancake_db_idl::dml::{FieldValue, ReadSegmentColumnRequest, ReadSegmentColumnResponse};
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

impl Display for FileType {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}",
      match self {
        FileType::Flush => "F",
        FileType::Compact => "C",
      }
    )
  }
}

#[derive(Clone, Debug)]
struct SegmentColumnContinuation {
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

impl Display for SegmentColumnContinuation {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}/{}/{}",
      self.version,
      self.file_type,
      self.offset,
    )
  }
}

impl TryFrom<String> for SegmentColumnContinuation {
  type Error = ServerError;

  fn try_from(s: String) -> ServerResult<Self> {
    let parts = s
      .split('/')
      .collect::<Vec<&str>>();

    if parts.len() != 3 {
      return Err(ServerError::invalid("invalid continuation token"));
    }

    let version = parts[0].parse::<u64>()
      .map_err(|_| ServerError::invalid("invalid continuation token version"))?;

    let file_type = if parts[1] == "F" {
      FileType::Flush
    } else if parts[1] == "C" {
      FileType::Compact
    } else {
      return Err(ServerError::invalid("invalid continuation token file type"));
    };

    let offset = parts[2].parse::<u64>()
      .map_err(|_| ServerError::invalid("invalid continuation token offset"))?;

    Ok(SegmentColumnContinuation {
      version,
      file_type,
      offset,
    })
  }
}

pub struct ReadSegmentColumnOp {
  pub req: ReadSegmentColumnRequest,
}

#[async_trait]
impl ServerOp<SegmentReadLocks> for ReadSegmentColumnOp {
  type Response = ReadSegmentColumnResponse;

  fn get_key(&self) -> ServerResult<SegmentKey> {
    let partition = NormalizedPartition::from_raw_fields(&self.req.partition)?;
    Ok(SegmentKey {
      table_name: self.req.table_name.clone(),
      partition,
      segment_id: Uuid::from_str(&self.req.segment_id)?,
    })
  }

  async fn execute_with_locks(&self, server: &Server, locks: SegmentReadLocks) -> ServerResult<ReadSegmentColumnResponse> {
    let req = &self.req;
    common::validate_entity_name_for_read("table name", &req.table_name)?;
    common::validate_segment_id(&req.segment_id)?;
    common::validate_entity_name_for_read("column name", &req.column_name)?;

    let SegmentReadLocks {
      table_meta,
      segment_meta,
      segment_key,
    } = locks;
    let col_name = req.column_name.clone();

    let maybe_col_meta = table_meta.schema.columns
      .iter()
      .find(|&meta| meta.name == col_name);
    if maybe_col_meta.is_none() {
      return Err(ServerError::does_not_exist("column", &col_name));
    }
    let col_meta = maybe_col_meta.unwrap();

    let is_explicit_column = segment_meta.explicit_columns.contains(&col_name);
    let continuation = if req.continuation_token.is_empty() {
      // If the segment explicitly contains this column and version > 0,
      // it probably has compacted data.
      // Otherwise it definitely doesn't, and we can skip to flushed+staged data.
      let version = segment_meta.read_version;
      if is_explicit_column && version > 0 {
        SegmentColumnContinuation::new(FileType::Compact, version)
      } else {
        SegmentColumnContinuation::new(FileType::Flush, version)
      }
    } else {
      SegmentColumnContinuation::try_from(req.continuation_token.clone())?
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
    let implicit_nulls_count = if is_explicit_column {
      0
    } else {
      let staged_count = segment_meta.staged_n - segment_meta.staged_deleted_n;
      row_count - staged_count as u32
    };
    let mut response = ReadSegmentColumnResponse {
      row_count,
      implicit_nulls_count,
      ..Default::default()
    };
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

        let next_token = if compressed_data.len() < opts.read_page_byte_size {
          let has_uncompressed_data = common::file_exists(dirs::flush_col_file(
            dir,
            &compaction_key,
            &col_name
          )).await?;
          if has_uncompressed_data {
            SegmentColumnContinuation {
              version: continuation.version,
              file_type: FileType::Flush,
              offset: 0,
            }.to_string()
          } else {
            "".to_string()
          }
        } else {
          SegmentColumnContinuation {
            version: continuation.version,
            file_type: FileType::Compact,
            offset: continuation.offset + compressed_data.len() as u64,
          }.to_string()
        };

        response.codec = codec;
        response.compressed_data = compressed_data;
        response.continuation_token = next_token;
      },
      FileType::Flush => {
        let uncompressed_filename = dirs::flush_col_file(
          dir,
          &compaction_key,
          &col_name,
        );
        response.uncompressed_data = common::read_with_offset(
          uncompressed_filename,
          continuation.offset,
          opts.read_page_byte_size,
        ).await?;

        response.continuation_token = SegmentColumnContinuation {
          version: continuation.version,
          file_type: FileType::Flush,
          offset: continuation.offset + response.uncompressed_data.len() as u64
        }.to_string();
        if response.uncompressed_data.len() < opts.read_page_byte_size {
          // we have reached the end of flushed data
          response.continuation_token = "".to_string();

          // encode staged data on the fly and append it
          let staged_rows_path = dirs::staged_rows_path(dir, &segment_key);
          let staged_bytes = fs::read(&staged_rows_path).await?;
          let staged_rows = common::staged_bytes_to_rows(&staged_bytes)?;
          let staged_values = staged_rows.iter()
            .map(|row| common::single_field_from_row(row, &col_name).value.unwrap_or_default())
            .collect::<Vec<FieldValue>>();
          let encoder = encoding::new_encoder(
            col_meta.dtype.enum_value_or_default(),
            col_meta.nested_list_depth as u8
          );
          let staged_bytes = encoder.encode(&staged_values)?;
          response.uncompressed_data.extend(staged_bytes);
        }
      }
    }
    Ok(response)
  }
}
