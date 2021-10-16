use pancake_db_idl::dml::{ReadSegmentColumnRequest, ReadSegmentColumnResponse};
use crate::ops::traits::ServerOp;
use crate::locks::segment::SegmentReadLocks;

use crate::server::Server;
use crate::errors::{ServerResult, ServerError};
use crate::utils::common;
use crate::utils::dirs;
use std::fmt::{Display, Formatter};
use std::convert::TryFrom;
use async_trait::async_trait;
use crate::types::{NormalizedPartition, SegmentKey};

#[derive(Clone, Debug)]
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
  fn new(version: u64) -> Self {
    SegmentColumnContinuation {
      version,
      file_type: if version > 0 { FileType::Compact } else { FileType::Flush },
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
      segment_id: self.req.segment_id.clone(),
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

    let mut valid_col = false;
    for col_meta_item in &table_meta.schema.columns {
      if col_meta_item.name == col_name {
        valid_col = true;
      }
    }

    if !valid_col {
      return Err(ServerError::does_not_exist("column", &col_name));
    }

    let continuation = if req.continuation_token.is_empty() {
      SegmentColumnContinuation::new(segment_meta.read_version)
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
    let row_count = (segment_meta.all_time_n - segment_meta.all_time_deleted_n) as u32;
    let mut response = ReadSegmentColumnResponse {
      row_count,
      ..Default::default()
    };
    match continuation.file_type {
      FileType::Compact => {
        let compressed_filename = dirs::compact_col_file(
          &opts.dir,
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
            &opts.dir,
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
          &opts.dir,
          &compaction_key,
          &col_name,
        );
        let uncompressed_data = common::read_with_offset(
          uncompressed_filename,
          continuation.offset,
          opts.read_page_byte_size,
        ).await?;
        let next_token = if uncompressed_data.len() < opts.read_page_byte_size {
          "".to_string()
        } else {
          SegmentColumnContinuation {
            version: continuation.version,
            file_type: FileType::Flush,
            offset: continuation.offset + uncompressed_data.len() as u64
          }.to_string()
        };

        response.uncompressed_data = uncompressed_data;
        response.continuation_token = next_token;
      }
    }
    Ok(response)
  }
}
