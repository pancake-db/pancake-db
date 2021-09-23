use std::convert::{Infallible, TryFrom};

use hyper::body::Bytes;
use pancake_db_idl::dml::{FieldValue, ListSegmentsRequest, ListSegmentsResponse, PartitionField, ReadSegmentColumnRequest, ReadSegmentColumnResponse, Segment};
use pancake_db_idl::schema::{ColumnMeta, PartitionMeta};
use tokio::fs;
use warp::{Filter, Rejection, Reply};

use pancake_db_core::compression;
use pancake_db_core::encoding;
use crate::errors::{ServerError, ServerResult};

use crate::dirs;
use crate::storage::flush::FlushMetadata;
use crate::types::{NormalizedPartition, PartitionKey, SegmentKey, CompactionKey};
use crate::utils;

use super::Server;
use warp::http::Response;
use std::fmt::{Display, Formatter};

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

impl Server {
  pub async fn read_compact_col(
    &self,
    segment_key: &SegmentKey,
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    codec: &str,
    limit: usize,
  ) -> ServerResult<Vec<FieldValue>> {
    let compaction_key = segment_key.compaction_key(metadata.read_version);
    let path = dirs::compact_col_file(&self.opts.dir, &compaction_key, &col.name);
    let bytes = utils::read_or_empty(&path).await?;
    if bytes.is_empty() {
      Ok(Vec::new())
    } else {
      let decompressor = compression::new_codec(col.dtype.unwrap(), codec)?;
      let decoded = decompressor.decompress(bytes, col.nested_list_depth as u8)?;
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
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    limit: usize,
  ) -> ServerResult<Vec<FieldValue>> {
    let compaction_key = segment_key.compaction_key(metadata.read_version);
    let path = dirs::flush_col_file(&self.opts.dir, &compaction_key, &col.name);
    let bytes = utils::read_or_empty(&path).await?;
    if bytes.is_empty() {
      Ok(Vec::new())
    } else {
      let dtype = utils::unwrap_dtype(col.dtype)?;
      let decoder = encoding::new_field_value_decoder(dtype, col.nested_list_depth as u8);
      Ok(decoder.decode_limited(&bytes, limit)?)
    }
  }

  pub async fn read_col(
    &self,
    segment_key: &SegmentKey,
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    maybe_compression_params: Option<&String>,
    limit: usize,
  ) -> ServerResult<Vec<FieldValue>> {
    let mut values = Vec::new();
    if let Some(compression_params) = maybe_compression_params {
      values.extend(
        self.read_compact_col(
          segment_key,
          col,
          metadata,
          compression_params,
          limit
        ).await?
      );
    }
    if values.len() < limit {
      values.extend(self.read_flush_col(
        segment_key,
        col,
        metadata,
        limit - values.len()
      ).await?);
    }
    Ok(values)
  }

  async fn list_subpartitions(
    &self,
    table_name: &str,
    parent: &[PartitionField],
    meta: &PartitionMeta,
  ) -> ServerResult<Vec<PartitionField>> {
    let dir = dirs::partition_dir(
      &self.opts.dir,
      &PartitionKey {
        table_name: table_name.to_string(),
        partition: NormalizedPartition::partial(parent)?
      }
    );
    let mut res = Vec::new();
    let mut read_dir = fs::read_dir(&dir).await?;
    while let Ok(Some(entry)) = read_dir.next_entry().await {
      if !entry.file_type().await.unwrap().is_dir() {
        continue;
      }

      let fname = entry.file_name();
      let parts = fname
        .to_str()
        .unwrap()
        .split('=')
        .collect::<Vec<&str>>();

      if parts.len() != 2 {
        continue;
      }
      if *parts[0] != meta.name {
        continue;
      }
      let parsed = utils::partition_field_from_string(
        &meta.name,
        parts[1],
        meta.dtype.unwrap(),
      )?;
      res.push(parsed);
    }
    Ok(res)
  }

  async fn list_segments(&self, req: ListSegmentsRequest) -> ServerResult<ListSegmentsResponse> {
    utils::validate_entity_name_for_read("table name", &req.table_name)?;

    let schema = self.schema_cache
      .get_result(&req.table_name)
      .await?;

    let mut partitions: Vec<Vec<PartitionField>> = vec![vec![]];
    for meta in &schema.partitioning {
      let mut new_partitions: Vec<Vec<PartitionField>> = Vec::new();
      for partition in &partitions {
        let subpartitions = self.list_subpartitions(&req.table_name, partition, meta)
          .await?;

        for leaf in subpartitions {
          let mut new_partition = partition.clone();
          new_partition.push(leaf);
          if utils::satisfies_filters(&new_partition, &req.partition_filter)? {
            new_partitions.push(new_partition);
          }
        }
      }
      partitions = new_partitions;
    }

    let mut segments = Vec::new();
    for partition in &partitions {
      let normalized_partition = NormalizedPartition::partial(partition)?;
      let partition_key = PartitionKey {
        table_name: req.table_name.clone(),
        partition: normalized_partition,
      };
      let segments_meta = self.segments_metadata_cache
        .get_result(&partition_key)
        .await?;
      for segment_id in &segments_meta.segment_ids {
        let count = if req.include_counts {
          self.get_count(partition_key.segment_key(segment_id.clone())).await
        } else {
          0_u32
        };
        segments.push(Segment {
          partition: partition.clone(),
          segment_id: segment_id.clone(),
          count,
          ..Default::default()
        });
      }
    }

    Ok(ListSegmentsResponse {
      segments,
      continuation_token: "".to_string(),
      ..Default::default()
    })
  }

  async fn get_count(&self, segment_key: SegmentKey) -> u32 {
    self.flush_metadata_cache.get(&segment_key)
      .await
      .n as u32
  }

  async fn list_segments_from_bytes(&self, body: Bytes) -> ServerResult<ListSegmentsResponse> {
    let req = utils::parse_pb::<ListSegmentsRequest>(body)?;
    self.list_segments(req).await
  }

  pub fn list_segments_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::get()
      .and(warp::path("list_segments"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::warp_list_segments)
  }

  async fn warp_list_segments(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    utils::pancake_result_into_warp(server.list_segments_from_bytes(body).await)
  }

  pub fn read_segment_column_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::get()
      .and(warp::path("read_segment_column"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::warp_read_segment_column)
  }

  async fn read_segment_column(&self, req: ReadSegmentColumnRequest) -> ServerResult<ReadSegmentColumnResponse> {
    utils::validate_entity_name_for_read("table name", &req.table_name)?;
    utils::validate_segment_id(&req.segment_id)?;
    utils::validate_entity_name_for_read("column name", &req.column_name)?;

    let col_name = req.column_name;
    // TODO fix edge case where data in a new column is written between schema read and flush metadata read
    let schema = self.schema_cache
      .get_result(&req.table_name)
      .await?;

    let mut valid_col = false;
    for col_meta_item in &schema.columns {
      if col_meta_item.name == col_name {
        valid_col = true;
      }
    }

    if !valid_col {
      return Err(ServerError::does_not_exist("column", &col_name));
    }

    let partition = NormalizedPartition::full(&schema, &req.partition)?;
    let segment_key = SegmentKey {
      table_name: req.table_name.clone(),
      partition,
      segment_id: req.segment_id.clone(),
    };
    let flush_meta = self.flush_metadata_cache
      .get(&segment_key)
      .await;

    let continuation = if req.continuation_token.is_empty() {
      SegmentColumnContinuation::new(flush_meta.read_version)
    } else {
      SegmentColumnContinuation::try_from(req.continuation_token.clone())?
    };

    let compaction_key = segment_key.compaction_key(continuation.version);
    let compaction = self.compaction_cache
      .get(compaction_key.clone())
      .await;

    let mut response = ReadSegmentColumnResponse {
      row_count: flush_meta.n as u32,
      ..Default::default()
    };
    match continuation.file_type {
      FileType::Compact => {
        let compressed_filename = dirs::compact_col_file(
          &self.opts.dir,
          &compaction_key,
          &col_name,
        );
        let codec = compaction.col_codecs
          .get(&col_name)
          .map(|c| c.clone() as String)
          .unwrap_or_default();

        let compressed_data = utils::read_with_offset(
          compressed_filename,
          continuation.offset,
          self.opts.read_page_byte_size,
        ).await?;

        let next_token = if compressed_data.len() < self.opts.read_page_byte_size {
          if self.has_uncompressed_data(&compaction_key, &col_name).await? {
            SegmentColumnContinuation {
              version: continuation.version,
              file_type: FileType::Flush,
              offset: 0
            }.to_string()
          } else {
            "".to_string()
          }
        } else {
          SegmentColumnContinuation {
            version: continuation.version,
            file_type: FileType::Compact,
            offset: continuation.offset + compressed_data.len() as u64
          }.to_string()
        };

        response.codec = codec;
        response.compressed_data = compressed_data;
        response.continuation_token = next_token;
      },
      FileType::Flush => {
        let uncompressed_filename = dirs::flush_col_file(
          &self.opts.dir,
          &compaction_key,
          &col_name,
        );
        let uncompressed_data = utils::read_with_offset(
          uncompressed_filename,
          continuation.offset,
          self.opts.read_page_byte_size,
        ).await?;
        let next_token = if uncompressed_data.len() < self.opts.read_page_byte_size {
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

  async fn has_uncompressed_data(&self, compaction_key: &CompactionKey, column_name: &str) -> ServerResult<bool> {
    Ok(utils::file_exists(dirs::flush_col_file(&self.opts.dir, compaction_key, column_name)).await?)
  }

  async fn read_segment_column_from_bytes(&self, body: Bytes) -> ServerResult<ReadSegmentColumnResponse> {
    let req = utils::parse_pb::<ReadSegmentColumnRequest>(body)?;
    self.read_segment_column(req).await
  }

  async fn warp_read_segment_column(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    let pancake_res = server.read_segment_column_from_bytes(body).await;
    if pancake_res.is_err() {
      return utils::pancake_result_into_warp(pancake_res);
    }

    let resp = pancake_res.unwrap();
    let mut resp_meta = resp.clone();
    resp_meta.uncompressed_data = Vec::new();
    resp_meta.compressed_data = Vec::new();
    let mut resp_bytes = protobuf::json::print_to_string(&resp_meta)
      .unwrap()
      .into_bytes();
    resp_bytes.extend("\n".as_bytes());
    if !resp.uncompressed_data.is_empty() {
      resp_bytes.extend(resp.uncompressed_data)
    } else {
      resp_bytes.extend(resp.compressed_data)
    }
    Ok(Box::new(
      Response::builder()
        .body(resp_bytes)
        .expect("unable to build response")
    ))
  }
}