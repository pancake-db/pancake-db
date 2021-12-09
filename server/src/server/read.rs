use std::convert::Infallible;

use hyper::body::Bytes;
use pancake_db_core::compression;
use pancake_db_core::encoding;
use pancake_db_idl::dml::{FieldValue, ListSegmentsRequest, ListSegmentsResponse, PartitionField, PartitionFilter, ReadSegmentColumnRequest, ReadSegmentColumnResponse};
use pancake_db_idl::schema::{ColumnMeta, PartitionMeta};
use warp::{Filter, Rejection, Reply};
use warp::http::Response;

use crate::errors::ServerResult;
use crate::ops::list_segments::ListSegmentsOp;
use crate::ops::read_segment_column::ReadSegmentColumnOp;
use crate::ops::traits::ServerOp;
use crate::types::{NormalizedPartition, PartitionKey, SegmentKey};
use crate::utils::{dirs, navigation};
use crate::utils::common;

use super::Server;

const LIST_ROUTE_NAME: &str = "list_segments";
const READ_ROUTE_NAME: &str = "read_segment_column";

impl Server {
  pub async fn read_compact_col(
    &self,
    segment_key: &SegmentKey,
    col: &ColumnMeta,
    read_version: u64,
    codec: &str,
    limit: usize,
  ) -> ServerResult<Vec<FieldValue>> {
    let compaction_key = segment_key.compaction_key(read_version);
    let path = dirs::compact_col_file(&self.opts.dir, &compaction_key, &col.name);
    let bytes = common::read_or_empty(&path).await?;
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
    read_version: u64,
    limit: usize,
  ) -> ServerResult<Vec<FieldValue>> {
    let compaction_key = segment_key.compaction_key(read_version);
    let path = dirs::flush_col_file(&self.opts.dir, &compaction_key, &col.name);
    let bytes = common::read_or_empty(&path).await?;
    if bytes.is_empty() {
      Ok(Vec::new())
    } else {
      let dtype = common::unwrap_dtype(col.dtype)?;
      let decoder = encoding::new_field_value_decoder(dtype, col.nested_list_depth as u8);
      Ok(decoder.decode_limited(&bytes, limit)?)
    }
  }

  pub async fn read_col(
    &self,
    segment_key: &SegmentKey,
    col: &ColumnMeta,
    read_version: u64,
    maybe_compression_params: Option<&String>,
    limit: usize,
  ) -> ServerResult<Vec<FieldValue>> {
    let mut values = Vec::new();
    if let Some(compression_params) = maybe_compression_params {
      values.extend(
        self.read_compact_col(
          segment_key,
          col,
          read_version,
          compression_params,
          limit
        ).await?
      );
    }
    if values.len() < limit {
      values.extend(self.read_flush_col(
        segment_key,
        col,
        read_version,
        limit - values.len()
      ).await?);
    }
    Ok(values)
  }

  async fn list_segments(&self, req: ListSegmentsRequest) -> ServerResult<ListSegmentsResponse> {
    ListSegmentsOp { req }.execute(self).await
  }

  async fn list_segments_from_bytes(&self, body: Bytes) -> ServerResult<ListSegmentsResponse> {
    let req = common::parse_pb::<ListSegmentsRequest>(body)?;
    self.list_segments(req).await
  }

  pub fn list_segments_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::get()
      .and(warp::path(LIST_ROUTE_NAME))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::warp_list_segments)
  }

  async fn warp_list_segments(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    Self::log_request(LIST_ROUTE_NAME, &body);
    common::pancake_result_into_warp(
      server.list_segments_from_bytes(body).await,
      LIST_ROUTE_NAME
    )
  }

  pub fn read_segment_column_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::get()
      .and(warp::path(READ_ROUTE_NAME))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::warp_read_segment_column)
  }

  async fn read_segment_column(&self, req: ReadSegmentColumnRequest) -> ServerResult<ReadSegmentColumnResponse> {
    ReadSegmentColumnOp { req }.execute(self).await
  }

  async fn read_segment_column_from_bytes(&self, body: Bytes) -> ServerResult<ReadSegmentColumnResponse> {
    let req = common::parse_pb::<ReadSegmentColumnRequest>(body)?;
    self.read_segment_column(req).await
  }

  async fn warp_read_segment_column(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    Self::log_request(READ_ROUTE_NAME, &body);
    let pancake_res = server.read_segment_column_from_bytes(body).await;
    if pancake_res.is_err() {
      return common::pancake_result_into_warp(pancake_res, READ_ROUTE_NAME);
    }

    let resp = pancake_res.unwrap();
    let mut resp_meta = resp.clone();
    resp_meta.data = Vec::new();
    let mut resp_bytes = protobuf::json::print_to_string(&resp_meta)
      .unwrap()
      .into_bytes();
    resp_bytes.extend("\n".as_bytes());
    resp_bytes.extend(resp.data);
    log::info!(
      "replying OK to {} request with {} bytes",
      READ_ROUTE_NAME,
      resp_bytes.len()
    );
    Ok(Box::new(
      Response::builder()
        .body(resp_bytes)
        .expect("unable to build response")
    ))
  }

  pub async fn list_partitions(
    &self,
    table_name: &str,
    mut partitioning: Vec<PartitionMeta>,
    filters: &[PartitionFilter],
  ) -> ServerResult<Vec<Vec<PartitionField>>> {
    let mut partitions: Vec<Vec<PartitionField>> = vec![vec![]];
    partitioning.sort_by_key(|meta| meta.name.clone());
    for meta in &partitioning {
      let mut new_partitions: Vec<Vec<PartitionField>> = Vec::new();
      for partition in &partitions {
        let subdir = dirs::partition_dir(
          &self.opts.dir,
          &PartitionKey {
            table_name: table_name.to_string(),
            partition: NormalizedPartition::from_raw_fields(partition)?
          }
        );
        let subpartitions = navigation::list_subpartitions(&subdir, meta)
          .await?;

        for leaf in subpartitions {
          let mut new_partition = partition.clone();
          new_partition.push(leaf);
          if common::satisfies_filters(&new_partition, filters)? {
            new_partitions.push(new_partition);
          }
        }
      }
      partitions = new_partitions;
    }
    Ok(partitions)
  }
}