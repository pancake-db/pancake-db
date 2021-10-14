use std::convert::Infallible;

use hyper::body::Bytes;
use pancake_db_idl::dml::{FieldValue, ListSegmentsRequest, ListSegmentsResponse, ReadSegmentColumnRequest, ReadSegmentColumnResponse};
use pancake_db_idl::schema::ColumnMeta;
use warp::{Filter, Rejection, Reply};
use warp::http::Response;

use pancake_db_core::compression;
use pancake_db_core::encoding;

use crate::dirs;
use crate::errors::ServerResult;
use crate::ops::list_segments::ListSegmentsOp;
use crate::ops::read_segment_column::ReadSegmentColumnOp;
use crate::ops::traits::ServerOp;
use crate::types::SegmentKey;
use crate::utils;

use super::Server;

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
    read_version: u64,
    limit: usize,
  ) -> ServerResult<Vec<FieldValue>> {
    let compaction_key = segment_key.compaction_key(read_version);
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
    ListSegmentsOp { req }.execute(&self).await
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
    ReadSegmentColumnOp { req }.execute(&self).await
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