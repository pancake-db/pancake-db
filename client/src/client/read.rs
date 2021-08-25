use hyper::{Body, Method, Request, StatusCode};
use hyper::body::HttpBody;

use crate::errors::{ClientError, ClientResult};

use super::Client;
use pancake_db_idl::dml::{ListSegmentsRequest, ListSegmentsResponse, ReadSegmentColumnRequest, ReadSegmentColumnResponse};

fn parse_read_segment_response(bytes: Vec<u8>) -> ClientResult<ReadSegmentColumnResponse> {
  let delim_bytes = "}\n".as_bytes();
  let mut i = 0;
  loop {
    let end_idx = i + delim_bytes.len();
    if end_idx > bytes.len() {
      return Err(ClientError::other(format!("could not parse read segment column response")));
    }
    if &bytes[i..end_idx] == delim_bytes {
      break;
    }
    i += 1;
  }
  let content_str = String::from_utf8(bytes[0..i + 1].to_vec())?;
  let mut res = ReadSegmentColumnResponse::new();
  protobuf::json::merge_from_str(&mut res, &content_str)?;
  let rest = bytes[i + delim_bytes.len()..].to_vec();
  if res.codec.is_empty() {
    res.uncompressed_data = rest;
  } else {
    res.compressed_data = rest;
  }
  Ok(res)

}

impl Client {
  pub async fn list_segments(&self, req: &ListSegmentsRequest) -> ClientResult<ListSegmentsResponse> {
    let uri = self.rest_endpoint("list_segments");
    let pb_str = protobuf::json::print_to_string(req)?;

    let http_req = Request::builder()
      .method(Method::GET)
      .uri(&uri)
      .header("Content-Type", "application/json")
      .body(Body::from(pb_str))?;
    let mut resp = self.h_client.request(http_req).await?;
    let status = resp.status();
    let mut content = String::new();
    while let Some(chunk) = resp.body_mut().data().await {
      content.push_str(&String::from_utf8(chunk?.to_vec())?);
    }

    if status != StatusCode::OK {
      return Err(ClientError::http(status, &content));
    }
    let mut res = ListSegmentsResponse::new();
    protobuf::json::merge_from_str(&mut res, &content)?;
    Ok(res)
  }

  pub async fn read_segment_column(&self, req: &ReadSegmentColumnRequest) -> ClientResult<ReadSegmentColumnResponse> {
    let uri = self.rest_endpoint("read_segment_column");
    let pb_str = protobuf::json::print_to_string(req)?;

    let http_req = Request::builder()
      .method(Method::GET)
      .uri(&uri)
      .header("Content-Type", "application/json")
      .body(Body::from(pb_str))?;
    let mut resp = self.h_client.request(http_req).await?;
    let status = resp.status();
    let mut content = Vec::new();
    while let Some(chunk) = resp.body_mut().data().await {
      content.extend(chunk?);
    }

    if status != StatusCode::OK {
      let content_str = String::from_utf8(content).unwrap_or("<unparseable bytes>".to_string());
      return Err(ClientError::http(status, &content_str));
    }

    parse_read_segment_response(content)
  }
}
