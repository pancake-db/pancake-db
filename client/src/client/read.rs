use hyper::{Body, Method, Request, StatusCode};
use hyper::body::HttpBody;

use crate::errors::{Error, Result};

use super::Client;
use pancake_db_idl::dml::{ListSegmentsRequest, ListSegmentsResponse, ReadSegmentColumnRequest, ReadSegmentColumnResponse};

impl Client {
  pub async fn list_segments(&self, req: &ListSegmentsRequest) -> Result<ListSegmentsResponse> {
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
      return Err(Error::http(status, &content));
    }
    let mut res = ListSegmentsResponse::new();
    protobuf::json::merge_from_str(&mut res, &content)?;
    Ok(res)
  }

  pub async fn read_segment_column(&self, req: &ReadSegmentColumnRequest) -> Result<ReadSegmentColumnResponse> {
    let uri = self.rest_endpoint("read_segment_column");
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
      return Err(Error::http(status, &content));
    }
    let mut res = ReadSegmentColumnResponse::new();
    protobuf::json::merge_from_str(&mut res, &content)?;
    Ok(res)
  }
}
