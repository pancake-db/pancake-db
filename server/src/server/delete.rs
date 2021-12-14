use std::convert::Infallible;

use hyper::body::Bytes;
use pancake_db_idl::ddl::{DropTableRequest, DropTableResponse};
use pancake_db_idl::dml::{DeleteFromSegmentRequest, DeleteFromSegmentResponse};
use warp::{Filter, Rejection, Reply};

use crate::errors::ServerResult;
use crate::ops::delete_from_segment::DeleteFromSegmentOp;
use crate::ops::drop_table::DropTableOp;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::utils::common;

const DROP_ROUTE_NAME: &str = "drop_table";
const DELETE_FROM_SEGMENT_ROUTE_NAME: &str = "delete_from_segment";

impl Server {
  pub async fn delete_from_segment(&self, req: DeleteFromSegmentRequest) -> ServerResult<DeleteFromSegmentResponse> {
    DeleteFromSegmentOp { req }.execute(self).await
  }

  pub async fn drop_table(&self, req: DropTableRequest) -> ServerResult<DropTableResponse> {
    DropTableOp { req }.execute(self).await
  }

  async fn delete_from_segment_from_bytes(&self, body: Bytes) -> ServerResult<DeleteFromSegmentResponse> {
    let req = common::parse_pb::<DeleteFromSegmentRequest>(body)?;
    self.delete_from_segment(req).await
  }

  async fn drop_table_from_bytes(&self, body: Bytes) -> ServerResult<DropTableResponse> {
    let req = common::parse_pb::<DropTableRequest>(body)?;
    self.drop_table(req).await
  }

  async fn delete_from_segment_from_body(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    Self::log_request(DELETE_FROM_SEGMENT_ROUTE_NAME, &body);
    common::pancake_result_into_warp(
      server.delete_from_segment_from_bytes(body).await,
      DROP_ROUTE_NAME,
    )
  }

  async fn drop_table_from_body(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    Self::log_request(DROP_ROUTE_NAME, &body);
    common::pancake_result_into_warp(
      server.drop_table_from_bytes(body).await,
      DROP_ROUTE_NAME,
    )
  }

  pub fn delete_from_segment_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path(DELETE_FROM_SEGMENT_ROUTE_NAME))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::delete_from_segment_from_body)
  }

  pub fn drop_table_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path(DROP_ROUTE_NAME))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::drop_table_from_body)
  }
}