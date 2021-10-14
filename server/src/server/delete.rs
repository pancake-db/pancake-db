use std::convert::Infallible;

use hyper::body::Bytes;
use pancake_db_idl::ddl::{DropTableRequest, DropTableResponse};
use warp::{Filter, Rejection, Reply};

use crate::errors::ServerResult;
use crate::ops::drop_table::DropTableOp;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::utils;

impl Server {
  pub async fn drop_table(&self, req: DropTableRequest) -> ServerResult<DropTableResponse> {
    DropTableOp { req }.execute(&self).await
  }

  pub fn drop_table_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path("drop_table"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::drop_table_from_body)
  }

  async fn drop_table_from_bytes(&self, body: Bytes) -> ServerResult<DropTableResponse> {
    let req = utils::parse_pb::<DropTableRequest>(body)?;
    self.drop_table(req).await
  }

  async fn drop_table_from_body(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    utils::pancake_result_into_warp(server.drop_table_from_bytes(body).await)
  }
}