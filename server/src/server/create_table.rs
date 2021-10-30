use std::convert::Infallible;


use hyper::body::Bytes;
use pancake_db_idl::ddl::{CreateTableRequest, CreateTableResponse};
use warp::{Filter, Rejection, Reply};

use crate::errors::ServerResult;
use crate::ops::create_table::CreateTableOp;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::utils::common;

const ROUTE_NAME: &str = "create_table";

impl Server {
  pub async fn create_table(&self, req: CreateTableRequest) -> ServerResult<CreateTableResponse> {
    CreateTableOp { req }.execute(self).await
  }

  pub fn create_table_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path(ROUTE_NAME))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::create_table_from_body)
  }

  async fn create_table_from_bytes(&self, body: Bytes) -> ServerResult<CreateTableResponse> {
    let req = common::parse_pb::<CreateTableRequest>(body)?;
    self.create_table(req).await
  }

  async fn create_table_from_body(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    Self::log_request(ROUTE_NAME, &body);
    common::pancake_result_into_warp(
      server.create_table_from_bytes(body).await,
      ROUTE_NAME,
    )
  }
}

