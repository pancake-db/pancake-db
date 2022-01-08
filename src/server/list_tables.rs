use std::convert::Infallible;

use hyper::body::Bytes;
use pancake_db_idl::ddl::{ListTablesRequest, ListTablesResponse};
use warp::{Filter, Rejection, Reply};

use crate::errors::ServerResult;

use crate::server::Server;
use crate::utils::common;
use crate::ops::list_tables::ListTablesOp;
use crate::ops::traits::ServerOp;

const ROUTE_NAME: &str = "list_tables";

impl Server {
  pub async fn list_tables(&self, req: ListTablesRequest) -> ServerResult<ListTablesResponse> {
    ListTablesOp { req }.execute(self).await
  }

  pub fn list_tables_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::get()
      .and(warp::path(ROUTE_NAME))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::list_tables_from_body)
  }

  async fn list_tables_from_bytes(&self, body: Bytes) -> ServerResult<ListTablesResponse> {
    let req = common::parse_pb::<ListTablesRequest>(body)?;
    self.list_tables(req).await
  }

  async fn list_tables_from_body(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    Self::log_request(ROUTE_NAME, &body);
    common::pancake_result_into_warp(
      server.list_tables_from_bytes(body).await,
      ROUTE_NAME,
    )
  }
}

