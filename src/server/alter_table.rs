use std::convert::Infallible;

use hyper::body::Bytes;
use pancake_db_idl::ddl::{AlterTableRequest, AlterTableResponse};
use warp::{Filter, Rejection, Reply};

use crate::errors::ServerResult;
use crate::ops::alter_table::AlterTableOp;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::utils::common;

const ROUTE_NAME: &str = "alter_table";

impl Server {
  pub async fn alter_table(&self, req: AlterTableRequest) -> ServerResult<AlterTableResponse> {
    AlterTableOp { req }.execute(self).await
  }

  pub fn alter_table_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path(ROUTE_NAME))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::alter_table_from_body)
  }

  async fn alter_table_from_bytes(&self, body: Bytes) -> ServerResult<AlterTableResponse> {
    let req = common::parse_pb::<AlterTableRequest>(body)?;
    self.alter_table(req).await
  }

  async fn alter_table_from_body(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    Self::log_request(ROUTE_NAME, &body);
    common::pancake_result_into_warp(
      server.alter_table_from_bytes(body).await,
      ROUTE_NAME,
    )
  }
}

