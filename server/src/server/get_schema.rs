use std::convert::Infallible;

use hyper::body::Bytes;
use pancake_db_idl::ddl::{GetSchemaRequest, GetSchemaResponse};
use protobuf::MessageField;
use warp::{Filter, Rejection, Reply};

use crate::errors::ServerResult;

use crate::server::Server;
use crate::utils;

impl Server {
  pub async fn get_schema(&self, req: GetSchemaRequest) -> ServerResult<GetSchemaResponse> {
    self.schema_cache.get_or_err(&req.table_name)
      .await
      .map(|schema| GetSchemaResponse {
        schema: MessageField::some(schema),
        ..Default::default()
      })
  }

  pub fn get_schema_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::get()
      .and(warp::path("get_schema"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::get_schema_from_body)
  }

  async fn get_schema_from_bytes(&self, body: Bytes) -> ServerResult<GetSchemaResponse> {
    let req = utils::parse_pb::<GetSchemaRequest>(body)?;
    self.get_schema(req).await
  }

  async fn get_schema_from_body(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    utils::pancake_result_into_warp(server.get_schema_from_bytes(body).await)
  }
}

