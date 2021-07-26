use std::convert::Infallible;

use pancake_db_core::errors::{PancakeError, PancakeResult};
use pancake_db_idl::ddl::{CreateTableRequest, CreateTableResponse};
use warp::{Filter, Rejection, Reply};

use crate::dirs;
use crate::server::Server;
use crate::utils;
use hyper::body::Bytes;

impl Server {
  pub async fn create_table(&self, req: CreateTableRequest) -> PancakeResult<CreateTableResponse> {
    let schema = match &req.schema.0 {
      Some(s) => Ok(s),
      None => Err(PancakeError::invalid("missing table schema")),
    }?.as_ref();

    utils::create_if_new(&self.dir).await?;

    let table_dir = dirs::table_dir(&self.dir, &req.table_name);
    utils::create_if_new(&table_dir).await?;

    self.schema_cache.assert(&req.table_name, schema).await?;
    Ok(CreateTableResponse {..Default::default()})
  }

  pub fn create_table_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path("create_table"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::create_table_from_body)
  }

  async fn create_table_from_bytes(&self, body: Bytes) -> PancakeResult<CreateTableResponse> {
    let req = utils::parse_pb::<CreateTableRequest>(body)?;
    self.create_table(req).await
  }

  async fn create_table_from_body(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    utils::pancake_result_into_warp(server.create_table_from_bytes(body).await)
  }
}

