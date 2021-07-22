use std::convert::Infallible;

use pancake_db_core::errors::{PancakeError, PancakeResult};
use pancake_db_idl::ddl::{CreateTableRequest, CreateTableResponse};
use warp::{Filter, Rejection, Reply};

use crate::dirs;
use crate::server::Server;
use crate::utils;

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
      .and(warp::filters::body::json())
      .and_then(Self::create_table_pb)
  }

  async fn create_table_pb(server: Server, req: CreateTableRequest) -> Result<impl Reply, Infallible> {
    utils::pancake_result_into_warp(server.create_table(req).await)
  }
}

