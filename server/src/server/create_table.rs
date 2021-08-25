use std::convert::Infallible;

use crate::errors::{ServerError, ServerResult};
use pancake_db_idl::ddl::{CreateTableRequest, CreateTableResponse};
use pancake_db_idl::ddl::create_table_request::Mode;
use warp::{Filter, Rejection, Reply};

use crate::dirs;
use crate::server::Server;
use crate::utils;
use hyper::body::Bytes;

impl Server {
  pub async fn create_table(&self, req: CreateTableRequest) -> ServerResult<CreateTableResponse> {
    let schema = match &req.schema.0 {
      Some(s) => Ok(s),
      None => Err(ServerError::invalid("missing table schema")),
    }?.as_ref();

    utils::create_if_new(&self.opts.dir).await?;

    let table_dir = dirs::table_dir(&self.opts.dir, &req.table_name);
    let already_exists = utils::create_if_new(&table_dir).await?;
    match (req.mode.enum_value_or_default(), already_exists) {
      (Mode::FAIL_IF_EXISTS, true) => {
        Err(ServerError::invalid(&format!("table already exists: {}", req.table_name)))
      },
      _ => {
        self.schema_cache.assert(&req.table_name, schema).await?;
        Ok(CreateTableResponse {
          already_exists,
          ..Default::default()
        })
      }
    }
  }

  pub fn create_table_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path("create_table"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::create_table_from_body)
  }

  async fn create_table_from_bytes(&self, body: Bytes) -> ServerResult<CreateTableResponse> {
    let req = utils::parse_pb::<CreateTableRequest>(body)?;
    self.create_table(req).await
  }

  async fn create_table_from_body(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    utils::pancake_result_into_warp(server.create_table_from_bytes(body).await)
  }
}

