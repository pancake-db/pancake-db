use std::convert::Infallible;

use crate::errors::{ServerError, ServerResult};
use pancake_db_idl::ddl::{CreateTableRequest, CreateTableResponse};
use warp::{Filter, Rejection, Reply};

use crate::server::Server;
use crate::utils;
use hyper::body::Bytes;

// no one should ever need deeper nesting than this
const MAX_NESTED_LIST_DEPTH: u32 = 3;

impl Server {
  pub async fn create_table(&self, req: CreateTableRequest) -> ServerResult<CreateTableResponse> {
    let schema = match &req.schema.0 {
      Some(s) => Ok(s),
      None => Err(ServerError::invalid("missing table schema")),
    }?.as_ref();

    utils::validate_entity_name_for_write("table name", &req.table_name)?;
    for meta in &schema.partitioning {
      utils::validate_entity_name_for_write("partition name", &meta.name)?;
    }
    for meta in &schema.columns {
      utils::validate_entity_name_for_write("column name", &meta.name)?;
      if meta.nested_list_depth > MAX_NESTED_LIST_DEPTH {
        return Err(ServerError::invalid(&format!(
          "nested_list_depth may not exceed {} but was {} for {}",
          MAX_NESTED_LIST_DEPTH,
          meta.nested_list_depth,
          meta.name,
        )))
      }
    }

    utils::create_if_new(&self.opts.dir).await?;

    let already_exists = self.schema_cache
      .create(&req.table_name, schema, req.mode.enum_value_or_default()).await?;
    Ok(CreateTableResponse {
      already_exists,
      ..Default::default()
    })
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

