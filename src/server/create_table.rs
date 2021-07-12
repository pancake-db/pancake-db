use pancake_db_idl::ddl::{CreateTableRequest, CreateTableResponse};
use pancake_db_idl::schema::Schema;
use warp::{Filter, Rejection, Reply};

use crate::dirs;
use crate::server::Server;
use crate::utils;

impl Server {
  pub async fn create_table(&self, name: &String, schema: &Schema) -> Result<(), &'static str> {
    utils::create_if_new(&self.dir).await?;

    let table_dir = dirs::table_dir(&self.dir, name);
    utils::create_if_new(&table_dir).await?;

    self.schema_cache.assert(name, schema).await
  }

  pub fn create_table_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path("rest"))
      .and(warp::path("create_table"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::json())
      .and_then(Self::create_table_pb)
  }

  async fn create_table_pb(server: Server, req: CreateTableRequest) -> Result<impl Reply, Rejection> {
    match server.create_table(&req.table_name, &req.schema.0.expect("missing schema")).await {
      Ok(_) => Ok(warp::reply::json(&CreateTableResponse::new())),
      Err(_) => Err(warp::reject()),
    }
  }
}

