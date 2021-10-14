use std::convert::Infallible;

use hyper::body::Bytes;
use pancake_db_idl::ddl::{DropTableRequest, DropTableResponse};


use warp::{Filter, Rejection, Reply};


use crate::errors::{ServerResult};
use crate::server::Server;

use crate::utils;

impl Server {
  pub async fn drop_table(&self, _req: DropTableRequest) -> ServerResult<DropTableResponse> {
    // // clear schema cache and keep it locked until we finish up here
    // let mut schema_mux_guard = self.schema_cache.data.write().await;
    // let schema_map = &mut *schema_mux_guard;
    //
    // // verify table exists
    // if !schema_map.contains_key(&req.table_name) {
    //   let maybe_schema = Schema::load(&self.opts.dir, &req.table_name).await?;
    //   if maybe_schema.is_none() {
    //     return Err(ServerError::does_not_exist("table", &req.table_name));
    //   }
    // }
    //
    // // clear metadata
    // schema_map.insert(req.table_name.to_string(), None);
    //
    // fs::remove_dir_all(dirs::table_dir(&self.opts.dir, &req.table_name)).await?;
    //
    Ok(DropTableResponse {..Default::default()})
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