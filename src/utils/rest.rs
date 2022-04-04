use std::convert::Infallible;

use hyper::body::Bytes;
use hyper::Response;
use serde::de::DeserializeOwned;
use serde::Serialize;
use warp::{Filter, Rejection, Reply};

use crate::{Server, ServerResult};
use crate::errors::ServerError;
use crate::ops::create_table_rest::CreateTableRestOp;
use crate::ops::drop_table_rest::DropTableRestOp;
use crate::ops::list_tables_rest::ListTablesRestOp;
use crate::ops::traits::RestRoute;
use crate::ops::write_to_partition_rest::WriteToPartitionRestOp;

pub fn warp_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
  warp::path("api")
    .and(
      warp_sub_filter::<CreateTableRestOp>()
        .or(warp_sub_filter::<DropTableRestOp>())
        .or(warp_sub_filter::<ListTablesRestOp>())
        .or(warp_sub_filter::<WriteToPartitionRestOp>())
    )
}

pub fn warp_sub_filter<Route>() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone
  where Route: RestRoute, Route::Response: Serialize {
  warp::get()
    .and(warp::path(Route::ROUTE_NAME))
    .and(warp::filters::ext::get::<Server>())
    .and(warp::filters::body::bytes())
    .and_then(warp_execute::<Route>)
}

async fn warp_execute<Route>(
  server: Server,
  body: Bytes,
) -> Result<Box<dyn Reply>, Infallible>
  where Route: RestRoute, Route::Response: Serialize {
  log::info!(
    "received REST request for {} containing {} bytes",
    Route::ROUTE_NAME,
    body.len(),
  );
  pancake_result_into_warp(
    execute_from_body::<Route>(&server, body).await,
    Route::ROUTE_NAME,
  )
}

async fn execute_from_body<Route>(
  server: &Server,
  body: Bytes,
) -> ServerResult<Route::Response>
  where Route: RestRoute, Route::Response: Serialize {
  let req = parse_rest_req(body)?;
  Route::new_op(req).execute(server).await
}

#[derive(Serialize)]
struct ErrorResponse {
  pub message: String,
}

fn pancake_result_into_warp<T: Serialize>(
  server_res: ServerResult<T>,
  route_name: &str,
) -> Result<Box<dyn Reply>, Infallible> {
  let body_res = server_res.and_then(|pb|
    serde_json::to_string(&pb)
      .map_err(|_| ServerError::internal("unable to write response as json"))
  );
  match body_res {
    Ok(body) => {
      log::info!(
        "replying OK to {} request with {} bytes",
        route_name,
        body.len()
      );
      Ok(Box::new(Response::new(body)))
    },
    Err(e) => {
      let reply = warp::reply::json(&ErrorResponse {
        message: e.to_client_string(),
      });
      let status = e.kind.warp_status_code();
      log::info!(
        "replying ERR to {} request with status {}: {}",
        route_name,
        status,
        e,
      );
      Ok(Box::new(warp::reply::with_status(
        reply,
        status,
      )))
    }
  }
}

fn parse_rest_req<T: DeserializeOwned>(body: Bytes) -> ServerResult<T> {
  let body_string = String::from_utf8(body.to_vec())
    .map_err(|_| ServerError::invalid("body bytes do not parse to string"))?;
  let req = serde_json::from_str(&body_string)
    .map_err(|_| ServerError::invalid("body string does not parse to the correct request format"))?;
  Ok(req)
}
