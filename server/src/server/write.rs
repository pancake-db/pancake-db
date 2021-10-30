use std::convert::Infallible;

use hyper::body::Bytes;
use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse};
use warp::{Filter, Rejection, Reply};

use crate::utils::common;
use crate::errors::ServerResult;
use crate::ops::traits::ServerOp;
use crate::ops::write_to_partition::WriteToPartitionOp;

use super::Server;

const ROUTE_NAME: &str = "write_to_partition";

impl Server {
  pub async fn write_to_partition(&self, req: WriteToPartitionRequest) -> ServerResult<WriteToPartitionResponse> {
    WriteToPartitionOp { req }.execute(&self).await
  }

  pub fn write_to_partition_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path(ROUTE_NAME))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::warp_write_to_partition)
  }

  async fn write_to_partition_from_bytes(&self, body: Bytes) -> ServerResult<WriteToPartitionResponse> {
    let req = common::parse_pb::<WriteToPartitionRequest>(body)?;
    self.write_to_partition(req).await
  }

  async fn warp_write_to_partition(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    Self::log_request(ROUTE_NAME, &body);
    common::pancake_result_into_warp(
      server.write_to_partition_from_bytes(body).await,
      ROUTE_NAME,
    )
  }
}
