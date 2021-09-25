use hyper::Method;
use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse};

use crate::errors::ClientResult;

use super::Client;

impl Client {
  pub async fn write_to_partition(&self, req: &WriteToPartitionRequest) -> ClientResult<WriteToPartitionResponse> {
    self.simple_json_request::<WriteToPartitionRequest, WriteToPartitionResponse>(
      "write_to_partition",
      Method::POST,
      req,
    ).await
  }
}
