use hyper::Method;
use pancake_db_idl::ddl::{DropTableRequest, DropTableResponse};

use crate::Client;
use crate::errors::ClientResult;

impl Client {
  pub async fn drop_table(&self, req: &DropTableRequest) -> ClientResult<DropTableResponse> {
    self.simple_json_request::<DropTableRequest, DropTableResponse>(
      "drop_table",
      Method::POST,
      req,
    ).await
  }
}
