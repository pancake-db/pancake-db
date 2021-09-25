use hyper::Method;
use pancake_db_idl::ddl::{CreateTableRequest, CreateTableResponse};

use crate::errors::ClientResult;

use super::Client;

impl Client {
  pub async fn create_table(&self, req: &CreateTableRequest) -> ClientResult<CreateTableResponse> {
    self.simple_json_request::<CreateTableRequest, CreateTableResponse>(
      "create_table",
      Method::POST,
      req,
    ).await
  }
}
