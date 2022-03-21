use pancake_db_idl::ddl::{CreateTableRequest, CreateTableResponse};

use crate::errors::ServerResult;
use crate::ops::create_table::CreateTableOp;
use crate::ops::traits::ServerOp;
use crate::server::Server;

impl Server {
  pub async fn create_table(&self, req: CreateTableRequest) -> ServerResult<CreateTableResponse> {
    CreateTableOp { req }.execute(self).await
  }
}

