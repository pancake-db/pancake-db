use pancake_db_idl::ddl::{GetSchemaRequest, GetSchemaResponse};

use crate::errors::ServerResult;
use crate::ops::get_schema::GetSchemaOp;
use crate::ops::traits::ServerOp;
use crate::server::Server;

impl Server {
  pub async fn get_schema(&self, req: GetSchemaRequest) -> ServerResult<GetSchemaResponse> {
    GetSchemaOp { req }.execute(self).await
  }
}

