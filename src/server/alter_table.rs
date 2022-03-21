use pancake_db_idl::ddl::{AlterTableRequest, AlterTableResponse};

use crate::errors::ServerResult;
use crate::ops::alter_table::AlterTableOp;
use crate::ops::traits::ServerOp;
use crate::server::Server;

impl Server {
  pub async fn alter_table(&self, req: AlterTableRequest) -> ServerResult<AlterTableResponse> {
    AlterTableOp { req }.execute(self).await
  }
}

