use async_trait::async_trait;
use pancake_db_idl::ddl::{GetSchemaRequest, GetSchemaResponse};

use crate::errors::ServerResult;
use crate::locks::table::TableReadLocks;
use crate::ops::traits::ServerOp;

use crate::server::Server;

pub struct GetSchemaOp {
  pub req: GetSchemaRequest,
}

#[async_trait]
impl ServerOp for GetSchemaOp {
  type Locks = TableReadLocks;
  type Response = GetSchemaResponse;

  fn get_key(&self) -> ServerResult<String> {
    Ok(self.req.table_name.clone())
  }

  async fn execute_with_locks(&self, _server: &Server, locks: TableReadLocks) -> ServerResult<GetSchemaResponse> {
    let TableReadLocks { table_meta } = locks;
    Ok(GetSchemaResponse {
      schema: Some(table_meta.schema()),
      ..Default::default()
    })
  }
}