use async_trait::async_trait;
use pancake_db_idl::ddl::{GetSchemaRequest, GetSchemaResponse};
use protobuf::MessageField;

use crate::errors::ServerResult;
use crate::locks::table::TableReadLocks;
use crate::ops::traits::ServerOp;
use crate::opt::Opt;
use crate::server::Server;

pub struct GetSchemaOp {
  req: GetSchemaRequest,
}

#[async_trait]
impl ServerOp<TableReadLocks> for GetSchemaOp {
  type Response = GetSchemaResponse;

  fn get_key(&self) -> String {
    self.req.table_name.clone()
  }

  async fn execute_with_locks(&self, _server: &Server, locks: TableReadLocks) -> ServerResult<GetSchemaResponse> {
    let TableReadLocks { schema } = locks;
    Ok(GetSchemaResponse {
      schema: MessageField::some(schema),
      ..Default::default()
    })
  }
}