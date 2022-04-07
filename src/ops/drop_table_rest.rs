use pancake_db_idl::ddl::DropTableRequest;

use crate::{Server, ServerResult};
use crate::locks::table::TableWriteLocks;
use crate::locks::traits::ServerOpLocks;
use crate::ops::drop_table::DropTableOp;
use crate::ops::traits::{RestRoute, ServerOp};
use crate::serde_models::{DropTableRequestSerde, EmptySerde};

pub struct DropTableRestOp {
  pub req: DropTableRequestSerde
}

#[async_trait::async_trait]
impl ServerOp for DropTableRestOp {
  type Locks = TableWriteLocks;
  type Response = EmptySerde;

  fn get_key(&self) -> ServerResult<<Self::Locks as ServerOpLocks>::Key> {
    Ok(self.req.table_name.to_string())
  }

  async fn execute_with_locks(&self, server: &Server, locks: Self::Locks) -> ServerResult<Self::Response> {
    let pb_req = DropTableRequest {
      table_name: self.req.table_name.to_string()
    };
    DropTableOp { req: pb_req }.execute_with_locks(server, locks).await?;
    Ok(EmptySerde {})
  }
}

impl RestRoute for DropTableRestOp {
  type Req = DropTableRequestSerde;

  const ROUTE_NAME: &'static str = "drop_table";

  fn new_op(req: Self::Req) -> DropTableRestOp {
    DropTableRestOp { req }
  }
}
