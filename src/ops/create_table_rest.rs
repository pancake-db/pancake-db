use pancake_db_idl::ddl::CreateTableRequest;
use pancake_db_idl::schema::Schema;

use crate::{Server, ServerResult};
use crate::locks::table::TableWriteLocks;
use crate::locks::traits::ServerOpLocks;
use crate::ops::create_table::CreateTableOp;
use crate::ops::traits::{RestRoute, ServerOp};
use crate::serde_models::{CreateTableRequestSerde, CreateTableResponseSerde};

pub struct CreateTableRestOp {
  pub req: CreateTableRequestSerde
}

#[async_trait::async_trait]
impl ServerOp for CreateTableRestOp {
  type Locks = TableWriteLocks;
  type Response = CreateTableResponseSerde;

  fn get_key(&self) -> ServerResult<<Self::Locks as ServerOpLocks>::Key> {
    Ok(self.req.table_name.to_string())
  }

  async fn execute_with_locks(&self, server: &Server, locks: Self::Locks) -> ServerResult<Self::Response> {
    let req = &self.req;
    let pb_req = CreateTableRequest {
      table_name: req.table_name.clone(),
      schema: Some(Schema::from(&req.schema)),
      mode: i32::from(req.mode),
    };
    let pb_resp = CreateTableOp { req: pb_req }.execute_with_locks(
      server,
      locks,
    ).await?;
    Ok(CreateTableResponseSerde {
      already_exists: pb_resp.already_exists,
      columns_added: pb_resp.columns_added.clone(),
    })
  }
}

impl RestRoute for CreateTableRestOp {
  type Req = CreateTableRequestSerde;

  const ROUTE_NAME: &'static str = "create_table";

  fn new_op(req: Self::Req) -> CreateTableRestOp {
    CreateTableRestOp { req }
  }
}
