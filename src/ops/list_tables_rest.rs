use pancake_db_idl::ddl::ListTablesRequest;

use crate::{Server, ServerResult};
use crate::locks::traits::ServerOpLocks;
use crate::locks::trivial::TrivialLocks;
use crate::ops::list_tables::ListTablesOp;
use crate::ops::traits::{RestRoute, ServerOp};
use crate::serde_models::{EmptySerde, ListTablesResponseSerde, TableInfoSerde};

pub struct ListTablesRestOp;

#[async_trait::async_trait]
impl ServerOp for ListTablesRestOp {
  type Locks = TrivialLocks;
  type Response = ListTablesResponseSerde;

  fn get_key(&self) -> ServerResult<<Self::Locks as ServerOpLocks>::Key> {
    Ok(())
  }

  async fn execute_with_locks(&self, server: &Server, locks: Self::Locks) -> ServerResult<Self::Response> {
    let pb_resp = ListTablesOp { req: ListTablesRequest::default() }.execute_with_locks(server, locks).await?;
    Ok(ListTablesResponseSerde {
      tables: pb_resp.tables.iter()
        .map(|t| TableInfoSerde {
          table_name: t.table_name.to_string()
        })
        .collect()
    })
  }
}

impl RestRoute for ListTablesRestOp {
  type Req = EmptySerde;

  const ROUTE_NAME: &'static str = "list_tables";

  fn new_op(_req: Self::Req) -> ListTablesRestOp {
    ListTablesRestOp
  }
}
