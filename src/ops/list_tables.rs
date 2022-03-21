use async_trait::async_trait;
use pancake_db_idl::ddl::{ListTablesRequest, ListTablesResponse, TableInfo};

use crate::errors::ServerResult;
use crate::locks::trivial::TrivialLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;

pub struct ListTablesOp {
  pub req: ListTablesRequest,
}

#[async_trait]
impl ServerOp for ListTablesOp {
  type Locks = TrivialLocks;
  type Response = ListTablesResponse;

  fn get_key(&self) -> ServerResult<()> {
    Ok(())
  }

  async fn execute_with_locks(
    &self,
    server: &Server,
    _locks: TrivialLocks,
  ) -> ServerResult<ListTablesResponse> {
    let mut tables = Vec::new();
    for info in server.internal_list_tables().await? {
      tables.push(TableInfo {
        table_name: info.name,
        ..Default::default()
      })
    }
    Ok(ListTablesResponse {
      tables,
      ..Default::default()
    })
  }
}