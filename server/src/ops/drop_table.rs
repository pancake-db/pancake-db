use async_trait::async_trait;
use pancake_db_idl::ddl::{DropTableRequest, DropTableResponse};
use tokio::fs;

use crate::dirs;
use crate::errors::ServerResult;
use crate::locks::table::TableWriteLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::utils;

pub struct DropTableOp {
  pub req: DropTableRequest,
}

#[async_trait]
impl ServerOp<TableWriteLocks> for DropTableOp {
  type Response = DropTableResponse;

  fn get_key(&self) -> ServerResult<String> {
    Ok(self.req.table_name.clone())
  }

  async fn execute_with_locks(
    &self,
    server: &Server,
    locks: TableWriteLocks
  ) -> ServerResult<Self::Response> where TableWriteLocks: 'async_trait {
    let opts = &server.opts;
    let table_name = &self.req.table_name;
    utils::validate_entity_name_for_write("table name", table_name)?;
    let TableWriteLocks {
      mut maybe_schema_guard
    } = locks;
    let maybe_schema = &mut *maybe_schema_guard;

    log::info!("dropping table: {}", table_name);

    // TODO make this recoverable by adding drop file, deleting non-
    // drop files in directory, and then the whole directory
    *maybe_schema = None;
    server.partition_metadata_cache.prune(|key| &key.table_name == table_name)
      .await;
    server.segment_metadata_cache.prune(|key| &key.table_name == table_name)
      .await;
    // don't really need to prune compaction metadata because nothing will try to use it
    // without the segment metadata
    fs::remove_dir_all(dirs::table_dir(&opts.dir, table_name)).await?;

    Ok(DropTableResponse {..Default::default()})
  }
}
