use async_trait::async_trait;
use pancake_db_idl::ddl::{DropTableRequest, DropTableResponse};
use tokio::fs;

use crate::dirs;
use crate::errors::{ServerResult, ServerError};
use crate::locks::table::TableWriteLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::utils;
use crate::storage::Metadata;
use crate::storage::table::TableMetadata;
use std::path::Path;

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
    let dir = &opts.dir;
    let table_name = &self.req.table_name;
    utils::validate_entity_name_for_write("table name", table_name)?;
    let TableWriteLocks {
      mut maybe_table_guard
    } = locks;
    let maybe_table = &mut *maybe_table_guard;

    if maybe_table.is_none() {
      return Err(ServerError::does_not_exist("table", table_name));
    }

    log::info!("dropping table: {}", table_name);

    // first mark table as dropped so we can ensure recovery
    let table_meta = maybe_table.as_mut().unwrap();
    table_meta.dropped = true;
    table_meta.overwrite(dir, table_name).await?;

    *maybe_table = None;
    server.partition_metadata_cache.prune(|key| &key.table_name == table_name)
      .await;
    server.segment_metadata_cache.prune(|key| &key.table_name == table_name)
      .await;
    // don't really need to prune compaction metadata because nothing will try to use it
    // without the segment metadata
    // we need to delete data directory first, or else we might delete the `dropped`
    // flag in the metadata, crash, and leave all the data on disk, unable to resume
    Self::remove_data(dir, table_name).await?;

    Ok(DropTableResponse {..Default::default()})
  }
}

impl DropTableOp {
  async fn remove_data(dir: &Path, table_name: &str) -> ServerResult<()> {
    fs::remove_dir_all(dirs::table_data_dir(dir, table_name)).await?;
    fs::remove_dir_all(dirs::table_dir(dir, table_name)).await?;
    Ok(())
  }

  pub async fn recover(
    server: &Server,
    table_name: &str,
    table_meta: &TableMetadata
  ) -> ServerResult<()> {
    if table_meta.dropped {
      log::debug!(
        "identified interrupted drop for table {}; removing files",
        table_name
      );
      Self::remove_data(&server.opts.dir, table_name).await?;
    }
    Ok(())
  }
}
