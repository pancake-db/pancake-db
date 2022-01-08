use async_trait::async_trait;
use pancake_db_idl::ddl::{AlterTableRequest, AlterTableResponse};

use crate::constants::{MAX_NESTED_LIST_DEPTH, MAX_N_COLUMNS};
use crate::errors::{ServerError, ServerResult};
use crate::locks::table::TableWriteLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::metadata::PersistentMetadata;
use crate::utils::common;

pub struct AlterTableOp {
  pub req: AlterTableRequest,
}

#[async_trait]
impl ServerOp<TableWriteLocks> for AlterTableOp {
  type Response = AlterTableResponse;

  fn get_key(&self) -> ServerResult<String> {
    Ok(self.req.table_name.clone())
  }

  async fn execute_with_locks(
    &self,
    server: &Server,
    locks: TableWriteLocks,
  ) -> ServerResult<AlterTableResponse> {
    let req = &self.req;
    let table_name = &req.table_name;
    let dir = &server.opts.dir;

    let TableWriteLocks {
      mut maybe_table_guard
    } = locks;
    let table_meta = common::unwrap_metadata(table_name, &*maybe_table_guard)?;

    common::validate_entity_name_for_write("table name", &req.table_name)?;
    if req.new_columns.is_empty() {
      return Err(ServerError::invalid("alter table request contains 0 alterations"))
    }
    let all_column_names: Vec<String> = table_meta.schema.columns.keys()
      .chain(req.new_columns.keys())
      .cloned()
      .collect();
    let total_n_cols = all_column_names.len();
    common::check_no_duplicate_names("column", all_column_names)?;
    if total_n_cols > MAX_N_COLUMNS {
      return Err(ServerError::invalid(format!(
        "number of columns must not exceed {} but was {}+{}={}; rethink your data model",
        MAX_N_COLUMNS,
        table_meta.schema.columns.len(),
        req.new_columns.len(),
        total_n_cols,
      )));
    }
    for (col_name, col_meta) in &req.new_columns {
      common::validate_entity_name_for_write("column name", col_name)?;
      if col_meta.nested_list_depth > MAX_NESTED_LIST_DEPTH {
        return Err(ServerError::invalid(format!(
          "nested_list_depth may not exceed {} but was {} for {}",
          MAX_NESTED_LIST_DEPTH,
          col_meta.nested_list_depth,
          col_name,
        )));
      }
    }

    let mut new_table_meta = table_meta.clone();
    new_table_meta.schema.columns.extend(req.new_columns.clone());
    new_table_meta.overwrite(dir, table_name).await?;
    *maybe_table_guard = Some(new_table_meta);

    Ok(AlterTableResponse {
      ..Default::default()
    })
  }
}