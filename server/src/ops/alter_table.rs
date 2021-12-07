use std::collections::HashSet;

use async_trait::async_trait;
use pancake_db_idl::ddl::{AlterTableRequest, AlterTableResponse};

use crate::constants::MAX_NESTED_LIST_DEPTH;
use crate::errors::{ServerError, ServerResult};
use crate::locks::table::TableWriteLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::Metadata;
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

    common::validate_entity_name_for_write("table name", &req.table_name)?;
    if req.new_columns.is_empty() {
      return Err(ServerError::invalid("alter table request contains 0 alterations"))
    }
    for meta in &req.new_columns {
      common::validate_entity_name_for_write("column name", &meta.name)?;
      if meta.nested_list_depth > MAX_NESTED_LIST_DEPTH {
        return Err(ServerError::invalid(&format!(
          "nested_list_depth may not exceed {} but was {} for {}",
          MAX_NESTED_LIST_DEPTH,
          meta.nested_list_depth,
          meta.name,
        )))
      }
    }

    let TableWriteLocks {
      mut maybe_table_guard
    } = locks;
    let maybe_table = &mut *maybe_table_guard;

    match maybe_table {
      Some(table_meta) => {
        let existing_column_names: HashSet<_> = table_meta.schema.columns
          .iter()
          .map(|c| c.name.clone())
          .collect();
        for column in &req.new_columns {
          if existing_column_names.contains(&column.name) {
            return Err(ServerError::invalid(&format!(
              "column {} already exists",
              column.name
            )))
          }
        }

        let mut new_table_meta = table_meta.clone();
        new_table_meta.schema.columns.extend_from_slice(&req.new_columns);
        new_table_meta.overwrite(dir, table_name).await?;
        *maybe_table_guard = Some(new_table_meta);
        Ok(())
      }
      None => {
        Err(ServerError::does_not_exist("table", table_name))
      }
    }?;

    Ok(AlterTableResponse {
      ..Default::default()
    })
  }
}