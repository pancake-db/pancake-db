use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use pancake_db_idl::ddl::{CreateTableRequest, CreateTableResponse, AlterTableRequest};
use pancake_db_idl::ddl::create_table_request::SchemaMode;
use pancake_db_idl::schema::Schema;

use crate::constants::{MAX_PARTITIONING_DEPTH, MAX_NESTED_LIST_DEPTH, MAX_N_COLUMNS};
use crate::utils::dirs;
use crate::errors::{ServerError, ServerResult};
use crate::locks::table::TableWriteLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::Metadata;
use crate::utils::common;
use crate::storage::table::TableMetadata;
use crate::ops::alter_table::AlterTableOp;

fn partitioning_matches(schema0: &Schema, schema1: &Schema) -> bool {
  if schema0.partitioning.len() != schema1.partitioning.len() {
    return false;
  }

  let mut partitioning = HashMap::new();
  schema0.partitioning.iter()
    .for_each(|meta| {
      partitioning.insert(meta.name.clone(), meta);
    });

  for meta1 in &schema1.partitioning {
    let agrees = match partitioning.get(&meta1.name) {
      Some(meta0) => *meta0 == meta1,
      None => false,
    };
    if !agrees {
      return false;
    }
  }
  true
}

fn is_subset(sub_schema: &Schema, schema: &Schema) -> bool {
  let mut columns = HashMap::new();
  for column in &schema.columns {
    columns.insert(column.name.clone(), column);
  }

  for sub_column in &sub_schema.columns {
    let agrees = match columns.get(&sub_column.name) {
      Some(column) => *column == sub_column,
      None => false,
    };
    if !agrees {
      return false;
    }
  }
  true
}

pub struct CreateTableOp {
  pub req: CreateTableRequest,
}

#[async_trait]
impl ServerOp<TableWriteLocks> for CreateTableOp {
  type Response = CreateTableResponse;

  fn get_key(&self) -> ServerResult<String> {
    Ok(self.req.table_name.clone())
  }

  async fn execute_with_locks(
    &self,
    server: &Server,
    mut locks: TableWriteLocks,
  ) -> ServerResult<CreateTableResponse> {
    let req = &self.req;
    let table_name = &req.table_name;
    let dir = &server.opts.dir;
    let schema_mode = self.req.mode.enum_value()
      .map_err(|mode| ServerError::invalid(&format!("unknown schema mode {}", mode)))?;

    let schema = match &req.schema.0 {
      Some(s) => Ok(s),
      None => Err(ServerError::invalid("missing table schema")),
    }?.as_ref();

    common::validate_entity_name_for_write("table name", &req.table_name)?;
    if schema.partitioning.len() > MAX_PARTITIONING_DEPTH {
      return Err(ServerError::invalid(&format!(
        "number of partition fields may not exceed {} but was {}",
        MAX_PARTITIONING_DEPTH,
        schema.partitioning.len(),
      )));
    }
    if schema.columns.len() > MAX_N_COLUMNS {
      return Err(ServerError::invalid(&format!(
        "number of columns may not exceed {} but was {}; rethink your data model",
        MAX_N_COLUMNS,
        schema.columns.len(),
      )));
    }
    for meta in &schema.partitioning {
      common::validate_entity_name_for_write("partition name", &meta.name)?;
    }
    for meta in &schema.columns {
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

    let maybe_table = &mut *locks.maybe_table_guard;
    let mut result = CreateTableResponse {..Default::default()};

    match maybe_table {
      Some(table_meta) => {
        result.already_exists = true;
        if !partitioning_matches(schema, &table_meta.schema) {
          return Err(ServerError::invalid("existing schema has different partitioning"))
        }

        match schema_mode {
          SchemaMode::FAIL_IF_EXISTS => Err(ServerError::invalid("table already exists")),
          SchemaMode::OK_IF_EXACT => {
            if is_subset(&table_meta.schema, schema) && is_subset(schema, &table_meta.schema) {
              Ok(result)
            } else {
              Err(ServerError::invalid("existing schema columns are not identical"))
            }
          },
          SchemaMode::ADD_NEW_COLUMNS => {
            if is_subset(&table_meta.schema, schema) {
              let mut new_columns = Vec::new();
              let existing_col_names: HashSet<_> = table_meta.schema.columns
                .iter()
                .map(|c| c.name.clone())
                .collect();
              for col_meta in &schema.columns {
                if !existing_col_names.contains(&col_meta.name) {
                  new_columns.push(col_meta.clone());
                  result.columns_added.push(col_meta.name.clone());
                }
              }
              let alter_table_op = AlterTableOp {
                req: AlterTableRequest {
                  table_name: req.table_name.to_string(),
                  new_columns,
                  ..Default::default()
                }
              };
              alter_table_op.execute_with_locks(server, locks).await?;
              Ok(result)
            } else {
              Err(ServerError::invalid("existing schema contains columns not in declared schema"))
            }
          }
        }
      }
      None => {
        log::info!("creating new table: {}", table_name);

        common::create_if_new(dir).await?;
        let table_dir = dirs::table_dir(dir, table_name);
        common::create_if_new(table_dir).await?;
        let table_data_dir = dirs::table_data_dir(dir, table_name);
        common::create_if_new(table_data_dir).await?;

        let table_meta = TableMetadata::new(schema.clone());
        *maybe_table = Some(table_meta.clone());
        table_meta.overwrite(dir, table_name).await?;
        Ok(result)
      }
    }
  }
}