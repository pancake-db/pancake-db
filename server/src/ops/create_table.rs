use std::collections::HashMap;

use async_trait::async_trait;
use pancake_db_idl::ddl::{CreateTableRequest, CreateTableResponse};
use pancake_db_idl::ddl::create_table_request::SchemaMode;
use pancake_db_idl::schema::Schema;

use crate::dirs;
use crate::errors::{ServerError, ServerResult};
use crate::locks::table::TableWriteLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::storage::Metadata;
use crate::utils;

const MAX_NESTED_LIST_DEPTH: u32 = 3;

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
  req: CreateTableRequest,
}

#[async_trait]
impl<'a> ServerOp<TableWriteLocks<'a>> for CreateTableOp {
  type Response = CreateTableResponse;

  fn get_key(&self) -> String {
    self.req.table_name.clone()
  }

  async fn execute_with_locks(&self, server: &Server, locks: TableWriteLocks<'a>) -> ServerResult<CreateTableResponse> {
    let req = &self.req;
    let table_name = &req.table_name;
    let dir = &server.opts.dir;
    let schema_mode = self.req.mode.enum_value()
      .map_err(|mode| ServerError::invalid(&format!("unknown schema mode {}", mode)))?;

    let schema = match &req.schema.0 {
      Some(s) => Ok(s),
      None => Err(ServerError::invalid("missing table schema")),
    }?.as_ref();

    utils::validate_entity_name_for_write("table name", &req.table_name)?;
    for meta in &schema.partitioning {
      utils::validate_entity_name_for_write("partition name", &meta.name)?;
    }
    for meta in &schema.columns {
      utils::validate_entity_name_for_write("column name", &meta.name)?;
      if meta.nested_list_depth > MAX_NESTED_LIST_DEPTH {
        return Err(ServerError::invalid(&format!(
          "nested_list_depth may not exceed {} but was {} for {}",
          MAX_NESTED_LIST_DEPTH,
          meta.nested_list_depth,
          meta.name,
        )))
      }
    }

    let TableWriteLocks { maybe_schema } = locks;

    let already_exists = match maybe_schema {
      Some(existing_schema) => {
        if !partitioning_matches(schema, existing_schema) {
          return Err(ServerError::invalid("existing schema has different partitioning"))
        }
        match schema_mode {
          SchemaMode::FAIL_IF_EXISTS => Err(ServerError::invalid("table already exists")),
          SchemaMode::OK_IF_EXACT => {
            if is_subset(existing_schema, schema) && is_subset(schema, existing_schema) {
              Ok(true)
            } else {
              Err(ServerError::invalid("existing schema columns are not identical"))
            }
          },
        }
      },
      None => {
        log::info!("creating new table: {}", table_name);

        utils::create_if_new(dir).await?;
        let table_dir = dirs::table_dir(dir, table_name);
        utils::create_if_new(table_dir).await?;

        *maybe_schema = Some(schema.clone());
        schema.overwrite(dir, table_name).await?;
        Ok(false)
      }
    }?;

    Ok(CreateTableResponse {
      already_exists,
      ..Default::default()
    })
  }
}