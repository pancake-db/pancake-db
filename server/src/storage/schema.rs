use std::path::PathBuf;

use pancake_db_idl::ddl::create_table_request::SchemaMode;
use pancake_db_idl::schema::Schema;
use protobuf::json;
use protobuf::json::parse_from_str;

use crate::{dirs, utils};
use crate::errors::{ServerError, ServerResult};
use crate::storage::traits::{MetadataJson, MetadataKey};

use super::traits::{CacheData, Metadata};
use std::collections::HashMap;

type SchemaKey = String;

impl MetadataKey for SchemaKey {
  const ENTITY_NAME: &'static str = "schema";
}

impl MetadataJson for Schema {
  fn to_json_string(&self) -> ServerResult<String> {
    json::print_to_string(self)
      .map_err(|_| ServerError::internal("unable to print schema to json string"))
  }

  fn from_json_str(s: &str) -> ServerResult<Schema> {
    Ok(parse_from_str(s)?)
  }
}

impl Metadata<SchemaKey> for Schema {
  fn relative_path(table_name: &SchemaKey) -> PathBuf {
    dirs::table_subdir(table_name)
      .join("schema.json")
  }
}

pub type SchemaCache = CacheData<SchemaKey, Schema>;

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

impl SchemaCache {
  pub async fn create(&self, table_name: &str, schema: &Schema, mode: SchemaMode) -> ServerResult<bool> {
    utils::validate_entity_name_for_write("table name", table_name)?;
    for meta in &schema.partitioning {
      utils::validate_entity_name_for_write("partition name", &meta.name)?;
    }
    for meta in &schema.columns {
      utils::validate_entity_name_for_write("column name", &meta.name)?;
    }

    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    let table_name_string = table_name.to_string();
    if !map.contains_key(&table_name_string) {
      map.insert(
        table_name.to_string(),
        Schema::load(&self.dir, &table_name_string).await
      );
    }

    match map.get(table_name).unwrap() {
      Some(existing_schema) => {
        if !partitioning_matches(schema, existing_schema) {
          return Err(ServerError::invalid("existing schema has different partitioning"))
        }
        match mode {
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
        map.insert(table_name.to_string(), Some(schema.clone()));
        let table_dir = dirs::table_dir(&self.dir, &table_name);
        utils::create_if_new(table_dir).await?;
        schema.overwrite(&self.dir, &table_name_string).await?;
        Ok(false)
      }
    }
  }
}
