use std::path::PathBuf;

use pancake_db_idl::schema::Schema;
use protobuf::json;
use protobuf::json::parse_from_str;

use crate::dirs;
use crate::errors::{ServerError, ServerResult};
use crate::storage::traits::{MetadataJson, MetadataKey};

use super::traits::{CacheData, Metadata};

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

impl SchemaCache {
  pub async fn assert(&self, table_name: &str, schema: &Schema) -> ServerResult<()> {
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    let table_name_string = table_name.to_string();
    if !map.contains_key(&table_name_string) {
      map.insert(
        table_name.to_string(),
        Schema::load(&self.dir, &table_name_string).await
      );
    }

    return match map.get(table_name).unwrap() {
      Some(existing_schema) => {
        if existing_schema == schema {
          Ok(())
        } else {
          Err(ServerError::invalid("existing schema does not match"))
        }
      },
      None => {
        map.insert(table_name.to_string(), Some(schema.clone()));
        schema.overwrite(&self.dir, &table_name_string).await?;
        Ok(())
      }
    };
  }
}
