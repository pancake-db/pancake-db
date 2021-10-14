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
