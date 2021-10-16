use std::convert::TryFrom;
use std::path::PathBuf;

use pancake_db_idl::schema::Schema;
use protobuf::json;
use protobuf::json::{ParseError, PrintError};
use serde::{Deserialize, Serialize};

use crate::utils::dirs;
use crate::errors::{ServerError, ServerResult};
use crate::storage::traits::{MetadataJson, MetadataKey};

use super::traits::{CacheData, Metadata};
use crate::constants::TABLE_METADATA_FILENAME;

type TableKey = String;

#[derive(Serialize, Deserialize)]
struct TableMetadataSerde {
  pub schema_string: String,
  pub dropped: bool,
}

#[derive(Clone)]
pub struct TableMetadata {
  pub schema: Schema,
  pub dropped: bool,
}

impl TryFrom<TableMetadata> for TableMetadataSerde {
  type Error = PrintError;

  fn try_from(value: TableMetadata) -> Result<Self, Self::Error> {
    let schema_string = json::print_to_string(&value.schema)?;
    Ok(TableMetadataSerde {
      schema_string,
      dropped: value.dropped,
    })
  }
}

impl TryFrom<TableMetadataSerde> for TableMetadata {
  type Error = ParseError;

  fn try_from(value: TableMetadataSerde) -> Result<Self, Self::Error> {
    let schema = json::parse_from_str(&value.schema_string)?;
    Ok(TableMetadata {
      schema,
      dropped: value.dropped,
    })
  }
}

impl MetadataKey for TableKey {
  const ENTITY_NAME: &'static str = "table";
}

impl TableMetadata {
  pub fn new(schema: Schema) -> Self {
    TableMetadata {
      schema,
      dropped: false,
    }
  }
}

impl MetadataJson for TableMetadata {
  fn to_json_string(&self) -> ServerResult<String> {
    let table_meta_serde = TableMetadataSerde::try_from(self.clone())
      .map_err(|_| ServerError::internal("unable to print schema to json string"))?;
    Ok(serde_json::to_string(&table_meta_serde)?)
  }

  fn from_json_str(s: &str) -> ServerResult<Self> {
    let table_meta_serde: TableMetadataSerde = serde_json::from_str::<'_, TableMetadataSerde>(s)?;
    Ok(TableMetadata::try_from(table_meta_serde)?)
  }
}

impl Metadata<TableKey> for TableMetadata {
  fn relative_path(table_name: &TableKey) -> PathBuf {
    dirs::relative_table_dir(table_name)
      .join(TABLE_METADATA_FILENAME)
  }
}

pub type TableMetadataCache = CacheData<TableKey, TableMetadata>;
