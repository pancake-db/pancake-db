use std::collections::HashMap;
use std::path::PathBuf;

use pancake_db_idl::schema::{Schema, ColumnMeta, PartitionMeta};
use serde::{Deserialize, Serialize};

use crate::utils::dirs;
use crate::metadata::traits::MetadataKey;

use super::traits::{PersistentCacheData, PersistentMetadata};
use crate::constants::TABLE_METADATA_FILENAME;

type TableKey = String;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionMetaSerde {
  pub dtype: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnMetaSerde {
  pub dtype: i32,
  pub nested_list_depth: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaSerde {
  pub partitioning: HashMap<String, PartitionMetaSerde>,
  pub columns: HashMap<String, ColumnMetaSerde>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TableMetadata {
  schema: SchemaSerde,
  pub dropped: bool,
}

impl From<&ColumnMeta> for ColumnMetaSerde {
  fn from(meta: &ColumnMeta) -> Self {
    ColumnMetaSerde {
      nested_list_depth: meta.nested_list_depth,
      dtype: meta.dtype,
    }
  }
}

impl From<&ColumnMetaSerde> for ColumnMeta {
  fn from(meta: &ColumnMetaSerde) -> Self {
    ColumnMeta {
      dtype: meta.dtype,
      nested_list_depth: meta.nested_list_depth,
    }
  }
}

impl From<&PartitionMeta> for PartitionMetaSerde {
  fn from(meta: &PartitionMeta) -> Self {
    PartitionMetaSerde {
      dtype: meta.dtype
    }
  }
}

impl From<&PartitionMetaSerde> for PartitionMeta {
  fn from(meta: &PartitionMetaSerde) -> Self {
    PartitionMeta {
      dtype: meta.dtype
    }
  }
}

impl From<&Schema> for SchemaSerde {
  fn from(schema: &Schema) -> Self {
    SchemaSerde {
      columns: schema.columns.iter()
        .map(|(k, v)| (k.to_string(), ColumnMetaSerde::from(v)))
        .collect(),
      partitioning: schema.partitioning.iter()
        .map(|(k, v)| (k.to_string(), PartitionMetaSerde::from(v)))
        .collect(),
    }
  }
}

impl From<&SchemaSerde> for Schema {
  fn from(schema: &SchemaSerde) -> Self {
    Schema {
      columns: schema.columns.iter()
        .map(|(k, v)| (k.to_string(), ColumnMeta::from(v)))
        .collect(),
      partitioning: schema.partitioning.iter()
        .map(|(k, v)| (k.to_string(), PartitionMeta::from(v)))
        .collect(),
    }
  }
}

impl MetadataKey for TableKey {
  const ENTITY_NAME: &'static str = "table";
}

impl TableMetadata {
  pub fn new(schema: &Schema) -> Self {
    TableMetadata {
      schema: SchemaSerde::from(schema),
      dropped: false,
    }
  }

  pub fn schema(&self) -> Schema {
    Schema::from(&self.schema)
  }

  pub fn extend_columns(&mut self, columns: &HashMap<String, ColumnMeta>) {
    self.schema.columns.extend(
      columns.iter()
        .map(|(k, v)| (k.to_string(), ColumnMetaSerde::from(v)))
    )
  }
}

crate::impl_metadata_serde_json!(TableMetadata);

impl PersistentMetadata<TableKey> for TableMetadata {
  // no one should have more tables than this anyway
  const CACHE_SIZE_LIMIT: usize = 16384;

  fn relative_path(table_name: &TableKey) -> PathBuf {
    dirs::relative_table_dir(table_name)
      .join(TABLE_METADATA_FILENAME)
  }
}

pub type TableMetadataCache = PersistentCacheData<TableKey, TableMetadata>;
