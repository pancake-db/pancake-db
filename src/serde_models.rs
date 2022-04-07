use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

use pancake_db_idl::ddl::create_table_request::SchemaMode;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use pancake_db_idl::schema::{ColumnMeta, PartitionMeta, Schema};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::ServerError;
use crate::ServerResult;

macro_rules! impl_serde_enum {
  ($t: ident, $orig: ty, {$($vars: ident),*}) => {
    #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub enum $t {
      $($vars),*
    }

    impl From<$orig> for $t {
      fn from(x: $orig) -> $t {
        match x {
          $(<$orig>::$vars => <$t>::$vars),*
        }
      }
    }

    impl From<$t> for $orig {
      fn from(x: $t) -> $orig {
        match x {
          $(<$t>::$vars => <$orig>::$vars),*
        }
      }
    }

    impl TryFrom<i32> for $t {
      type Error = ServerError;

      fn try_from(x: i32) -> ServerResult<$t> {
        let dtype = <$orig>::from_i32(x)
          .ok_or_else(|| ServerError::invalid("unknown enum value"))?;
        Ok($t::from(dtype))
      }
    }

    impl From<$t> for i32 {
      fn from(x: $t) -> i32 {
        <$orig>::from(x) as i32
      }
    }
  }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmptySerde {}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteToPartitionRequestSerde {
  pub table_name: String,
  #[serde(default)]
  pub partition: HashMap<String, Value>,
  pub rows: Vec<HashMap<String, Value>>,
}

impl_serde_enum!(
  DataTypeSerde,
  DataType,
  {Bool, Bytes, Float32, Float64, Int64, String, TimestampMicros}
);

impl_serde_enum!(
  PartitionDataTypeSerde,
  PartitionDataType,
  {Bool, Int64, String, TimestampMinute}
);

impl_serde_enum!(
  SchemaModeSerde,
  SchemaMode,
  {FailIfExists, OkIfExact, AddNewColumns}
);

impl Default for SchemaModeSerde {
  fn default() -> Self {
    Self::FailIfExists
  }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnMetaSerde {
  pub dtype: DataTypeSerde,
  #[serde(default)]
  pub nested_list_depth: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionMetaSerde {
  pub dtype: PartitionDataTypeSerde,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaSerde {
  #[serde(default)]
  pub partitioning: HashMap<String, PartitionMetaSerde>,
  pub columns: HashMap<String, ColumnMetaSerde>,
}

impl TryFrom<&Schema> for SchemaSerde {
  type Error = ServerError;

  fn try_from(schema: &Schema) -> ServerResult<Self> {
    let mut partitioning = HashMap::new();
    let mut columns = HashMap::new();
    for (k, v) in &schema.partitioning {
      partitioning.insert(k.to_string(), PartitionMetaSerde {
        dtype: v.dtype.try_into()?
      });
    }
    for (k, v) in &schema.columns {
      columns.insert(k.to_string(), ColumnMetaSerde {
        dtype: v.dtype.try_into()?,
        nested_list_depth: v.nested_list_depth,
      });
    }
    Ok(SchemaSerde { partitioning, columns })
  }
}

impl From<&SchemaSerde> for Schema {
  fn from(schema: &SchemaSerde) -> Self {
    Schema {
      partitioning: schema.partitioning.iter()
        .map(|(k, v)| (k.to_string(), PartitionMeta {
          dtype: v.dtype.into()
        }))
        .collect(),
      columns: schema.columns.iter()
        .map(|(k, v)| (k.to_string(), ColumnMeta {
          dtype: v.dtype.into(),
          nested_list_depth: v.nested_list_depth,
        }))
        .collect(),
    }
  }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTableRequestSerde {
  pub table_name: String,
  pub schema: SchemaSerde,
  #[serde(default)]
  pub mode: SchemaModeSerde,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTableResponseSerde {
  pub already_exists: bool,
  pub columns_added: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DropTableRequestSerde {
  pub table_name: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableInfoSerde {
  pub table_name: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTablesResponseSerde {
  pub tables: Vec<TableInfoSerde>,
}
