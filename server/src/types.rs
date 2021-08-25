use std::collections::HashMap;
use std::path::PathBuf;

use crate::errors::{ServerError, ServerResult};
use pancake_db_idl::dml::partition_field::Value;
use pancake_db_idl::dml::PartitionField;
use pancake_db_idl::schema::Schema;
use serde::{Deserialize, Serialize};

use crate::utils;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum PartitionValue {
  STRING(String),
  INT64(i64),
  BOOL(bool),
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NormalizedPartitionField {
  pub name: String,
  pub value: PartitionValue,
}

impl NormalizedPartitionField {
  pub fn to_path_buf(&self) -> PathBuf {
    let value_str = match &self.value {
      PartitionValue::STRING(x) => x.clone(),
      PartitionValue::INT64(x) => x.to_string(),
      PartitionValue::BOOL(x) => if *x {"true"} else {"false"}.to_string()
    };
    PathBuf::from(format!("{}={}", self.name, value_str))
  }
}

impl From<&PartitionField> for NormalizedPartitionField {
  fn from(raw_field: &PartitionField) -> NormalizedPartitionField {
    let value = match raw_field.value.as_ref().expect("raw partition field is missing value") {
      Value::string_val(x) => PartitionValue::STRING(x.clone()),
      Value::int64_val(x) => PartitionValue::INT64(*x),
      Value::bool_val(x) => PartitionValue::BOOL(*x),
    };
    NormalizedPartitionField {
      name: raw_field.name.clone(),
      value,
    }
  }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NormalizedPartition {
  pub fields: Vec<NormalizedPartitionField>
}

impl NormalizedPartition {
  pub fn to_path_buf(&self) -> PathBuf {
    self.fields.iter().map(|f| f.to_path_buf()).collect()
  }

  pub fn partial(raw_fields: &[PartitionField]) -> ServerResult<NormalizedPartition> {
    let mut fields = Vec::new();
    for raw_field in raw_fields {
      if raw_field.value.is_none() {
        return Err(ServerError::invalid("partition field has no value"));
      }
      fields.push(NormalizedPartitionField::from(raw_field));
    }
    Ok(NormalizedPartition { fields })
  }

  pub fn full(
    schema: &Schema,
    raw_fields: &[PartitionField]
  ) -> ServerResult<NormalizedPartition> {
    if schema.partitioning.len() != raw_fields.len() {
      return Err(ServerError::invalid("number of partition fields does not match schema"));
    }

    let mut raw_field_map = HashMap::new();
    for raw_field in raw_fields {
      raw_field_map.insert(&raw_field.name, raw_field);
    }
    let mut fields = Vec::new();
    for meta in &schema.partitioning {
      let maybe_raw_field = raw_field_map.get(&meta.name);
      if maybe_raw_field.is_none() {
        return Err(ServerError::invalid("partition field is missing"));
      }
      let raw_field = *maybe_raw_field.unwrap();
      if !utils::partition_dtype_matches_field(
        &meta.dtype.unwrap(),
        &raw_field
      ) {
        return Err(ServerError::invalid("partition field dtype does not match schema"));
      }
      fields.push(NormalizedPartitionField::from(raw_field));
    }

    Ok(NormalizedPartition {
      fields
    })
  }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct PartitionKey {
  pub table_name: String,
  pub partition: NormalizedPartition,
}

impl PartitionKey {
  pub fn segment_key(&self, segment_id: String) -> SegmentKey {
    SegmentKey {
      table_name: self.table_name.clone(),
      partition: self.partition.clone(),
      segment_id,
    }
  }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct SegmentKey {
  pub table_name: String,
  pub partition: NormalizedPartition,
  pub segment_id: String,
}

impl SegmentKey {
  pub fn partition_key(&self) -> PartitionKey {
    PartitionKey {
      table_name: self.table_name.clone(),
      partition: self.partition.clone(),
    }
  }

  pub fn compaction_key(&self, version: u64) -> CompactionKey {
    CompactionKey {
      table_name: self.table_name.clone(),
      partition: self.partition.clone(),
      segment_id: self.segment_id.clone(),
      version,
    }
  }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct CompactionKey {
  pub table_name: String,
  pub partition: NormalizedPartition,
  pub segment_id: String,
  pub version: u64,
}

impl CompactionKey {
  pub fn segment_key(&self) -> SegmentKey {
    SegmentKey {
      table_name: self.table_name.clone(),
      partition: self.partition.clone(),
      segment_id: self.segment_id.clone(),
    }
  }
}
