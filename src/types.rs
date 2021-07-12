use pancake_db_idl::dml::PartitionField;
use pancake_db_idl::dml::partition_field::Value;

use serde::{Deserialize, Serialize};
use pancake_db_idl::schema::Schema;
use std::collections::HashMap;
use crate::utils;

#[derive(Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum PartitionValue {
  STRING(String),
  INT64(i64),
}

#[derive(Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NormalizedPartitionField {
  pub name: String,
  pub value: PartitionValue,
}

impl NormalizedPartitionField {
  pub fn to_string(&self) -> String {
    let value_str = match &self.value {
      PartitionValue::STRING(x) => x.clone(),
      PartitionValue::INT64(x) => x.to_string(),
    };
    format!("{}={}", self.name, value_str)
  }
}

impl From<&PartitionField> for NormalizedPartitionField {
  fn from(raw_field: &PartitionField) -> NormalizedPartitionField {
    let value = match raw_field.value.as_ref().expect("raw partition field is missing value") {
      Value::string_val(x) => PartitionValue::STRING(x.clone()),
      Value::int64_val(x) => PartitionValue::INT64(*x),
    };
    NormalizedPartitionField {
      name: raw_field.name.clone(),
      value,
    }
  }
}

#[derive(Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NormalizedPartition {
  pub fields: Vec<NormalizedPartitionField>
}

impl NormalizedPartition {
  pub fn partial(raw_fields: &[PartitionField]) -> Result<NormalizedPartition, &'static str> {
    let mut fields = Vec::new();
    for raw_field in raw_fields {
      if raw_field.value.is_none() {
        return Err("partition field has no value");
      }
      fields.push(NormalizedPartitionField::from(raw_field));
    }
    Ok(NormalizedPartition { fields })
  }

  pub fn full(
    schema: &Schema,
    raw_fields: &[PartitionField]
  ) -> Result<NormalizedPartition, &'static str> {
    if schema.partitioning.len() != raw_fields.len() {
      return Err("number of partition fields does not match schema");
    }

    let mut raw_field_map = HashMap::new();
    for raw_field in raw_fields {
      raw_field_map.insert(&raw_field.name, raw_field);
    }
    let mut fields = Vec::new();
    for meta in &schema.partitioning {
      let maybe_raw_field = raw_field_map.get(&meta.name);
      if maybe_raw_field.is_none() {
        return Err("partition field is missing");
      }
      let raw_field = *maybe_raw_field.unwrap();
      if !utils::partition_dtype_matches_field(
        &meta.dtype.unwrap(),
        &raw_field
      ) {
        return Err("partition field dtype does not match schema");
      }
      fields.push(NormalizedPartitionField::from(raw_field));
    }

    Ok(NormalizedPartition {
      fields
    })
  }
}

#[derive(Hash, PartialEq, Eq, Clone)]
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

#[derive(Hash, PartialEq, Eq, Clone)]
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

#[derive(Hash, PartialEq, Eq, Clone)]
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

