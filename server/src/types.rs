use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use pancake_db_idl::dml::partition_field::Value;
use pancake_db_idl::dml::PartitionField;
use pancake_db_idl::schema::Schema;
use protobuf::well_known_types::Timestamp;
use serde::{Deserialize, Serialize};

use crate::errors::{ServerError, ServerResult};
use crate::utils;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct PartitionMinute {
  pub minutes: i64,
}

impl TryFrom<&Timestamp> for PartitionMinute {
  type Error = ServerError;

  fn try_from(t: &Timestamp) -> ServerResult<PartitionMinute> {
    if t.nanos != 0 {
      Err(ServerError::invalid(&format!(
        "whole minute expected but {} nanoseconds found in timestamp",
        t.nanos,
      )))
    } else if t.seconds % 60 != 0 {
      Err(ServerError::invalid(&format!(
        "whole minute expected but {} extra seconds found in timestamp",
        t.seconds,
      )))
    } else {
      Ok(PartitionMinute {
        minutes: t.seconds.div_euclid(60)
      })
    }
  }
}

// we use our own type instead of idl partition_field.Value so we can
// have Hash, among other things
#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum PartitionValue {
  STRING(String),
  INT64(i64),
  BOOL(bool),
  MINUTE(PartitionMinute),
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NormalizedPartitionField {
  pub name: String,
  pub value: PartitionValue,
}

impl Display for NormalizedPartitionField {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let value_str = match &self.value {
      PartitionValue::STRING(x) => x.clone(),
      PartitionValue::INT64(x) => x.to_string(),
      PartitionValue::BOOL(x) => if *x {"true"} else {"false"}.to_string(),
      PartitionValue::MINUTE(x) => x.minutes.to_string(), // TODO
    };
    write!(
      f,
      "{}={}",
      self.name,
      value_str,
    )
  }
}

impl NormalizedPartitionField {
  pub fn to_path_buf(&self) -> PathBuf {
    PathBuf::from(self.to_string())
  }
}

impl TryFrom<&PartitionField> for NormalizedPartitionField {
  type Error = ServerError;

  fn try_from(raw_field: &PartitionField) -> ServerResult<NormalizedPartitionField> {
    let value_result: ServerResult<PartitionValue> = match raw_field.value.as_ref().expect("raw partition field is missing value") {
      Value::string_val(x) => {
        utils::validate_partition_string(x)?;
        Ok(PartitionValue::STRING(x.clone()))
      },
      Value::int64_val(x) => Ok(PartitionValue::INT64(*x)),
      Value::bool_val(x) => Ok(PartitionValue::BOOL(*x)),
      Value::timestamp_val(x) => {
        let minute = PartitionMinute::try_from(x)?;
        Ok(PartitionValue::MINUTE(minute))
      }
    };
    let value = value_result?;
    Ok(NormalizedPartitionField {
      name: raw_field.name.clone(),
      value,
    })
  }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NormalizedPartition {
  pub fields: Vec<NormalizedPartitionField>
}

impl Display for NormalizedPartition {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    for field in &self.fields {
      field.fmt(f)?;
    }
    Ok(())
  }
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
      fields.push(NormalizedPartitionField::try_from(raw_field)?);
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
      fields.push(NormalizedPartitionField::try_from(raw_field)?);
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

impl Display for PartitionKey {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{}/{}",
      self.table_name,
      self.partition,
    )
  }
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

impl Display for SegmentKey {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{}/{} segment {}",
      self.table_name,
      self.partition,
      self.segment_id
    )
  }
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
