use std::collections::HashMap;
use std::time::SystemTime;

use async_trait::async_trait;
use chrono::DateTime;
use pancake_db_idl::dml::{FieldValue, PartitionFieldValue, RepeatedFieldValue, Row, WriteToPartitionRequest};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dml::partition_field_value::Value as PartitionValue;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use pancake_db_idl::schema::{ColumnMeta, PartitionMeta};
use prost_types::Timestamp;
use serde_json::{Number, Value as JsonValue};

use crate::{Server, ServerResult};
use crate::errors::ServerError;
use crate::locks::partition::PartitionWriteLocks;
use crate::locks::table::GlobalTableReadLocks;
use crate::ops::traits::{RestRoute, ServerOp};
use crate::ops::write_to_partition::WriteToPartitionOp;
use crate::serde_models::{EmptySerde, WriteToPartitionRequestSerde};
use crate::types::{NormalizedPartition, PartitionKey};

pub struct WriteToPartitionRestOp {
  pub req: WriteToPartitionRequestSerde,
}

#[async_trait]
impl ServerOp for WriteToPartitionRestOp {
  type Locks = GlobalTableReadLocks;
  type Response = EmptySerde;

  fn get_key(&self) -> ServerResult<String> {
    Ok(self.req.table_name.to_string())
  }

  async fn execute_with_locks(&self, server: &Server, locks: GlobalTableReadLocks) -> ServerResult<Self::Response> {
    let schema = locks.table_meta.schema();
    let partition = self.pb_partition(&schema.partitioning)?;
    let rows = self.pb_rows(&schema.columns)?;

    let partition_key = PartitionKey {
      table_name: self.req.table_name.to_string(),
      partition: NormalizedPartition::from_raw_fields(&partition)?,
    };

    let partition_write_locks = PartitionWriteLocks::from_table_read(
      locks,
      &partition_key,
      server,
    ).await?;

    let req: WriteToPartitionRequest = WriteToPartitionRequest {
      table_name: self.req.table_name.to_string(),
      partition,
      rows,
    };
    WriteToPartitionOp { req }.execute_with_locks(server, partition_write_locks).await?;
    Ok(EmptySerde {})
  }
}

impl WriteToPartitionRestOp {
  fn pb_partition(&self, partitioning: &HashMap<String, PartitionMeta>) -> ServerResult<HashMap<String, PartitionFieldValue>> {
    let mut partition = HashMap::new();

    for (partition_name, partition_field) in &self.req.partition {
      let dtype = match partitioning.get(partition_name) {
        Some(meta) => PartitionDataType::from_i32(meta.dtype)
          .ok_or(ServerError::internal("unknown dtype")),
        None => Err(ServerError::invalid(format!(
          "partition column {} does not exist",
          partition_name,
        ))),
      }?;
      partition.insert(partition_name.to_string(), parse_partition_field_value(partition_field, dtype)?);
    }
    Ok(partition)

  }

  fn pb_rows(&self, col_metas: &HashMap<String, ColumnMeta>) -> ServerResult<Vec<Row>> {
    let mut rows = Vec::new();
    for row in &self.req.rows {
      let mut pb_row: Row = Row::default();
      for (col_name, value) in row {
        let col_meta = col_metas.get(col_name)
          .ok_or(ServerError::invalid(format!(
            "column {} does not exist",
            col_name,
          )))?;

        let dtype = DataType::from_i32(col_meta.dtype)
          .ok_or(ServerError::internal("unknown dtype"))?;

        let field_val = parse_field_value(value, dtype)?;
        pb_row.fields.insert(col_name.to_string(), field_val);
      }
      rows.push(pb_row);
    }
    Ok(rows)
  }
}

fn parse_timestamp(s: &str) -> ServerResult<Timestamp> {
  let chrono_t = DateTime::parse_from_rfc3339(s)?;
  Ok(Timestamp::from(SystemTime::from(chrono_t)))
}

fn number_to_i64(n: &Number) -> ServerResult<i64> {
  match n.as_i64() {
    Some(n_i64) => Ok(n_i64),
    None => Err(ServerError::invalid("cannot use floats for int64 data type")),
  }
}

fn parse_partition_field_value(json_value: &JsonValue, dtype: PartitionDataType) -> ServerResult<PartitionFieldValue> {
  let value = match (json_value, dtype) {
    (JsonValue::String(s), PartitionDataType::String) => Ok(PartitionValue::StringVal(s.to_string())),
    (JsonValue::String(s), PartitionDataType::TimestampMinute) => Ok(PartitionValue::TimestampVal(parse_timestamp(s)?)),
    (JsonValue::Number(n), PartitionDataType::Int64) => Ok(PartitionValue::Int64Val(number_to_i64(n)?)),
    (JsonValue::Bool(b), PartitionDataType::Bool) => Ok(PartitionValue::BoolVal(*b)),
    _ => Err(ServerError::invalid(format!(
      "unsupported type for partition field value: {} for {:?}",
      json_value,
      dtype,
    )))
  }?;

  Ok(PartitionFieldValue { value: Some(value) })
}

fn parse_field_value(json_value: &JsonValue, dtype: DataType) -> ServerResult<FieldValue> {
  if json_value.is_null() {
    return Ok(FieldValue::default());
  }

  let value = match (json_value, dtype) {
    (JsonValue::String(s), DataType::String) => Ok(Value::StringVal(s.to_string())),
    (JsonValue::String(s), DataType::TimestampMicros) => Ok(Value::TimestampVal(parse_timestamp(s)?)),
    (JsonValue::String(s), DataType::Bytes) => {
      let bytes = base64::decode(s).map_err(|_|
        ServerError::invalid(format!(
          "JSON string cannot be parsed as base 64 bytes: {}",
          s,
        ))
      )?;
      Ok(Value::BytesVal(bytes))
    },
    (JsonValue::Number(n), DataType::Int64) => Ok(Value::Int64Val(number_to_i64(n)?)),
    (JsonValue::Number(n), DataType::Float32) => Ok(Value::Float32Val(n.as_f64().unwrap() as f32)),
    (JsonValue::Number(n), DataType::Float64) => Ok(Value::Float64Val(n.as_f64().unwrap() as f64)),
    (JsonValue::Bool(b), DataType::Bool) => Ok(Value::BoolVal(*b)),
    (JsonValue::Array(a), _) => {
      let mut repeated_value: RepeatedFieldValue = RepeatedFieldValue::default();
      for value in a {
        let sub_field = parse_field_value(value, dtype)?;
        repeated_value.vals.push(sub_field);
      }
      Ok(Value::ListVal(repeated_value))
    },
    _ => Err(ServerError::invalid(format!(
      "Unsupported type for field value: {} for {:?}",
      json_value,
      dtype,
    ))),
  }?;
  Ok(FieldValue { value: Some(value) })
}

impl RestRoute for WriteToPartitionRestOp {
  type Req = WriteToPartitionRequestSerde;

  const ROUTE_NAME: &'static str = "write_to_partition";

  fn new_op(req: Self::Req) -> WriteToPartitionRestOp {
    WriteToPartitionRestOp { req }
  }
}
