use std::convert::Infallible;

use hyper::body::Bytes;
use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse};
use warp::{Filter, Rejection, Reply};

use crate::utils::common;
use crate::errors::{ServerError, ServerResult};
use crate::ops::traits::ServerOp;
use crate::ops::write_to_partition::WriteToPartitionOp;
use std::collections::HashMap;
use std::convert::TryFrom;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use chrono::DateTime;

use super::Server;

const ROUTE_NAME: &str = "write_to_partition_simple";


#[derive(Serialize, Deserialize)]
struct WriteToPartitionSimpleRequest {
  table_name: String,
  // TODO: change the inner HashMap to <String, Value>
  partition: HashMap<String, HashMap<String, String>>,
  rows: Vec<HashMap<String, HashMap<String, Value>>>,
}

fn parse_field(column: &String, field: &HashMap<String, Value>) -> Result<pancake_db_idl::dml::Field, ServerError> {
  let mut pb_field = pancake_db_idl::dml::Field::new();
  pb_field.name = column.clone();
  let field_val = field.get("value").unwrap().as_object().unwrap();
  if field_val.contains_key("list_val") {
    let value_array = field_val.get("list_val").unwrap().as_array().unwrap();
    let mut list_val = Vec::new();
    for value in value_array {
      let map_json = value.as_object().unwrap();
      let mut map: HashMap<String, Value> = HashMap::new();
      for (key, value) in map_json {
        // TODO: avoid the clone?
        map.insert(key.to_string(), value.clone());
      }
      // TODO: is column name supposed to change
      let sub_field = parse_field(column, &map)?;
      list_val.push(sub_field);
    }
  } else if field_val.contains_key("string_val") {
    let value_string = field_val.get("string_val").unwrap().to_string();
    let mut value_pb = pancake_db_idl::dml::FieldValue::new();
    value_pb.set_string_val(value_string);
    let value: Option<pancake_db_idl::dml::FieldValue> = Some(value_pb);
    pb_field.value = protobuf::MessageField::from_option(value);
  } else if field_val.contains_key("int64_val") {
    let value_int64 = field_val.get("int64_val").unwrap().as_i64().unwrap();
    let mut value_pb = pancake_db_idl::dml::FieldValue::new();
    value_pb.set_int64_val(value_int64);
    let value: Option<pancake_db_idl::dml::FieldValue> = Some(value_pb);
    pb_field.value = protobuf::MessageField::from_option(value);
  } else if field_val.contains_key("bool_val") {
    let value_bool = field_val.get("bool_val").unwrap().as_bool().unwrap();
    let mut value_pb = pancake_db_idl::dml::FieldValue::new();
    value_pb.set_bool_val(value_bool);
    let value: Option<pancake_db_idl::dml::FieldValue> = Some(value_pb);
    pb_field.value = protobuf::MessageField::from_option(value);
  } else if field_val.contains_key("timestamp_val") {
    let timestamp_string = field_val.get("timestamp_val").unwrap().to_string();
    let rfc3339 = DateTime::parse_from_rfc3339(&timestamp_string).unwrap();
    let mut value_pb = pancake_db_idl::dml::FieldValue::new();
    let mut timestamp = protobuf::well_known_types::Timestamp::new();
    timestamp.seconds = rfc3339.timestamp();
    timestamp.nanos = i32::try_from(rfc3339.timestamp_subsec_nanos()).ok().unwrap();
    value_pb.set_timestamp_val(timestamp);
    let value: Option<pancake_db_idl::dml::FieldValue> = Some(value_pb);
    pb_field.value = protobuf::MessageField::from_option(value);
  } else if field_val.contains_key("bytes_val") {
    let value_array = field_val.get("bytes_val").unwrap().as_array().unwrap();
    let value_bytes: Vec<u8> = value_array.iter().map(|x| x.as_u64().unwrap() as u8).collect();
    let mut value_pb = pancake_db_idl::dml::FieldValue::new();
    value_pb.set_bytes_val(value_bytes.to_vec());
} else if field_val.contains_key("float64_val") {
    let value_float64 = field_val.get("float64_val").unwrap().as_f64().unwrap();
    let mut value_pb = pancake_db_idl::dml::FieldValue::new();
    value_pb.set_float64_val(value_float64);
    let value: Option<pancake_db_idl::dml::FieldValue> = Some(value_pb);
    pb_field.value = protobuf::MessageField::from_option(value);
  } else {
    return Err(ServerError::invalid("field does not have correct type"));
  }
  Ok(pb_field)
}

pub fn parse_pb_from_simple_json(body: Bytes) -> ServerResult<WriteToPartitionRequest> {
  let body_string = String::from_utf8(body.to_vec()).map_err(|_|
    ServerError::invalid("body bytes do not parse to string")
  )?;
  let req_simple: WriteToPartitionSimpleRequest = serde_json::from_str(&body_string).map_err(|_|
    ServerError::invalid("body string does not parse to json")
  )?;
  let mut req = protobuf::json::parse_from_str::<WriteToPartitionRequest>(&body_string).map_err(|_|
    ServerError::invalid("body string does not parse to correct request format")
  )?;
  req.table_name = req_simple.table_name;
  for (name, partition_field) in req_simple.partition.iter() {
    let mut pb_partition_field = pancake_db_idl::dml::PartitionField::new();
    pb_partition_field.name = name.to_string();
    // TODO: replace the unwraps with better error handling
    let data_type = partition_field.get("data_type").unwrap().to_string();
    if data_type == "string" {
      pb_partition_field.set_string_val(partition_field.get("value").unwrap().to_string());
    } else if data_type == "int64" {
      pb_partition_field.set_int64_val(partition_field.get("value").unwrap().parse::<i64>().unwrap());
    } else if data_type == "bool" {
      pb_partition_field.set_bool_val(partition_field.get("value").unwrap().parse::<bool>().unwrap());
    } else if data_type == "timestamp" {
      let timestamp_string = partition_field.get("value").unwrap().to_string();
      let rfc3339 = DateTime::parse_from_rfc3339(&timestamp_string).unwrap();
      let mut timestamp = protobuf::well_known_types::Timestamp::new();
      timestamp.seconds = rfc3339.timestamp();
      timestamp.nanos = i32::try_from(rfc3339.timestamp_subsec_nanos()).ok().unwrap();
      pb_partition_field.set_timestamp_val(timestamp);
    } else {
      return Err(ServerError::invalid("partition field does not have correct data_type"));
    }
    req.partition.push(pb_partition_field);
  }

  for row in req_simple.rows.iter() {
    let mut pb_row = pancake_db_idl::dml::Row::new();
    for (column, value) in row.iter() {
      let pb_field = parse_field(column, value)?;
      pb_row.fields.push(pb_field);
    }
  }
  Ok(req)
}

impl Server {
  pub async fn write_to_partition_simple(&self, req: WriteToPartitionRequest) -> ServerResult<WriteToPartitionResponse> {
    WriteToPartitionOp { req }.execute(&self).await
  }

  pub fn write_to_partition_simple_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path(ROUTE_NAME))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::warp_write_to_partition_simple)
  }

  async fn write_to_partition_simple_from_bytes(&self, body: Bytes) -> ServerResult<WriteToPartitionResponse> {
    let req = parse_pb_from_simple_json(body)?;
    self.write_to_partition_simple(req).await
  }

  async fn warp_write_to_partition_simple(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    Self::log_request(ROUTE_NAME, &body);
    common::pancake_result_into_warp(
      server.write_to_partition_simple_from_bytes(body).await,
      ROUTE_NAME,
    )
  }
}
