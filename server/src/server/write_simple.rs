use std::convert::Infallible;

use hyper::body::Bytes;
use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse};
use warp::{Filter, Rejection, Reply};

use crate::errors::{ServerError, ServerResult};
use crate::ops::traits::ServerOp;
use crate::ops::write_to_partition::WriteToPartitionOp;
use crate::utils::common;
use base64::decode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use super::Server;

const ROUTE_NAME: &str = "write_to_partition_simple";

#[derive(Serialize, Deserialize)]
struct WriteToPartitionSimpleRequest {
  table_name: String,
  partition: HashMap<String, Value>,
  rows: Vec<HashMap<String, Value>>,
}

fn parse_timestamp(value: &Value) -> ServerResult<protobuf::well_known_types::Timestamp> {
  let value_str = value.to_string();
  let timestamp =
    protobuf::json::parse_from_str::<protobuf::well_known_types::Timestamp>(&value_str)
      .map_err(|_| ServerError::invalid("invalid timestamp"))?;
  Ok(timestamp)
}

fn parse_field_value(field_value: &Value) -> ServerResult<pancake_db_idl::dml::FieldValue> {
  let mut value_pb = pancake_db_idl::dml::FieldValue::new();
  match field_value {
    Value::String(s) => {
      value_pb.set_string_val(s.clone());
      Ok(value_pb)
    },
    Value::Number(n) if n.is_i64() => {
      value_pb.set_int64_val(n.as_i64().unwrap());
      Ok(value_pb)
    },
    Value::Number(n) if n.is_f64() => {
      value_pb.set_float64_val(n.as_f64().unwrap());
      Ok(value_pb)
    },
    Value::Bool(b) => {
      value_pb.set_bool_val(*b);
      Ok(value_pb)
    },
    Value::Object(o) => {
      if o.contains_key("timestamp") {
        let value = o.get("timestamp").unwrap();
        let timestamp = parse_timestamp(value)?;
        value_pb.set_timestamp_val(timestamp);
        Ok(value_pb)
      } else if o.contains_key("bytes") {
        let value = o.get("bytes").unwrap();
        let bytes_str = value.as_str().unwrap();
        let bytes = decode(bytes_str).unwrap();
        value_pb.set_bytes_val(bytes);
        Ok(value_pb)
      } else {
        Err(ServerError::invalid("unsupported object"))
      }
    },
    Value::Array(a) => {
      let mut repeated_value_pb = pancake_db_idl::dml::RepeatedFieldValue::new();
      for value in a {
        let sub_field = parse_field_value(value)?;
        repeated_value_pb.vals.push(sub_field);
      }
      let mut value_pb = pancake_db_idl::dml::FieldValue::new();
      value_pb.set_list_val(repeated_value_pb);
      Ok(value_pb)
    },
    _ => Err(ServerError::invalid(&format!(
      "Unsupported type for field value: {}",
      field_value
    ))),
  }
}

pub fn parse_pb_from_simple_json(body: Bytes) -> ServerResult<WriteToPartitionRequest> {
  let body_string = String::from_utf8(body.to_vec())
    .map_err(|_| ServerError::invalid("body bytes do not parse to string"))?;
  let req_simple: WriteToPartitionSimpleRequest = serde_json::from_str(&body_string)
    .map_err(|_| ServerError::invalid("body string does not parse to json"))?;
  let mut pb_req = pancake_db_idl::dml::WriteToPartitionRequest::new();
  pb_req.table_name = req_simple.table_name;
  for (_name, partition_field) in req_simple.partition.iter() {
    let mut pb_partition_field = pancake_db_idl::dml::PartitionField::new();
    match partition_field {
      Value::String(s) => {
        pb_partition_field.set_string_val(s.clone());
      },
      Value::Number(n) => {
        if n.is_i64() {
          pb_partition_field.set_int64_val(n.as_i64().unwrap());
        } else {
          return Err(ServerError::invalid(
            "numeric partition field is not an integer",
          ));
        }
      },
      Value::Bool(b) => {
        pb_partition_field.set_bool_val(*b);
      },
      Value::Object(o) => {
        if o.contains_key("timestamp") {
          let value = o.get("timestamp").unwrap();
          let timestamp = parse_timestamp(value)?;
          pb_partition_field.set_timestamp_val(timestamp);
        } else {
          return Err(ServerError::invalid(
            "object partition field is not a timestamp",
          ));
        }
      },
      _ => {
        return Err(ServerError::invalid(
          "partition field does not have correct type",
        ))
      }
    }
    pb_req.partition.push(pb_partition_field);
  }

  for row in req_simple.rows.iter() {
    let mut pb_row = pancake_db_idl::dml::Row::new();
    for (column, value) in row.iter() {
      let mut pb_field = pancake_db_idl::dml::Field::new();
      pb_field.name = column.to_string();
      let field_val = parse_field_value(value)?;
      pb_field.value = protobuf::MessageField::from_option(Some(field_val));
      pb_row.fields.push(pb_field);
    }
    pb_req.rows.push(pb_row);
  }
  Ok(pb_req)
}

impl Server {
  pub async fn write_to_partition_simple(
    &self,
    req: WriteToPartitionRequest,
  ) -> ServerResult<WriteToPartitionResponse> {
    WriteToPartitionOp { req }.execute(self).await
  }

  pub fn write_to_partition_simple_filter(
  ) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path(ROUTE_NAME))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::warp_write_to_partition_simple)
  }

  async fn write_to_partition_simple_from_bytes(
    &self,
    body: Bytes,
  ) -> ServerResult<WriteToPartitionResponse> {
    let req = parse_pb_from_simple_json(body)?;
    self.write_to_partition_simple(req).await
  }

  async fn warp_write_to_partition_simple(
    server: Server,
    body: Bytes,
  ) -> Result<impl Reply, Infallible> {
    Self::log_request(ROUTE_NAME, &body);
    common::pancake_result_into_warp(
      server.write_to_partition_simple_from_bytes(body).await,
      ROUTE_NAME,
    )
  }
}
