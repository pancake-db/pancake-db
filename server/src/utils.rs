use std::cmp::Ordering;
use std::convert::Infallible;
use std::fmt::Debug;
use std::io::{ErrorKind, SeekFrom};
use std::path::Path;

use hyper::body::Bytes;
use pancake_db_idl::dml::{FieldValue, partition_filter, PartitionFilter};
use pancake_db_idl::dml::{Field, PartitionField};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dml::partition_field::Value as PartitionValue;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use protobuf::{Message, ProtobufEnumOrUnknown};
use serde::Serialize;
use tokio::fs;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use warp::http::Response;
use warp::Reply;

use crate::errors::{ServerError, ServerResult};
use crate::constants::LIST_LENGTH_BYTES;

pub async fn file_exists(fname: impl AsRef<Path>) -> io::Result<bool> {
  match fs::File::open(fname).await {
    Ok(_) => Ok(true),
    Err(e) => {
      match e.kind() {
        io::ErrorKind::NotFound => Ok(false),
        _ => Err(e)
      }
    }
  }
}

pub async fn read_with_offset(fname: impl AsRef<Path>, offset: u64, bytes: usize) -> io::Result<Vec<u8>> {
  // return completed: bool, and bytes if any
  let mut maybe_file = fs::File::open(fname).await
    .map(Some)
    .or_else(|e| match e.kind() {
      io::ErrorKind::NotFound => Ok(None),
      _ => Err(e),
    })?;

  if maybe_file.is_none() {
    return Ok(Vec::new());
  }

  let file = maybe_file.as_mut().unwrap();
  file.seek(SeekFrom::Start(offset)).await?;

  let mut res = vec![0_u8].repeat(bytes);
  let mut count = 0;
  while count < bytes {
    match file.read(&mut res[count..]).await {
      Ok(0) => break,
      Ok(n) => {
        count += n;
      }
      Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
      Err(e) => return Err(e),
    }
  }
  if count < bytes {
    Ok(res[..count].to_vec())
  } else {
    Ok(res)
  }
}

// returns whether it already exists
pub async fn create_if_new(dir: impl AsRef<Path>) -> io::Result<bool> {
  match fs::create_dir(dir).await {
    Ok(_) => Ok(false),
    Err(e) => match e.kind() {
      ErrorKind::AlreadyExists => Ok(true),
      _ => Err(e),
    },
  }
}

pub async fn overwrite_file(path: impl AsRef<Path> + Debug, contents: &[u8]) -> io::Result<()> {
  let mut file = fs::File::create(path).await?;
  file.write_all(contents).await
}

pub async fn append_to_file(path: impl AsRef<Path> + Debug, contents: &[u8]) -> io::Result<()> {
  let mut file = fs::OpenOptions::new()
    .append(true)
    .create(true)
    .open(path)
    .await?;
  Ok(file.write_all(contents).await?)
}

pub fn dtype_matches_field(dtype: &DataType, field: &Field) -> bool {
  let value = field.value.get_ref();
  if value.value.is_none() {
    return true;
  }
  match dtype {
    DataType::STRING => traverse_field_value(value, &|v: &FieldValue| v.has_string_val()),
    DataType::INT64 => traverse_field_value(value, &|v: &FieldValue| v.has_int64_val()),
    DataType::BOOL => traverse_field_value(value, &|v: &FieldValue| v.has_bool_val()),
    DataType::BYTES => traverse_field_value(value, &|v: &FieldValue| v.has_bytes_val()),
    DataType::FLOAT64 => traverse_field_value(value, &|v: &FieldValue| v.has_float64_val()),
  }
}

fn traverse_field_value(value: &FieldValue, f: &dyn Fn(&FieldValue) -> bool) -> bool {
  if value.has_list_val() {
    for v in &value.get_list_val().vals {
      if !f(v) {
        return false;
      }
    }
    true
  } else {
    f(value)
  }
}

pub fn partition_dtype_matches_field(dtype: &PartitionDataType, field: &PartitionField) -> bool {
  match dtype {
    PartitionDataType::STRING => field.has_string_val(),
    PartitionDataType::INT64 => field.has_int64_val(),
    PartitionDataType::BOOL => field.has_bool_val(),
  }
}

pub fn partition_field_from_string(
  name: &str,
  value_str: &str,
  dtype: PartitionDataType
) -> ServerResult<PartitionField> {
  let value = match dtype {
    PartitionDataType::INT64 => {
      let parsed: Result<i64, _> = value_str.parse();
      match parsed {
        Ok(x) => Some(PartitionValue::int64_val(x)),
        Err(_) => None
      }
    },
    PartitionDataType::STRING => Some(PartitionValue::string_val(value_str.to_string())),
    PartitionDataType::BOOL => {
      if value_str == "true" {
        Some(PartitionValue::bool_val(true))
      } else if value_str == "false" {
        Some(PartitionValue::bool_val(false))
      } else {
        None
      }
    }
  };
  if value.is_none() {
    return Err(ServerError::internal("failed to parse partition field value"));
  }
  Ok(PartitionField {
    name: name.to_string(),
    value,
    ..Default::default()
  })
}

fn cmp_partition_field_values(v0: &PartitionValue, v1: &PartitionValue) -> ServerResult<Ordering> {
  match (v0, v1) {
    (PartitionValue::bool_val(x0), PartitionValue::bool_val(x1)) => Ok(x0.cmp(x1)),
    (PartitionValue::string_val(x0), PartitionValue::string_val(x1)) => Ok(x0.cmp(x1)),
    (PartitionValue::int64_val(x0), PartitionValue::int64_val(x1)) => Ok(x0.cmp(x1)),
    _ => Err(ServerError::invalid(&format!(
      "partition filter value {:?} does not match data type of actual value {:?}",
      v0,
      v1,
    )))
  }
}

fn field_satisfies_basic_filter(field: &PartitionField, criterion: &PartitionField, satisfies: &dyn Fn(Ordering) -> bool) -> ServerResult<bool> {
  if field.name != criterion.name {
    Ok(true)
  } else {
    let ordering = cmp_partition_field_values(
      field.value.as_ref().unwrap(),
      criterion.value.as_ref().unwrap()
    )?;
    Ok(satisfies(ordering))
  }
}

pub fn satisfies_filters(partition: &[PartitionField], filters: &[PartitionFilter]) -> ServerResult<bool> {
  for field in partition {
    for filter in filters {
      let satisfies = match &filter.value {
        Some(partition_filter::Value::equal_to(other)) => field_satisfies_basic_filter(
          field,
          other,
          &|ordering| matches!(ordering, Ordering::Equal),
        ),
        Some(partition_filter::Value::less_than(other)) => field_satisfies_basic_filter(
          field,
          other,
          &|ordering| matches!(ordering, Ordering::Less),
        ),
        Some(partition_filter::Value::less_or_eq_to(other)) => field_satisfies_basic_filter(
          field,
          other,
          &|ordering| !matches!(ordering, Ordering::Greater),
        ),
        Some(partition_filter::Value::greater_than(other)) => field_satisfies_basic_filter(
          field,
          other,
          &|ordering| matches!(ordering, Ordering::Greater),
        ),
        Some(partition_filter::Value::greater_or_eq_to(other)) => field_satisfies_basic_filter(
          field,
          other,
          &|ordering| !matches!(ordering, Ordering::Less),
        ),
        None => Ok(true),
      }?;
      if !satisfies {
        return Ok(false);
      }
    }
  }
  Ok(true)
}

pub async fn read_or_empty(path: &Path) -> io::Result<Vec<u8>> {
  match fs::read(path).await {
    Ok(bytes) => Ok(bytes),
    Err(e) => match e.kind() {
      ErrorKind::NotFound => Ok(Vec::new()),
      _ => Err(e),
    },
  }
}

#[derive(Serialize)]
struct ErrorResponse {
  pub message: String,
}

pub fn parse_pb<T: protobuf::Message>(body: Bytes) -> ServerResult<T> {
  let body_string = String::from_utf8(body.to_vec()).map_err(|_|
    ServerError::invalid("body bytes do not parse to string")
  )?;
  let req = protobuf::json::parse_from_str::<T>(&body_string).map_err(|_|
    ServerError::invalid("body string does not parse to correct request format")
  )?;
  Ok(req)
}

pub fn pancake_result_into_warp<T: Message>(res: ServerResult<T>) -> Result<Box<dyn Reply>, Infallible> {
  let options = protobuf::json::PrintOptions {
    always_output_default_values: true,
    ..Default::default()
  };
  let body_res = res.and_then(|pb|
    protobuf::json::print_to_string_with_options(&pb, &options)
      .map_err(|_| ServerError::internal("unable to write response as json"))
  );
  match body_res {
    Ok(body) => {
      Ok(Box::new(Response::new(body)))
    },
    Err(e) => {
      let reply = warp::reply::json(&ErrorResponse {
        message: e.to_string(),
      });
      Ok(Box::new(warp::reply::with_status(
        reply,
        e.kind.warp_status_code(),
      )))
    }
  }
}

pub fn unwrap_dtype(dtype: ProtobufEnumOrUnknown<DataType>) -> ServerResult<DataType> {
  dtype.enum_value()
    .map_err(|enum_code|
      ServerError::internal(&format!("unknown data type code {}", enum_code))
    )
}

pub fn byte_size_of_field(value: &FieldValue) -> usize {
  value.value.as_ref().map(|v| match v {
    Value::int64_val(_) => 8,
    Value::string_val(x) => LIST_LENGTH_BYTES + x.len(),
    Value::bool_val(_) => 1,
    Value::bytes_val(x) => LIST_LENGTH_BYTES + x.len(),
    Value::float64_val(_) => 8,
    Value::list_val(x) => {
      let mut res = 2;
      for v in &x.vals {
        res += byte_size_of_field(v);
      }
      res
    }
  }).unwrap_or(1)
}
