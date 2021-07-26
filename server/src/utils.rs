use std::convert::Infallible;
use std::fmt::Debug;
use std::io::ErrorKind;
use std::path::Path;

use pancake_db_core::errors::{PancakeError, PancakeErrorKind, PancakeResult};
use pancake_db_idl::dml::{partition_field, partition_filter, PartitionFilter, FieldValue};
use pancake_db_idl::dml::{Field, PartitionField};
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use serde::Serialize;
use tokio::fs;
use tokio::io;
use tokio::io::AsyncWriteExt;
use warp::http::StatusCode;
use warp::Reply;

pub async fn read_if_exists(fname: impl AsRef<Path>) -> Option<Vec<u8>> {
  match fs::read(fname).await {
    Ok(bytes) => Some(bytes),
    Err(_) => None,
  }
}

pub async fn create_if_new(dir: impl AsRef<Path>) -> io::Result<()> {
  match fs::create_dir(dir).await {
    Ok(_) => Ok(()),
    Err(e) => match e.kind() {
      ErrorKind::AlreadyExists => Ok(()),
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
  match dtype {
    DataType::STRING => traverse_field_value(value, &|v: &FieldValue| v.has_string_val()),
    DataType::INT64 => traverse_field_value(value, &|v: &FieldValue| v.has_int64_val()),
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
  }
}

pub fn partition_field_from_string(
  name: &str,
  value_str: &str,
  dtype: PartitionDataType
) -> PancakeResult<PartitionField> {
  let value = match dtype {
    PartitionDataType::INT64 => {
      let parsed: Result<i64, _> = value_str.parse();
      match parsed {
        Ok(x) => Some(partition_field::Value::int64_val(x)),
        Err(_) => None
      }
    },
    PartitionDataType::STRING => Some(partition_field::Value::string_val(value_str.to_string()))
  };
  if value.is_none() {
    return Err(PancakeError::internal("failed to parse partition field value"));
  }
  Ok(PartitionField {
    name: name.to_string(),
    value,
    ..Default::default()
  })
}

pub fn satisfies_filters(partition: &[PartitionField], filters: &[PartitionFilter]) -> bool {
  for field in partition {
    for filter in filters {
      let satisfies = match &filter.value {
        Some(partition_filter::Value::equal_to(other)) => {
          field.name != other.name || field.value == other.value
        },
        None => true,
      };
      if !satisfies {
        return false;
      }
    }
  }
  true
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

pub fn pancake_result_into_warp<T: Serialize>(res: PancakeResult<T>) -> Result<Box<dyn Reply>, Infallible> {
  match res {
    Ok(x) => Ok(Box::new(warp::reply::json(&x))),
    Err(e) => {
      let reply = warp::reply::json(&ErrorResponse {
        message: e.to_string(),
      });
      let status_code = match e.kind {
        PancakeErrorKind::DoesNotExist {entity_name: _, value: _} => StatusCode::NOT_FOUND,
        PancakeErrorKind::Invalid {explanation: _} => StatusCode::BAD_REQUEST,
        PancakeErrorKind::Internal {explanation: _} => StatusCode::INTERNAL_SERVER_ERROR,
      };
      Ok(Box::new(warp::reply::with_status(
        reply,
        status_code,
      )))
    }
  }
}
