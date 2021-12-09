use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::convert::TryInto;
use std::fmt::Debug;
use std::io::{ErrorKind, SeekFrom};
use std::path::Path;
use std::str::FromStr;

use hyper::body::Bytes;
use pancake_db_idl::dml::{FieldValue, partition_filter, PartitionFilter, Row};
use pancake_db_idl::dml::{Field, PartitionField};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dml::partition_field::Value as PartitionValue;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use pancake_db_idl::schema::Schema;
use protobuf::{Message, ProtobufEnumOrUnknown};
use protobuf::well_known_types::Timestamp;
use serde::Serialize;
use tokio::fs;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;
use warp::http::Response;
use warp::Reply;

use crate::constants::{LIST_LENGTH_BYTES, MAX_FIELD_BYTE_SIZE, MAX_NAME_LENGTH};
use crate::errors::{ServerError, ServerResult};
use crate::storage::{Metadata, MetadataKey};
use crate::storage::compaction::Compaction;
use crate::storage::segment::SegmentMetadata;
use crate::types::{NormalizedPartitionField, NormalizedPartitionValue};

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

pub async fn file_nonempty(fname: impl AsRef<Path>) -> io::Result<bool> {
  match fs::File::open(fname).await {
    Ok(mut f) => {
      let mut buf = vec![0_u8];
      let bytes_read = f.read(&mut buf).await?;
      Ok(bytes_read > 0)
    },
    Err(e) if matches!(e.kind(), io::ErrorKind::NotFound) => Ok(false),
    Err(e) => Err(e),
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
  let checker = match dtype {
    DataType::STRING => |v: &FieldValue| v.has_string_val(),
    DataType::INT64 =>  |v: &FieldValue| v.has_int64_val(),
    DataType::BOOL => |v: &FieldValue| v.has_bool_val(),
    DataType::BYTES =>  |v: &FieldValue| v.has_bytes_val(),
    DataType::FLOAT32 => |v: &FieldValue| v.has_float32_val(),
    DataType::FLOAT64 => |v: &FieldValue| v.has_float64_val(),
    DataType::TIMESTAMP_MICROS => |v: &FieldValue| v.has_timestamp_val(),
  };
  traverse_field_value(value, &checker)
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

pub fn partition_dtype_matches_field(dtype: &PartitionDataType, field: &NormalizedPartitionField) -> bool {
  let value = field.value.clone();
  match dtype {
    PartitionDataType::STRING => matches!(value, NormalizedPartitionValue::String(_)),
    PartitionDataType::INT64 => matches!(value, NormalizedPartitionValue::Int64(_)),
    PartitionDataType::BOOL => matches!(value, NormalizedPartitionValue::Bool(_)),
    PartitionDataType::TIMESTAMP_MINUTE => matches!(value, NormalizedPartitionValue::Minute(_)),
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
    },
    PartitionDataType::TIMESTAMP_MINUTE => {
      // TODO
      match value_str.parse::<i64>() {
        Ok(x) => {
          let mut t = Timestamp::new();
          t.seconds = x * 60;
          Some(PartitionValue::timestamp_val(t))
        },
        Err(_) => None,
      }
    },
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
    (PartitionValue::timestamp_val(x0), PartitionValue::timestamp_val(x1)) =>
      Ok((x0.seconds, x0.nanos).cmp(&(x1.seconds, x1.nanos))),
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

pub fn pancake_result_into_warp<T: Message>(
  server_res: ServerResult<T>,
  route_name: &str,
) -> Result<Box<dyn Reply>, Infallible> {
  let options = protobuf::json::PrintOptions {
    always_output_default_values: true,
    ..Default::default()
  };
  let body_res = server_res.and_then(|pb|
    protobuf::json::print_to_string_with_options(&pb, &options)
      .map_err(|_| ServerError::internal("unable to write response as json"))
  );
  match body_res {
    Ok(body) => {
      log::info!(
        "replying OK to {} request with {} bytes",
        route_name,
        body.len()
      );
      Ok(Box::new(Response::new(body)))
    },
    Err(e) => {
      let reply = warp::reply::json(&ErrorResponse {
        message: e.to_client_string(),
      });
      let status = e.kind.warp_status_code();
      log::info!(
        "replying ERR to {} request with status {}",
        route_name,
        status,
      );
      Ok(Box::new(warp::reply::with_status(
        reply,
        status,
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
    Value::float32_val(_) => 4,
    Value::float64_val(_) => 8,
    Value::timestamp_val(_) => 12,
    Value::list_val(x) => {
      let mut res = LIST_LENGTH_BYTES;
      for v in &x.vals {
        res += byte_size_of_field(v);
      }
      res
    }
  }).unwrap_or(1)
}

pub fn validate_segment_id(segment_id: &str) -> ServerResult<()> {
  match Uuid::from_str(segment_id) {
    Ok(_) => Ok(()),
    Err(_) => Err(ServerError::invalid(&format!(
      "{} is not a valid segment id (uuid)",
      segment_id,
    )))
  }
}

pub fn validate_partition_string(value: &str) -> ServerResult<()> {
  let mut allowable_special_chars = HashSet::new();
  allowable_special_chars.extend(vec!['-', '_', '!', '*', '(', ')']);
  if value.chars().any(|c| !c.is_ascii_alphanumeric() && !allowable_special_chars.contains(&c)) {
    let allowable_string = allowable_special_chars.iter()
      .map(|c| c.to_string())
      .collect::<Vec<String>>()
      .join("");
    return Err(ServerError::invalid(&format!(
      "partition string \"{}\" must contain only alphanumeric characters and characters from {}",
      value,
      allowable_string,
    )))
  }
  Ok(())
}

pub fn validate_entity_name_for_read(entity: &str, name: &str) -> ServerResult<()> {
  validate_entity_name(entity, name, false)
}

pub fn validate_entity_name_for_write(entity: &str, name: &str) -> ServerResult<()> {
  validate_entity_name(entity, name, true)
}

fn validate_entity_name(entity: &str, name: &str, is_write: bool) -> ServerResult<()> {
  let first_char = match name.chars().next() {
    Some(c) => Ok(c),
    None => Err(ServerError::invalid(&format!("{} name must not be empty", entity)))
  }?;
  if name.len() > MAX_NAME_LENGTH {
    return Err(ServerError::invalid(&format!(
      "{} name \"{}...\" must not contain over {} bytes",
      entity,
      &name[0..MAX_NAME_LENGTH - 3],
      MAX_NAME_LENGTH,
    )))
  }

  if is_write && first_char == '_' {
    return Err(ServerError::invalid(&format!(
      "{} name \"{}\" must not start with an underscore",
      entity,
      name
    )));
  }

  if name.chars().any(|c| c != '_' && !c.is_ascii_alphanumeric()) {
    return Err(ServerError::invalid(&format!(
      "{} name \"{}\" must contain only underscores and alphanumeric characters",
      entity,
      name
    )))
  }

  Ok(())
}

pub fn validate_rows(schema: &Schema, rows: &[Row]) -> ServerResult<()> {
  let mut col_map = HashMap::new();
  for col in &schema.columns {
    col_map.insert(&col.name, col);
  }
  for row in rows {
    for field in &row.fields {
      let mut err_msgs = Vec::new();
      match col_map.get(&field.name) {
        Some(col) => {
          if !dtype_matches_field(&col.dtype.unwrap(), field) {
            err_msgs.push(format!(
              "invalid field value for column {} with dtype {:?}: {:?}",
              col.name,
              col.dtype,
              field,
            ));
          }
        },
        _ => {
          err_msgs.push(format!("unknown column: {}", field.name));
        },
      };

      if field.value.is_some() {
        let byte_size = byte_size_of_field(field.value.as_ref().unwrap());
        if byte_size > MAX_FIELD_BYTE_SIZE {
          err_msgs.push(format!(
            "field for {} exceeds max byte size of {}",
            field.name,
            MAX_FIELD_BYTE_SIZE
          ))
        }
      }

      if !err_msgs.is_empty() {
        return Err(ServerError::invalid(&err_msgs.join("; ")));
      }
    }
  }
  Ok(())
}

pub fn rows_to_staged_bytes(rows: &[Row]) -> ServerResult<Vec<u8>> {
  let mut res = Vec::new();
  for row in rows {
    let nondescript_bytes = row.write_to_bytes()?;
    res.extend((nondescript_bytes.len() as u32).to_be_bytes());
    res.extend(nondescript_bytes);
  }
  Ok(res)
}

pub fn staged_bytes_to_rows(bytes: &[u8]) -> ServerResult<Vec<Row>> {
  let mut res = Vec::new();
  let mut i = 0;
  while i < bytes.len() {
    if bytes.len() < i + 4 {
      return Err(ServerError::internal("corrupt staged bytes; cannot read byte count"));
    }

    let len = u32::from_be_bytes((&bytes[i..i+4]).try_into().unwrap()) as usize;
    i += 4;

    if bytes.len() < i + len {
      return Err(ServerError::internal("corrupt staged bytes; cannot read proto bytes"));
    }
    res.push(Row::parse_from_bytes(&bytes[i..i+len])?);
    i += len;
  }
  Ok(res)
}

// number of rows (deleted or otherwise) in flush files (not compaction or staged)
pub fn flush_only_n(segment_meta: &SegmentMetadata, compaction: &Compaction) -> usize {
  let all_time_flushed_n = segment_meta.all_time_n - segment_meta.staged_n as u64;
  let all_time_compacted_n = compaction.compacted_n as u64 + compaction.omitted_n;
  (all_time_flushed_n - all_time_compacted_n) as usize
}

pub fn field_map(row: &Row) -> HashMap<&String, &Field> {
  let mut res = HashMap::new();
  for field in &row.fields {
    res.insert(&field.name, field);
  }
  res
}

pub fn single_field_from_row(row: &Row, name: &str) -> Field {
  // this returns a field with no value if the
  // row does not contain the name
  let mut res = Field {
    name: name.to_string(),
    ..Default::default()
  };
  for field in &row.fields {
    if field.name == *name {
      res.value = field.value.clone();
      break;
    }
  }
  res
}

pub fn unwrap_metadata<K: MetadataKey, M: Metadata<K>>(
  key: &K,
  metadata: &Option<M>,
) -> ServerResult<M> {
  match metadata {
    Some(m) => Ok(m.clone()),
    None => Err(ServerError::does_not_exist(K::ENTITY_NAME, &format!("{:?}", key)))
  }
}

// returns true if it creates a new file, false if the correct file already exists
pub async fn assert_file(path: &Path, content: Vec<u8>) -> ServerResult<bool> {
  match fs::read(path).await {
    Ok(bytes) => {
      if bytes == content {
        Ok(false)
      } else {
        Err(ServerError::invalid(&format!(
          "file {:?} already exists with different content",
          path
        )))
      }
    },
    Err(e) => {
      match e.kind() {
        io::ErrorKind::NotFound => {
          fs::write(path, &content).await?;
          Ok(true)
        }
        _ => Err(ServerError::from(e))
      }
    }
  }
}

pub fn check_no_duplicate_names(entity_name: &str, names: Vec<String>) -> ServerResult<()> {
  let mut seen = HashSet::new();
  let mut duplicates = Vec::new();
  for name in names {
    if seen.contains(&name) {
      duplicates.push(name)
    } else {
      seen.insert(name);
    }
  }
  if duplicates.len() > 0 {
    Err(ServerError::invalid(&format!(
      "duplicate {} names found: {}",
      entity_name,
      duplicates.join(", ")
    )))
  } else {
    Ok(())
  }
}
