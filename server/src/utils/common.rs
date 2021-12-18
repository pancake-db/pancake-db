use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::convert::TryInto;
use std::io::{ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use hyper::body::Bytes;
use pancake_db_idl::dml::{FieldValue, partition_filter, PartitionFilter, Row, PartitionFieldValue, PartitionFieldComparison};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dml::partition_field_value::Value as PartitionValue;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use pancake_db_idl::schema::{Schema, ColumnMeta};
use protobuf::{Message, ProtobufEnumOrUnknown};
use protobuf::well_known_types::Timestamp;
use serde::Serialize;
use tokio::fs;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;
use warp::http::Response;
use warp::Reply;

use crate::constants::{LIST_LENGTH_BYTES, MAX_FIELD_BYTE_SIZE, MAX_NAME_LENGTH, ROW_ID_COLUMN_NAME, WRITTEN_AT_COLUMN_NAME, FS_BLOCK_SIZE};
use crate::errors::{ServerError, ServerResult};
use crate::metadata::{PersistentMetadata, MetadataKey};
use crate::metadata::compaction::Compaction;
use crate::metadata::segment::SegmentMetadata;
use crate::types::{NormalizedPartitionField, NormalizedPartitionValue};
use pancake_db_idl::dml::partition_field_comparison::Operator;

pub async fn file_exists(fname: impl AsRef<Path>) -> ServerResult<bool> {
  match fs::File::open(fname.as_ref()).await {
    Ok(_) => Ok(true),
    Err(e) => {
      match e.kind() {
        io::ErrorKind::NotFound => Ok(false),
        _ => Err(ServerError::from(e).with_context(format!(
          "while checking existence of file {:?}",
          fname.as_ref(),
        )))
      }
    }
  }
}

pub async fn file_nonempty(fname: impl AsRef<Path>) -> ServerResult<bool> {
  match fs::File::open(fname.as_ref()).await {
    Ok(mut f) => {
      let mut buf = vec![0_u8];
      let bytes_read = f.read(&mut buf).await?;
      Ok(bytes_read > 0)
    },
    Err(e) if matches!(e.kind(), io::ErrorKind::NotFound) => Ok(false),
    Err(e) => Err(ServerError::from(e).with_context(format!(
      "while checking whether file {:?} is empty",
      fname.as_ref(),
    ))),
  }
}

pub async fn read_with_offset(fname: impl AsRef<Path>, offset: u64, bytes: usize) -> ServerResult<Vec<u8>> {
  // return completed: bool, and bytes if any
  let mut maybe_file = fs::File::open(fname.as_ref()).await
    .map(Some)
    .or_else(|e| match e.kind() {
      io::ErrorKind::NotFound => Ok(None),
      _ => Err(ServerError::from(e).with_context(format!(
        "while opening {:?} to read {} bytes with offset {}",
        fname.as_ref(),
        bytes,
        offset,
      ))),
    })?;

  if maybe_file.is_none() {
    return Ok(Vec::new());
  }

  let file = maybe_file.as_mut().unwrap();
  file.seek(SeekFrom::Start(offset)).await
    .map_err(|e| ServerError::from(e).with_context(format!(
      "while seeking {:?} to read {} bytes with offset {}",
      fname.as_ref(),
      bytes,
      offset,
    )))?;

  let mut res = vec![0_u8].repeat(bytes);
  let mut count = 0;
  while count < bytes {
    match file.read(&mut res[count..]).await {
      Ok(0) => break,
      Ok(n) => {
        count += n;
      }
      Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
      Err(e) => return Err(ServerError::from(e).with_context(format!(
        "while scanning {:?} to read {} bytes with offset {}",
        fname.as_ref(),
        bytes,
        offset,
      ))),
    }
  }
  if count < bytes {
    Ok(res[..count].to_vec())
  } else {
    Ok(res)
  }
}

// returns whether it already exists
pub async fn create_if_new(dir: impl AsRef<Path>) -> ServerResult<bool> {
  match fs::create_dir(dir.as_ref()).await {
    Ok(_) => Ok(false),
    Err(e) => match e.kind() {
      ErrorKind::AlreadyExists => Ok(true),
      _ => Err(ServerError::from(e).with_context(format!(
        "while creating directory {:?}",
        dir.as_ref(),
      ))),
    },
  }
}

pub async fn overwrite_file_atomic(
  path: impl AsRef<Path>,
  contents: impl AsRef<[u8]>,
  dir: &Path,
) -> ServerResult<()> {
  let path = path.as_ref();
  let use_temp_path = contents.as_ref().len() > FS_BLOCK_SIZE;
  let initial_write_path = if use_temp_path {
    PathBuf::from(path)
  } else {
    dir.join(format!("tmp/{}", Uuid::new_v4().to_string()))
  };
  fs::write(
    &initial_write_path,
    contents,
  ).await
    .map_err(|e| ServerError::from(e).with_context(format!(
      "while writing {:?} for atomic overwrite of {:?}",
      initial_write_path,
      path,
    )))?;

  if use_temp_path {
    fs::rename(
      &initial_write_path,
      path,
    ).await
      .map_err(|e| ServerError::from(e).with_context(format!(
        "while moving {:?} to {:?} for atomic overwrite",
        initial_write_path,
        path,
      )))?;
  }

  Ok(())
}

pub async fn overwrite_file(
  path: impl AsRef<Path>,
  contents: impl AsRef<[u8]>,
) -> ServerResult<()> {
  fs::write(
    path.as_ref(),
    contents,
  ).await
    .map_err(|e| ServerError::from(e).with_context(format!(
      "during non-atomic overwrite of {:?}",
      path.as_ref(),
    )))
}

pub async fn append_to_file(path: impl AsRef<Path>, contents: &[u8]) -> ServerResult<()> {
  let mut file = fs::OpenOptions::new()
    .append(true)
    .create(true)
    .open(path.as_ref())
    .await
    .map_err(|e| ServerError::from(e).with_context(format!(
      "while opening to append to {:?}",
      path.as_ref(),
    )))?;
  file.write_all(contents).await
    .map_err(|e| ServerError::from(e).with_context(format!(
      "while writing to append to {:?}",
      path.as_ref(),
    )))
}

pub async fn read_or_empty(path: &Path) -> ServerResult<Vec<u8>> {
  match fs::read(path).await {
    Ok(bytes) => Ok(bytes),
    Err(e) => match e.kind() {
      ErrorKind::NotFound => Ok(Vec::new()),
      _ => Err(ServerError::from(e).with_context(format!(
        "while reading {:?} (if it exists)",
        path
      ))),
    },
  }
}

pub fn dtype_matches_field(dtype: &DataType, fv: &FieldValue) -> bool {
  let checker = match dtype {
    DataType::STRING => |v: &FieldValue| v.has_string_val(),
    DataType::INT64 =>  |v: &FieldValue| v.has_int64_val(),
    DataType::BOOL => |v: &FieldValue| v.has_bool_val(),
    DataType::BYTES =>  |v: &FieldValue| v.has_bytes_val(),
    DataType::FLOAT32 => |v: &FieldValue| v.has_float32_val(),
    DataType::FLOAT64 => |v: &FieldValue| v.has_float64_val(),
    DataType::TIMESTAMP_MICROS => |v: &FieldValue| v.has_timestamp_val(),
  };
  traverse_field_value(fv, &checker)
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

pub fn partition_field_value_from_string(
  value_str: &str,
  dtype: PartitionDataType
) -> ServerResult<PartitionFieldValue> {
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
  Ok(PartitionFieldValue {
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
    _ => Err(ServerError::invalid(format!(
      "partition filter value {:?} does not match data type of actual value {:?}",
      v0,
      v1,
    )))
  }
}

fn field_satisfies_comparison_filter(name: &str, field: &PartitionFieldValue, comparison: &PartitionFieldComparison) -> ServerResult<bool> {
  let flat_comparison_value = comparison.value.as_ref()
    .map(|v| v.value.clone())
    .flatten();
  if flat_comparison_value.is_none() {
    return Err(ServerError::invalid(format!(
      "partition filter for {} has no value",
      comparison.name
    )))
  }

  if name != comparison.name {
    return Ok(true);
  }

  let ordering = cmp_partition_field_values(
    field.value.as_ref().unwrap(),
    &flat_comparison_value.unwrap(),
  )?;
  Ok(match comparison.operator.enum_value_or_default() {
    Operator::EQ_TO => matches!(ordering, Ordering::Equal),
    Operator::LESS_OR_EQ_TO => !matches!(ordering, Ordering::Greater),
    Operator::LESS => matches!(ordering, Ordering::Less),
    Operator::GREATER_OR_EQ_TO => !matches!(ordering, Ordering::Less),
    Operator::GREATER => matches!(ordering, Ordering::Greater),
  })
}

pub fn satisfies_filters(partition: &HashMap<String, PartitionFieldValue>, filters: &[PartitionFilter]) -> ServerResult<bool> {
  for (name, pfv) in partition {
    for filter in filters {
      let satisfies = match &filter.value {
        Some(partition_filter::Value::comparison(comparison)) => field_satisfies_comparison_filter(
          name,
          pfv,
          comparison,
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
        "replying ERR to {} request with status {}: {}",
        route_name,
        status,
        e,
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
      ServerError::internal(format!("unknown data type code {}", enum_code))
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
    Err(_) => Err(ServerError::invalid(format!(
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
    return Err(ServerError::invalid(format!(
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
    None => Err(ServerError::invalid(format!("{} name must not be empty", entity)))
  }?;
  if name.len() > MAX_NAME_LENGTH {
    return Err(ServerError::invalid(format!(
      "{} name \"{}...\" must not contain over {} bytes",
      entity,
      &name[0..MAX_NAME_LENGTH - 3],
      MAX_NAME_LENGTH,
    )))
  }

  if is_write && first_char == '_' {
    return Err(ServerError::invalid(format!(
      "{} name \"{}\" must not start with an underscore",
      entity,
      name
    )));
  }

  if name.chars().any(|c| c != '_' && !c.is_ascii_alphanumeric()) {
    return Err(ServerError::invalid(format!(
      "{} name \"{}\" must contain only underscores and alphanumeric characters",
      entity,
      name
    )))
  }

  Ok(())
}

pub fn validate_rows(schema: &Schema, rows: &[Row]) -> ServerResult<()> {
  for row in rows {
    for (col_name, fv) in &row.fields {
      let mut err_msgs = Vec::new();
      match schema.columns.get(col_name) {
        Some(col) => {
          if !dtype_matches_field(&col.dtype.unwrap(), fv) {
            err_msgs.push(format!(
              "invalid field value for column {} with dtype {:?}: {:?}",
              col_name,
              col.dtype,
              fv,
            ));
          }
        },
        _ => {
          err_msgs.push(format!("unknown column: {}", col_name));
        },
      };

      if byte_size_of_field(fv) > MAX_FIELD_BYTE_SIZE {
        err_msgs.push(format!(
          "field for {} exceeds max byte size of {}",
          col_name,
          MAX_FIELD_BYTE_SIZE
        ))
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
    let row = Row::parse_from_bytes(&bytes[i..i+len])
      .map_err(|e| ServerError::from(e).with_context("while parsing staged row bytes"))?;
    res.push(row);
    i += len;
  }
  Ok(res)
}

// number of rows (deleted or otherwise) in flush files (not compaction or staged)
pub fn flush_only_n(segment_meta: &SegmentMetadata, compaction: &Compaction) -> u32 {
  segment_meta.all_time_n - segment_meta.staged_n - compaction.all_time_compacted_n
}

pub fn unwrap_metadata<K: MetadataKey, M: PersistentMetadata<K>>(
  key: &K,
  metadata: &Option<M>,
) -> ServerResult<M> {
  match metadata {
    Some(m) => Ok(m.clone()),
    None => Err(ServerError::does_not_exist(K::ENTITY_NAME, format!("{:?}", key)))
  }
}

// returns true if it creates a new file, false if the correct file already exists
pub async fn assert_file(path: &Path, content: Vec<u8>) -> ServerResult<bool> {
  match fs::read(path).await {
    Ok(bytes) => {
      if bytes == content {
        Ok(false)
      } else {
        Err(ServerError::invalid(format!(
          "file {:?} already exists with different content",
          path
        )))
      }
    },
    Err(e) => {
      let context = format!(
        "while asserting file {:?}",
        path,
      );
      match e.kind() {
        io::ErrorKind::NotFound => {
          fs::write(path, &content).await
            .map_err(|e| ServerError::from(e).with_context(context))?;
          Ok(true)
        },
        _ => Err(ServerError::from(e).with_context(context))
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
  if !duplicates.is_empty() {
    Err(ServerError::invalid(format!(
      "duplicate {} names found: {}",
      entity_name,
      duplicates.join(", ")
    )))
  } else {
    Ok(())
  }
}

// return a schema including "DB" columns like _row_id
pub fn augmented_columns(schema: &Schema) -> HashMap<String, ColumnMeta> {
  let mut res = schema.columns.clone();
  // If we ever add UINT32 or TIMESTAMP_SECONDS types, those would be
  // more appropriate.
  res.insert(
    ROW_ID_COLUMN_NAME.to_string(),
    ColumnMeta {
      dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
      ..Default::default()
    }
  );
  res.insert(
    WRITTEN_AT_COLUMN_NAME.to_string(),
    ColumnMeta {
      dtype: ProtobufEnumOrUnknown::new(DataType::TIMESTAMP_MICROS),
      ..Default::default()
    }
  );
  res
}
