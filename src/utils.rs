use std::convert::TryInto;
use std::fmt::Debug;
use std::io::ErrorKind;
use std::path::Path;

use anyhow::{anyhow, Result};
use pancake_db_idl::dml::{partition_field, partition_filter, PartitionFilter};
use pancake_db_idl::dml::{Field, PartitionField};
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use std::array::TryFromSliceError;

pub async fn read_if_exists(fname: impl AsRef<Path>) -> Option<Vec<u8>> {
  match fs::read(fname).await {
    Ok(bytes) => Some(bytes),
    Err(_) => None,
  }
}

pub async fn create_if_new(dir: impl AsRef<Path>) -> Result<()> {
  match fs::create_dir(dir).await {
    Ok(_) => Ok(()),
    Err(e) => match e.kind() {
      ErrorKind::AlreadyExists => Ok(()),
      _ => Err(e.into()),
    },
  }
}

pub async fn overwrite_file(path: impl AsRef<Path> + Debug, contents: &[u8]) -> Result<()> {
  let mut file = fs::File::create(path).await.expect("unable to create file for overwrite");
  match file.write_all(contents).await {
    Err(_) => Err(anyhow!("write failure")),
    _ => Ok(())
  }
}

pub async fn append_to_file(path: impl AsRef<Path> + Debug, contents: &[u8]) -> Result<()> {
  let mut file = fs::OpenOptions::new()
    .append(true)
    .create(true)
    .open(path)
    .await?;
  Ok(file.write_all(contents).await?)
}

pub fn partition_dtype_matches_field(dtype: &PartitionDataType, field: &PartitionField) -> bool {
  match dtype {
    PartitionDataType::STRING => field.has_string_val(),
    PartitionDataType::INT64 => field.has_int64_val(),
  }
}

pub fn dtype_matches_field(dtype: &DataType, field: &Field) -> bool {
  let value = field.value.get_ref();
  match dtype {
    DataType::STRING => value.has_string_val(),
    DataType::INT64 => value.has_int64_val(),
  }
}

pub fn partition_field_from_string(
  name: &str,
  value_str: &str,
  dtype: PartitionDataType
) -> Result<PartitionField> {
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
    return Err(anyhow!("failed to parse partition field value"));
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

pub fn try_byte_array<const N: usize>(v: &[u8]) -> Result<[u8; N]> {
  match v.try_into() {
    Ok(res) => Ok(res),
    Err(e) => Err(anyhow!("byte array has wrong size")),
  }
}
