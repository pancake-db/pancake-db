use std::io::ErrorKind;
use std::path::Path;

use pancake_db_idl::dml::{partition_field, partition_filter, PartitionFilter};
use pancake_db_idl::dml::{Field, PartitionField};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use tokio::fs;
use tokio::io::AsyncWriteExt;

pub async fn read_if_exists(fname: impl AsRef<Path>) -> Option<Vec<u8>> {
  match fs::read(fname).await {
    Ok(bytes) => Some(bytes),
    Err(_) => None,
  }
}

pub async fn create_if_new(dir: impl AsRef<Path>) -> Result<(), &'static str> {
  match fs::create_dir(dir).await {
    Ok(_) => {
      return Ok(());
    },
    Err(e) => match e.kind() {
      ErrorKind::AlreadyExists => Ok(()),
      _ => Err("unknown creation error"),
    },
  }
}

pub async fn overwrite_file(path: impl AsRef<Path>, contents: &[u8]) -> Result<(), &'static str> {
  let mut file = fs::File::create(path).await.expect("unable to create file for overwrite");
  file.write_all(contents).await.unwrap();
  return Ok(());
}

// pub async fn assert_file(path: &String, expected: &String) -> Result<(), &'static str> {
//   match fs::read_to_string(path).await {
//     Ok(contents) => {
//       if contents.trim() == *expected {
//         return Ok(());
//       }
//       return Err("file contents not as expected");
//     },
//     Err(e) => match e.kind() {
//       ErrorKind::NotFound => {
//         let mut file = fs::File::create(path).await.expect("unable to create file");
//         file.write_all(expected.as_bytes()).await.expect("unable to write");
//         return Ok(());
//       },
//       _ => Err("unknown file read error"),
//     }
//   }
// }

pub async fn append_to_file(path: impl AsRef<Path>, contents: &[u8]) -> Result<(), &'static str> {
  let mut file = fs::OpenOptions::new()
    .append(true)
    .create(true)
    .open(path)
    .await
    .unwrap();
  file.write_all(contents).await.unwrap();
  return Ok(());
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

pub fn field_to_string(field: &Field) -> String {
  format!("{} {}", field.name, field_value_to_string(&field.value.0.as_ref().unwrap().value))
}

pub fn field_value_to_string(value: &Option<Value>) -> String {
  match value {
    None => {
      "None".to_string()
    },
    Some(Value::string_val(x)) => {
      format!("Str ({})", x)
    },
    Some(Value::int64_val(x)) => {
      format!("Int64 ({})", x)
    },
    Some(Value::list_val(x)) => {
      format!(
        "List ({})",
        x.vals
          .iter()
          .map(|v| field_value_to_string(&v.value))
          .collect::<Vec<String>>()
          .join(", ")
      )
    },
  }
}

pub fn partition_field_from_string(
  name: &str,
  value_str: &str,
  dtype: PartitionDataType
) -> Result<PartitionField, &'static str> {
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
    return Err("failed to parse partition field value");
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
