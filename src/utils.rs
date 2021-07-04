use std::io::ErrorKind;

use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::types::DataElem;

use pancake_db_idl::dtype::DataType;
use pancake_db_idl::dml::Field;
use pancake_db_idl::dml::field_value::Value;

pub async fn create_if_new(dir: &String) -> Result<(), &'static str> {
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

pub async fn overwrite_file(path: &str, contents: &[u8]) -> Result<(), &'static str> {
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

pub async fn append_to_file(path: &String, contents: &[u8]) -> Result<(), &'static str> {
  let mut file = fs::OpenOptions::new()
    .append(true)
    .create(true)
    .open(path)
    .await
    .unwrap();
  file.write_all(contents).await.unwrap();
  return Ok(());
}

pub fn dtype_matches_elem(dtype: &DataType, field: &Field) -> bool {
  let value = &field.value.0.as_ref().unwrap().value;
  match (dtype, value) {
    (DataType::STRING, Some(Value::string_val(_))) => true,
    (DataType::INT64, Some(Value::int64_val(_))) => true,
    _ => false
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
