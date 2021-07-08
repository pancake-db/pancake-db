use std::io::ErrorKind;
use std::path::Path;

use pancake_db_idl::dml::Field;
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;
use tokio::fs;
use tokio::io::AsyncWriteExt;

pub async fn read_if_exists(fname: impl AsRef<Path>) -> Option<Vec<u8>> {
  match fs::read(fname).await {
    Ok(bytes) => Some(bytes),
    Err(_) => None,
  }
}

pub async fn create_if_new(dir: &str) -> Result<(), &'static str> {
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

pub async fn append_to_file(path: &str, contents: &[u8]) -> Result<(), &'static str> {
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
  let f = Field::int64("".to_string(), 33);
  let v = f.value.get_ref();
  println!("{:?} -> {}", v, serde_json::to_string(v).expect("fail"));
  let value = field.value.get_ref();
  println!("checking {:?} vs {:?}", dtype, value);
  match dtype {
    DataType::STRING if value.has_string_val() => true,
    DataType::INT64 if value.has_int64_val() => true,
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
