use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::schema::ColumnMeta;

use crate::encoding;
use crate::storage::compaction::CompressionParams;

use super::{Compressor, Decompressor, ZSTD};

const ZSTD_LEVEL: i32 = 5;

pub struct ZstdCompressor {}

impl Compressor for ZstdCompressor {
  fn get_parameters(&self) -> CompressionParams {
    ZSTD.to_string()
  }

  fn compress_atoms(&self, values: &[Value]) -> Result<Vec<u8>, &'static str> {
    let raw_bytes = values.iter()
      .flat_map(|v| encoding::atomic_value_bytes(v).unwrap())
      .collect::<Vec<u8>>();
    match zstd::encode_all(&*raw_bytes, ZSTD_LEVEL) {
      Ok(res) => Ok(res),
      Err(_) => Err("zstd fail")
    }
  }
}

pub struct ZstdDecompressor {}

impl Decompressor for ZstdDecompressor {
  fn from_parameters(_: Option<&CompressionParams>) -> ZstdDecompressor {
    return ZstdDecompressor {};
  }

  fn decompress_atoms(&self, bytes: &[u8], meta: &ColumnMeta) -> Result<Vec<Value>, &'static str> {
    let decompressed_bytes = match zstd::decode_all(bytes) {
      Ok(res) => Ok(res),
      Err(_) => Err("zstd fail")
    }?;
    Ok(encoding::decode(&decompressed_bytes, meta)?
      .iter()
      .map(|v| v.value.as_ref().unwrap().clone())
      .collect())
  }
}

#[cfg(test)]
mod tests {
  use pancake_db_idl::dtype::DataType;
  use protobuf::ProtobufEnumOrUnknown;

  use super::*;

  #[test]
  fn test_serde() -> Result<(), &'static str> {
    let strings = vec!["orange", "banana", "grapefruit", "ÿ\\'\""];
    let values = strings.iter()
      .map(|s| Value::string_val(s.to_string()))
      .collect::<Vec<Value>>();

    let compressor = ZstdCompressor {};
    let bytes = compressor.compress_atoms(&values)?;
    println!("bytes {:?}", bytes);

    let decompressor = ZstdDecompressor {};
    let recovered_values = decompressor.decompress_atoms(
      &bytes,
      &ColumnMeta {dtype: ProtobufEnumOrUnknown::new(DataType::STRING), ..Default::default()}
    )?;
    let recovered_strings = recovered_values.iter()
      .map(|v| match v {
        Value::string_val(s) => s,
        _ => panic!("unexpected")
      })
      .collect::<Vec<&String>>();
    assert_eq!(
      strings,
      recovered_strings,
    );
    Ok(())
  }
}
