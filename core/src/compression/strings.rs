use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::schema::ColumnMeta;

use crate::encoding;
use crate::errors::{PancakeResult, PancakeError};

use super::{Compressor, Decompressor};
use crate::compression::{Primitive, ValueDecompressor, ValueCompressor};
use pancake_db_idl::dtype::DataType;

const ZSTD_LEVEL: i32 = 5;

impl Primitive for String {
  fn try_from_value(v: &Value) -> PancakeResult<String> {
    match v {
      Value::string_val(res) => Ok(res.clone()),
      _ => Err(PancakeError::internal("unable to extract string from value"))
    }
  }

  fn to_value(&self) -> Value {
    Value::string_val(self.clone())
  }
}

pub struct ZstdCompressor {}

impl Compressor for ZstdCompressor {
  type T = String;
  fn compress_primitives(&self, values: &[String]) -> PancakeResult<Vec<u8>> {
    let raw_bytes = values.iter()
      .flat_map(|p| encoding::string_atomic_value_bytes(p))
      .collect::<Vec<u8>>();
    Ok(zstd::encode_all(&*raw_bytes, ZSTD_LEVEL)?)
  }
}

pub struct ZstdDecompressor {}

impl Decompressor for ZstdDecompressor {
  type T = String;
  fn decompress_primitives(&self, bytes: &[u8], meta: &ColumnMeta) -> PancakeResult<Vec<String>> {
    let decompressed_bytes = zstd::decode_all(bytes)?;
    encoding::decode_strings(&decompressed_bytes)
  }
}

#[cfg(test)]
mod tests {
  use pancake_db_idl::dtype::DataType;
  use protobuf::ProtobufEnumOrUnknown;

  use super::*;

  #[test]
  fn test_serde() -> PancakeResult<()> {
    let strs = vec!["orange", "banana", "grapefruit", "ÿ\\'\""];
    let strings = strs.iter()
      .map(|s| s.to_string())
      .collect::<Vec<String>>();

    let compressor = ZstdCompressor {};
    let bytes = compressor.compress_primitives(&strings)?;

    let decompressor = ZstdDecompressor {};
    let recovered_strings = decompressor.decompress_primitives(
      &bytes,
      &ColumnMeta {
        dtype: ProtobufEnumOrUnknown::new(DataType::STRING),
        ..Default::default()
      }
    )?;
    assert_eq!(
      strings,
      recovered_strings,
    );
    Ok(())
  }
}
