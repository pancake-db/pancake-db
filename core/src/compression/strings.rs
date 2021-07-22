use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::schema::ColumnMeta;

use crate::encoding;
use crate::errors::PancakeResult;

use super::{CompressionParams, Compressor, Decompressor};

const ZSTD_LEVEL: i32 = 5;

pub struct ZstdCompressor {}

impl Compressor for ZstdCompressor {
  fn compress_atoms(&self, values: &[Value]) -> PancakeResult<Vec<u8>> {
    let raw_bytes = values.iter()
      .flat_map(|v| encoding::atomic_value_bytes(v).unwrap())
      .collect::<Vec<u8>>();
    Ok(zstd::encode_all(&*raw_bytes, ZSTD_LEVEL)?)
  }
}

pub struct ZstdDecompressor {}

impl Decompressor for ZstdDecompressor {
  fn from_parameters(_: Option<&CompressionParams>) -> ZstdDecompressor {
    ZstdDecompressor {}
  }

  fn decompress_atoms(&self, bytes: &[u8], meta: &ColumnMeta) -> PancakeResult<Vec<Value>> {
    let decompressed_bytes = zstd::decode_all(bytes)?;
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
  fn test_serde() -> PancakeResult<()> {
    let strings = vec!["orange", "banana", "grapefruit", "ÿ\\'\""];
    let values = strings.iter()
      .map(|s| Value::string_val(s.to_string()))
      .collect::<Vec<Value>>();

    let compressor = ZstdCompressor {};
    let bytes = compressor.compress_atoms(&values)?;

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
