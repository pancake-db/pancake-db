use pancake_db_idl::dml::field_value::Value;

use crate::errors::{PancakeResult, PancakeError};

use super::Codec;
use crate::compression::{Primitive, ZSTD};
use crate::compression::zstd_codec::{StringZstdCodec, BytesZstdCodec};

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

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<T=Self>>> {
    if codec == ZSTD {
      Some(Box::new(StringZstdCodec {}))
    } else {
      None
    }
  }
}

impl Primitive for Vec<u8> {
  fn try_from_value(v: &Value) -> PancakeResult<Vec<u8>> {
    match v {
      Value::bytes_val(res) => Ok(res.clone()),
      _ => Err(PancakeError::internal("unable to extract string from value"))
    }
  }

  fn to_value(&self) -> Value {
    Value::bytes_val(self.clone())
  }

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<T=Self>>> {
    if codec == ZSTD {
      Some(Box::new(BytesZstdCodec {}))
    } else {
      None
    }
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
