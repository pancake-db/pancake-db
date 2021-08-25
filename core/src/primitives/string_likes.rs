use pancake_db_idl::dml::field_value::Value;

use crate::compression::Codec;
use crate::compression::ZSTD;
use crate::compression::zstd_codec::{BytesZstdCodec, StringZstdCodec};
use crate::encoding;
use crate::encoding::ByteReader;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::Primitive;

use super::StringLike;
use std::string::FromUtf8Error;
use std::convert::Infallible;
use pancake_db_idl::dtype::DataType;

impl StringLike for String {
  type Error = FromUtf8Error;
  fn into_bytes(&self) -> Vec<u8> {
    self.as_bytes().to_vec()
  }
  fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error> {
    String::from_utf8(bytes)
  }
}

impl StringLike for Vec<u8> {
  type Error = Infallible;
  fn into_bytes(&self) -> Vec<u8> {
    self.clone()
  }

  fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error> {
    Ok(bytes)
  }
}

impl Primitive for String {
  const DTYPE: DataType = DataType::STRING;
  fn try_from_value(v: &Value) -> CoreResult<String> {
    match v {
      Value::string_val(res) => Ok(res.clone()),
      _ => Err(CoreError::invalid("unable to extract string from value"))
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

  fn encode(&self) -> Vec<u8> {
    encoding::string_like_atomic_value_bytes(self)
  }

  fn decode(reader: &mut ByteReader) -> CoreResult<Self> {
    encoding::decode_string_like(reader)
  }
}

impl Primitive for Vec<u8> {
  const DTYPE: DataType = DataType::BYTES;
  fn try_from_value(v: &Value) -> CoreResult<Vec<u8>> {
    match v {
      Value::bytes_val(res) => Ok(res.clone()),
      _ => Err(CoreError::invalid("unable to extract string from value"))
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

  fn encode(&self) -> Vec<u8> {
    encoding::string_like_atomic_value_bytes(self)
  }

  fn decode(reader: &mut ByteReader) -> CoreResult<Self> {
    encoding::decode_string_like(reader)
  }
}


#[cfg(test)]
mod tests {
  use pancake_db_idl::dtype::DataType;
  use protobuf::ProtobufEnumOrUnknown;

  use super::*;

  #[test]
  fn test_serde() -> CoreResult<()> {
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
