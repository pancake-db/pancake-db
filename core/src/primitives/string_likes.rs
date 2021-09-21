use std::convert::Infallible;
use std::string::FromUtf8Error;

use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;

use crate::compression::Codec;
use crate::compression::ZSTD;
use crate::compression::zstd_codec::ZstdCodec;
use crate::encoding;
use crate::encoding::ByteReader;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::{Atom, Primitive};

use super::StringLike;

impl Atom for u8 {}

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
  const IS_ATOMIC: bool = false;

  type A = u8;

  fn try_from_value(v: &Value) -> CoreResult<String> {
    match v {
      Value::string_val(res) => Ok(res.clone()),
      _ => Err(CoreError::invalid("unable to extract string from value"))
    }
  }

  fn to_value(&self) -> Value {
    Value::string_val(self.clone())
  }

  fn to_atoms(&self) -> Vec<u8> {
    self.into_bytes()
  }

  fn try_from_atoms(atoms: &[u8]) -> CoreResult<Self> {
    Ok(String::try_from_bytes(atoms.to_vec())?)
  }

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<P=Self>>> {
    if codec == ZSTD {
      Some(Box::new(ZstdCodec::<String>::default()))
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
  const IS_ATOMIC: bool = false;

  type A = u8;

  fn try_from_value(v: &Value) -> CoreResult<Vec<u8>> {
    match v {
      Value::bytes_val(res) => Ok(res.clone()),
      _ => Err(CoreError::invalid("unable to extract string from value"))
    }
  }

  fn to_value(&self) -> Value {
    Value::bytes_val(self.clone())
  }

  fn to_atoms(&self) -> Vec<u8> {
    self.to_vec()
  }

  fn try_from_atoms(atoms: &[u8]) -> CoreResult<Self> {
    Ok(atoms.to_vec())
  }

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<P=Self>>> {
    if codec == ZSTD {
      Some(Box::new(ZstdCodec::<Vec<u8>>::default()))
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
  use super::*;
  use pancake_db_idl::dml::FieldValue;

  #[test]
  fn test_serde() -> CoreResult<()> {
    let strs = vec!["orange", "banana", "grapefruit", "ÿ\\'\""];
    let fvs = strs.iter()
      .map(|s| FieldValue {
        value: Some(Value::string_val(s.to_string())),
        ..Default::default()
      })
      .collect::<Vec<FieldValue>>();

    let value_codec = String::new_value_codec(ZSTD).unwrap();

    let bytes = value_codec.compress(&fvs, 0)?;
    let recovered_values = value_codec.decompress(bytes, 0)?;
    assert_eq!(
      fvs,
      recovered_values,
    );
    Ok(())
  }
}
