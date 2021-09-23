use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;

use crate::compression::Codec;
use crate::compression::ZSTD;
use crate::compression::zstd_codec::ZstdCodec;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::Primitive;

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
    self.as_bytes().to_vec()
  }

  fn try_from_atoms(atoms: &[u8]) -> CoreResult<Self> {
    Ok(String::from_utf8(atoms.to_vec())?)
  }

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<P=Self>>> {
    if codec == ZSTD {
      Some(Box::new(ZstdCodec::<String>::default()))
    } else {
      None
    }
  }
}

#[cfg(test)]
mod tests {
  use pancake_db_idl::dml::FieldValue;

  use super::*;

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
