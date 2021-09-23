use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;

use crate::compression::Codec;
use crate::compression::ZSTD;
use crate::compression::zstd_codec::ZstdCodec;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::{Atom, Primitive};

impl Atom for u8 {
  const BYTE_SIZE: usize = 1;

  fn to_bytes(&self) -> Vec<u8> {
    vec![*self]
  }

  fn try_from_bytes(bytes: &[u8]) -> CoreResult<u8> {
    Ok(bytes[0])
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
}
