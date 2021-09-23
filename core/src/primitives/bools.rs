use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;

use crate::compression::Codec;
use crate::compression::q_codec::BoolQCodec;
use crate::compression::Q_COMPRESS;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::{Atom, Primitive};

impl Atom for bool {
  const BYTE_SIZE: usize = 1;

  fn to_bytes(&self) -> Vec<u8> {
    vec![*self as u8]
  }

  fn try_from_bytes(bytes: &[u8]) -> CoreResult<bool> {
    let byte = bytes[0];
    if byte == 0 {
      Ok(false)
    } else if byte == 1 {
      Ok(true)
    } else {
      Err(CoreError::corrupt(&format!("unable to decode bit from byte {}", byte)))
    }
  }
}

impl Primitive for bool {
  const DTYPE: DataType = DataType::BOOL;
  const IS_ATOMIC: bool = true;

  type A = Self;

  fn try_from_value(v: &Value) -> CoreResult<bool> {
    match v {
      Value::bool_val(res) => Ok(*res),
      _ => Err(CoreError::invalid("cannot read bool from value")),
    }
  }

  fn to_value(&self) -> Value {
    Value::bool_val(*self)
  }

  fn to_atoms(&self) -> Vec<Self> {
    vec![*self]
  }

  fn try_from_atoms(atoms: &[Self]) -> CoreResult<Self> {
    Ok(atoms[0])
  }

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<P=Self>>> {
    if codec == Q_COMPRESS {
      Some(Box::new(BoolQCodec {}))
    } else {
      None
    }
  }
}

