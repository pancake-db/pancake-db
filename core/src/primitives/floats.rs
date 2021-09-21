use pancake_db_idl::dml::field_value::Value;

use crate::compression::Codec;
use crate::compression::q_codec::F64QCodec;
use crate::compression::Q_COMPRESS;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::{Primitive, Atom};
use crate::encoding::ByteReader;
use crate::utils;
use pancake_db_idl::dtype::DataType;

impl Atom for f64 {}

impl Primitive for f64 {
  const DTYPE: DataType = DataType::FLOAT64;
  const IS_ATOMIC: bool = true;

  type A = Self;

  fn try_from_value(v: &Value) -> CoreResult<f64> {
    match v {
      Value::float64_val(res) => Ok(*res),
      _ => Err(CoreError::invalid("cannot read bool from value")),
    }
  }

  fn to_value(&self) -> Value {
    Value::float64_val(*self)
  }

  fn to_atoms(&self) -> Vec<Self> {
    vec![*self]
  }

  fn try_from_atoms(atoms: &[Self]) -> CoreResult<Self> {
    Ok(atoms[0])
  }

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<P=Self>>> {
    if codec == Q_COMPRESS {
      Some(Box::new(F64QCodec {}))
    } else {
      None
    }
  }

  fn encode(&self) -> Vec<u8> {
    self.to_be_bytes().to_vec()
  }

  fn decode(reader: &mut ByteReader) -> CoreResult<Self> {
    let num_bytes = utils::try_byte_array::<8>(&reader.unescaped_read_n(8)?)?;
    Ok(f64::from_be_bytes(num_bytes))
  }
}

