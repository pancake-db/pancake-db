use pancake_db_idl::dml::field_value::Value;

use crate::compression::Codec;
use crate::compression::q_codec::BoolQCodec;
use crate::compression::Q_COMPRESS;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::Primitive;
use crate::encoding::ByteReader;
use pancake_db_idl::dtype::DataType;

impl Primitive for bool {
  const DTYPE: DataType = DataType::BOOL;
  fn try_from_value(v: &Value) -> CoreResult<bool> {
    match v {
      Value::bool_val(res) => Ok(*res),
      _ => Err(CoreError::invalid("cannot read bool from value")),
    }
  }

  fn to_value(&self) -> Value {
    Value::bool_val(*self)
  }

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<T=Self>>> {
    if codec == Q_COMPRESS {
      Some(Box::new(BoolQCodec {}))
    } else {
      None
    }
  }

  fn encode(&self) -> Vec<u8> {
    vec![*self as u8]
  }

  fn decode(reader: &mut ByteReader) -> CoreResult<bool> {
    let byte = reader.unescaped_read_one()?;
    if byte == 0 {
      Ok(false)
    } else if byte == 1 {
      Ok(true)
    } else {
      Err(CoreError::corrupt(&format!("unable to decode bit from byte {}", byte)))
    }
  }
}

