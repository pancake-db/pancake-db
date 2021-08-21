use pancake_db_idl::dml::field_value::Value;

use crate::compression::{Primitive, Q_COMPRESS};
use crate::compression::q_codec::BoolQCodec;
use crate::errors::{PancakeError, PancakeResult};

use super::Codec;

impl Primitive for bool {
  fn try_from_value(v: &Value) -> PancakeResult<bool> {
    match v {
      Value::bool_val(res) => Ok(*res),
      _ => Err(PancakeError::internal("cannot read bool from value")),
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
}

