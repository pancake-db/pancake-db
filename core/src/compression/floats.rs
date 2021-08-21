use pancake_db_idl::dml::field_value::Value;

use crate::compression::{Primitive, Q_COMPRESS};
use crate::compression::q_codec::F64QCodec;
use crate::errors::{PancakeError, PancakeResult};

use super::Codec;

impl Primitive for f64 {
  fn try_from_value(v: &Value) -> PancakeResult<f64> {
    match v {
      Value::float64_val(res) => Ok(*res),
      _ => Err(PancakeError::internal("cannot read bool from value")),
    }
  }

  fn to_value(&self) -> Value {
    Value::float64_val(*self)
  }

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<T=Self>>> {
    if codec == Q_COMPRESS {
      Some(Box::new(F64QCodec {}))
    } else {
      None
    }
  }
}

