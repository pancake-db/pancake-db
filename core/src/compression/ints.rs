use pancake_db_idl::dml::field_value::Value;

use crate::compression::{Primitive, Q_COMPRESS};
use crate::compression::q_codec::I64QCodec;
use crate::errors::{PancakeError, PancakeResult};

use super::Codec;

impl Primitive for i64 {
  fn try_from_value(v: &Value) -> PancakeResult<i64> {
    match v {
      Value::int64_val(res) => Ok(*res),
      _ => Err(PancakeError::internal("cannot read i64 from value")),
    }
  }

  fn to_value(&self) -> Value {
    Value::int64_val(*self)
  }

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<T=Self>>> {
    if codec == Q_COMPRESS {
      Some(Box::new(I64QCodec {}))
    } else {
      None
    }
  }
}

