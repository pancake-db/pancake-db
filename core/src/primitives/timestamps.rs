use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;
use protobuf::well_known_types::Timestamp;
use q_compress::TimestampNs;
use q_compress::types::NumberLike;

use crate::compression::Codec;
use crate::compression::q_codec::TimestampNsQCodec;
use crate::compression::Q_COMPRESS;
use crate::encoding::ByteReader;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::{Primitive, Atom};

impl Atom for TimestampNs {}

impl Primitive for TimestampNs {
  const DTYPE: DataType = DataType::TIMESTAMP_NS;
  const IS_ATOMIC: bool = true;

  type A = Self;

  fn try_from_value(v: &Value) -> CoreResult<TimestampNs> {
    match v {
      Value::timestamp_val(res) => Ok(TimestampNs::from_secs_and_nanos(res.seconds, res.nanos as u32)),
      _ => Err(CoreError::invalid("cannot read timestamp from value")),
    }
  }

  fn to_value(&self) -> Value {
    let mut t = Timestamp::new();
    let (secs, nanos) = self.to_secs_and_nanos();
    t.seconds = secs;
    t.nanos = nanos as i32;
    Value::timestamp_val(t)
  }

  fn to_atoms(&self) -> Vec<Self> {
    vec![*self]
  }

  fn try_from_atoms(atoms: &[Self]) -> CoreResult<Self> {
    Ok(atoms[0])
  }

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<P=Self>>> {
    if codec == Q_COMPRESS {
      Some(Box::new(TimestampNsQCodec {}))
    } else {
      None
    }
  }

  fn encode(&self) -> Vec<u8> {
    TimestampNs::bytes_from(*self)
  }

  fn decode(reader: &mut ByteReader) -> CoreResult<Self> {
    let bytes = reader.unescaped_read_n(12)?;
    Ok(TimestampNs::from_bytes(bytes))
  }
}

