use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::schema::ColumnMeta;
use q_compress::{BitReader, I64Decompressor};

use crate::errors::{PancakeError, PancakeResult};

use super::{Compressor, Decompressor};
use crate::compression::Primitive;

const Q_MAX_DEPTH: u32 = 7;

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
}

pub struct I64QCompressor {}

impl Compressor for I64QCompressor {
  type T = i64;
  fn compress_primitives(&self, primitives: &[i64]) -> PancakeResult<Vec<u8>> {
    let nums = primitives.to_vec();
    let compressor = q_compress::I64Compressor::train(
      nums,
      Q_MAX_DEPTH
    )?;
    Ok(compressor.compress(&primitives)?)
  }
}

pub struct I64QDecompressor {}

impl Decompressor for I64QDecompressor {
  type T = i64;
  fn decompress_primitives(&self, bytes: &[u8], _meta: &ColumnMeta) -> PancakeResult<Vec<i64>> {
    let mut bit_reader = BitReader::from(bytes.to_vec());
    let decompressor = I64Decompressor::from_reader(&mut bit_reader)?;
    Ok(decompressor.decompress(&mut bit_reader))
  }
}
