use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::schema::ColumnMeta;
use q_compress::{BitReader, I64Decompressor};

use crate::errors::{PancakeError, PancakeResult};

use super::{CompressionParams, Compressor, Decompressor};

const Q_MAX_DEPTH: u32 = 7;

pub struct I64QCompressor {}

fn get_num_values(values: &[Value]) -> PancakeResult<Vec<i64>> {
  let mut num_values = Vec::with_capacity(values.len());
  for v in values {
    num_values.push(match v {
      Value::int64_val(x) => Ok(*x),
      _ => Err(PancakeError::invalid(&format!("expected an int64 but found: {:?}", v)))
    }?);
  }
  Ok(num_values)
}

impl Compressor for I64QCompressor {
  fn compress_atoms(&self, values: &[Value]) -> PancakeResult<Vec<u8>> {
    let num_values = get_num_values(values)?;
    let compressor = q_compress::I64Compressor::train(
      num_values,
      Q_MAX_DEPTH
    )?;
    let num_values = get_num_values(values)?;
    Ok(compressor.compress(&num_values)?)
  }
}

pub struct I64QDecompressor {}

impl Decompressor for I64QDecompressor {
  fn from_parameters(_: Option<&CompressionParams>) -> I64QDecompressor {
    I64QDecompressor {}
  }

  fn decompress_atoms(&self, bytes: &[u8], _meta: &ColumnMeta) -> PancakeResult<Vec<Value>> {
    let mut bit_reader = BitReader::from(bytes.to_vec());
    let decompressor = I64Decompressor::from_reader(&mut bit_reader)?;
    let res = decompressor.decompress(&mut bit_reader)
      .iter()
      .map(|n| Value::int64_val(*n))
      .collect();
    Ok(res)
  }
}
