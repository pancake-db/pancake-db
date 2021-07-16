use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::schema::ColumnMeta;
use q_compress::{BitReader, I64Decompressor};

use crate::storage::compaction::CompressionParams;

use super::{Compressor, Decompressor};
use super::Q_COMPRESS;

const Q_MAX_DEPTH: u32 = 7;

pub struct I64QCompressor {}

fn get_num_values(values: &[Value]) -> Result<Vec<i64>, &'static str> {
  let mut num_values = Vec::with_capacity(values.len());
  for v in values {
    num_values.push(match v {
      Value::int64_val(x) => Ok(*x),
      _ => Err("unexpected dtype")
    }?);
  }
  Ok(num_values)
}

impl Compressor for I64QCompressor {
  fn get_parameters(&self) -> CompressionParams {
    Q_COMPRESS.to_string()
  }

  fn compress_atoms(&self, values: &[Value]) -> Result<Vec<u8>, &'static str> {
    let num_values = get_num_values(values)?;
    let maybe_compressor = q_compress::I64Compressor::train(
      num_values,
      Q_MAX_DEPTH
    );
    let compressor = match maybe_compressor {
      Ok(res) => Ok(res),
      Err(_) => Err("compressor training big sad")
    }?;
    let num_values = get_num_values(values)?;
    match compressor.compress(&num_values) {
      Ok(bytes) => Ok(bytes),
      Err(_) => Err("compressor big sad")
    }
  }
}

pub struct I64QDecompressor {}

impl Decompressor for I64QDecompressor {
  fn from_parameters(_: Option<&CompressionParams>) -> I64QDecompressor {
    return I64QDecompressor {};
  }

  fn decompress_atoms(&self, bytes: &[u8], _meta: &ColumnMeta) -> Result<Vec<Value>, &'static str> {
    let mut bit_reader = BitReader::from(bytes.to_vec());
    let decompressor = match I64Decompressor::from_reader(&mut bit_reader) {
      Ok(d) => Ok(d),
      Err(_) => Err("data does not seem to be in qco format")
    }?;
    let res = decompressor.decompress(&mut bit_reader)
      .iter()
      .map(|n| Value::int64_val(*n))
      .collect();
    Ok(res)
  }
}
