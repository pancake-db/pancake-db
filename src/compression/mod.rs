use pancake_db_idl::dtype::DataType;
use crate::storage::compaction::CompressionParams;
use pancake_db_idl::dml::field_value::Value;

mod strings;
mod ints;

pub fn get_compressor(dtype: &DataType, parameters: Option<&CompressionParams>) -> Box<dyn Compressor> {
  return match dtype {
    DataType::STRING => Box::new(strings::StringCompressor::from_parameters(parameters)),
    DataType::INT64 => Box::new(ints::Int64Compressor::from_parameters(parameters)),
  };
}

pub fn get_decompressor(dtype: &DataType, parameters: Option<&CompressionParams>) -> Box<dyn Decompressor> {
  return match dtype {
    DataType::STRING => Box::new(strings::StringDecompressor::from_parameters(parameters)),
    DataType::INT64 => Box::new(ints::Int64Decompressor::from_parameters(parameters)),
  };
}

pub fn compute_compression_params(elems: Vec<Value>, dtype: &DataType) -> String {
  return String::from("");
}

pub trait Compressor {
  fn from_parameters(parameters: Option<&CompressionParams>) -> Self where Self: Sized;
  fn encode(&mut self, value: &Value) -> Vec<u8>;
  //we're really using bits, so we might have some leftover ones at the end
  fn terminate(&mut self) -> Option<u8>;
}

pub trait Decompressor {
  fn from_parameters(parameters: Option<&CompressionParams>) -> Self where Self: Sized;
  fn decode(&mut self, bytes: &Vec<u8>) -> Vec<Value>;
}