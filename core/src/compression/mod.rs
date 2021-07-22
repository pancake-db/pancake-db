use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dml::FieldValue;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::schema::ColumnMeta;

use crate::compression::ints::I64QCompressor;
use crate::compression::strings::ZstdCompressor;
use crate::errors::{PancakeResult, PancakeError};

mod strings;
mod ints;

const Q_COMPRESS: &str = "q_compress";
const ZSTD: &str = "zstd";
pub type CompressionParams = String;

pub fn get_decompressor(
  dtype: DataType,
  parameters: Option<&CompressionParams>
) -> PancakeResult<Box<dyn Decompressor>> {
  match dtype {
    DataType::STRING if parameters.is_some() && parameters.unwrap() == ZSTD => Ok(Box::new(strings::ZstdDecompressor::from_parameters(parameters))),
    DataType::INT64 if parameters.is_some() && parameters.unwrap() == Q_COMPRESS => Ok(Box::new(ints::I64QDecompressor::from_parameters(parameters))),
    _ => Err(PancakeError::internal(&format!("unknown decompression param / data type combination: {:?}, {:?}", parameters, dtype))),
  }
}

pub fn choose_compression_params(dtype: DataType) -> CompressionParams {
  match dtype {
    DataType::INT64 => Q_COMPRESS.to_string(),
    DataType::STRING => ZSTD.to_string(),
  }
}

pub fn get_compressor(
  dtype: DataType,
  parameters: Option<&CompressionParams>
) -> PancakeResult<Box<dyn Compressor>> {
  match dtype {
    DataType::STRING if parameters.is_some() && parameters.unwrap() == ZSTD => Ok(Box::new(ZstdCompressor {})),
    DataType::INT64 if parameters.is_some() && parameters.unwrap() == Q_COMPRESS => Ok(Box::new(I64QCompressor {})),
    _ => Err(PancakeError::invalid(&format!("unknown compression param / data type combination: {:?}, {:?}", parameters, dtype))),
  }
}

pub trait Compressor {
  fn compress_atoms(&self, values: &[Value]) -> PancakeResult<Vec<u8>>;
  fn compress(&self, values: &[FieldValue], _meta: &ColumnMeta) -> PancakeResult<Vec<u8>> {
    self.compress_atoms(&values.iter()
      .flat_map(|v| v.value.clone())
      .collect::<Vec<Value>>()
    )
  }
}

pub trait Decompressor {
  fn from_parameters(parameters: Option<&CompressionParams>) -> Self where Self: Sized;
  fn decompress_atoms(&self, bytes: &[u8], meta: &ColumnMeta) -> PancakeResult<Vec<Value>>;
  fn decompress(&self, bytes: &[u8], meta: &ColumnMeta) -> PancakeResult<Vec<FieldValue>> {
    Ok(
      self.decompress_atoms(bytes, meta)?
        .iter()
        .map(|v| FieldValue {
          value: Some(v.clone()),
          ..Default::default()
        })
        .collect()
    )
  }
}