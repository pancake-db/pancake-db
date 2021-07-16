use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dml::FieldValue;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::schema::ColumnMeta;

use crate::compression::ints::I64QCompressor;
use crate::compression::strings::ZstdCompressor;
use crate::storage::compaction::CompressionParams;

mod strings;
mod ints;

const Q_COMPRESS: &str = "q_compress";
const ZSTD: &str = "zstd";

pub fn get_decompressor(dtype: DataType, parameters: Option<&CompressionParams>) -> Result<Box<dyn Decompressor>, &'static str> {
  println!("data type {:?} parameters {:?}", dtype, parameters);
  match dtype {
    DataType::STRING if parameters.is_some() && *parameters.unwrap() == ZSTD => Ok(Box::new(strings::ZstdDecompressor::from_parameters(parameters))),
    DataType::INT64 if parameters.is_some() && *parameters.unwrap() == Q_COMPRESS => Ok(Box::new(ints::I64QDecompressor::from_parameters(parameters))),
    _ => Err("unknown dtype or parameters"),
  }
}

pub fn choose_compression_params(dtype: DataType) -> CompressionParams {
  match dtype {
    DataType::INT64 => Q_COMPRESS.to_string(),
    DataType::STRING => ZSTD.to_string(),
  }
}

pub fn get_compressor(
  _dtype: DataType,
  maybe_params: Option<&CompressionParams>
) -> Result<Box<dyn Compressor>, &'static str> {
  if maybe_params.is_none() {
    return Err("haven't handled this case yet");
  }

  let params = maybe_params.unwrap();
  let compressor: Box<dyn Compressor> = if *params == Q_COMPRESS {
    Box::new(I64QCompressor {})
  } else if *params == ZSTD {
    Box::new(ZstdCompressor {})
  } else {
    return Err("wacky compression params m8");
  };
  Ok(compressor)
}

pub trait Compressor {
  fn get_parameters(&self) -> CompressionParams;
  fn compress_atoms(&self, values: &[Value]) -> Result<Vec<u8>, &'static str>;
  fn compress(&self, values: &[FieldValue], _meta: &ColumnMeta) -> Result<Vec<u8>, &'static str> {
    self.compress_atoms(&values.iter()
      .flat_map(|v| v.value.clone())
      .collect::<Vec<Value>>()
    )
  }
}

pub trait Decompressor {
  fn from_parameters(parameters: Option<&CompressionParams>) -> Self where Self: Sized;
  fn decompress_atoms(&self, bytes: &[u8], meta: &ColumnMeta) -> Result<Vec<Value>, &'static str>;
  fn decompress(&self, bytes: &[u8], meta: &ColumnMeta) -> Result<Vec<FieldValue>, &'static str> {
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