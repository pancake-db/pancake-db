use pancake_db_idl::dml::{FieldValue, RepeatedFieldValue};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::schema::ColumnMeta;
use q_compress::{BitReader, U32Decompressor};

use crate::compression::ints::I64QCompressor;
use crate::compression::strings::ZstdCompressor;
use crate::errors::{PancakeError, PancakeResult};

mod strings;
mod ints;

pub type CompressionParams = String;
pub const Q_COMPRESS: &str = "q_compress";
pub const ZSTD: &str = "zstd";
const REPETITION_LEVEL_Q_COMPRESSION_LEVEL: u32 = 6;

pub trait Primitive {
  fn try_from_value(v: &Value) -> PancakeResult<Self> where Self: Sized;
  fn to_value(&self) -> Value;
}

pub fn get_decompressor(
  dtype: DataType,
  parameters: &CompressionParams
) -> PancakeResult<Box<dyn ValueDecompressor>> {
  match dtype {
    DataType::STRING if parameters == ZSTD => Ok(Box::new(strings::ZstdDecompressor {})),
    DataType::INT64 if parameters == Q_COMPRESS => Ok(Box::new(ints::I64QDecompressor {})),
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
  parameters: &CompressionParams,
) -> PancakeResult<Box<dyn ValueCompressor>> {
  match dtype {
    DataType::STRING if parameters == ZSTD => Ok(Box::new(ZstdCompressor {})),
    DataType::INT64 if parameters == Q_COMPRESS => Ok(Box::new(I64QCompressor {})),
    _ => Err(PancakeError::invalid(&format!("unknown compression param / data type combination: {:?}, {:?}", parameters, dtype))),
  }
}

fn get_repetition_levels(
  value: &FieldValue,
  traverse_depth: u8,
  schema_depth: u8
) -> PancakeResult<Vec<u8>> {
  match &value.value {
    None => {
      if traverse_depth != 0 {
        return Err(PancakeError::internal("null value found in nested position"));
      }

      Ok(vec![0])
    },
    Some(Value::list_val(repeated)) => {
      if traverse_depth >= schema_depth {
        return Err(PancakeError::internal("traversed to deeper than schema depth"));
      }

      let mut res = Vec::new();
      for fv in &repeated.vals {
        res.extend(get_repetition_levels(fv, traverse_depth + 1, schema_depth)?)
      }
      res.push(traverse_depth + 1);
      Ok(res)
    },
    _ => {
      if traverse_depth != schema_depth {
        return Err(PancakeError::internal(
          &format!(
            "traverse depth of {} does not match schema depth of {}",
            traverse_depth,
            schema_depth
          )
        ))
      }
      Ok(vec![schema_depth + 1])
    }
  }
}

fn get_multi_repetition_levels(values: &[FieldValue], schema_depth: u8) -> PancakeResult<Vec<u32>> {
  let mut res = Vec::new();
  for fv in values {
    res.extend(
      get_repetition_levels(fv, 0, schema_depth)?.iter()
        .map(|&l| l as u32)
    )
  }
  Ok(res)
}

fn compress_repetition_levels(values: &[FieldValue], meta: &ColumnMeta) -> PancakeResult<Vec<u8>> {
  let rep_levels = get_multi_repetition_levels(values, meta.nested_list_depth as u8)?;
  let compressor = q_compress::U32Compressor::train(
    rep_levels.clone(),
    REPETITION_LEVEL_Q_COMPRESSION_LEVEL
  )?;
  Ok(compressor.compress(&rep_levels)?)
}

fn get_atoms(fv: &FieldValue) -> Vec<Value> {
  match &fv.value {
    None => Vec::new(),
    Some(Value::list_val(repeated)) => {
      repeated.vals.iter()
        .flat_map(get_atoms)
        .collect()
    },
    _ => vec![fv.value.clone().unwrap()]
  }
}

fn get_multi_atoms(values: &[FieldValue]) -> Vec<Value> {
  values.iter()
    .flat_map(get_atoms)
    .collect()
}

pub trait Compressor {
  type T: Primitive;
  fn compress_primitives(&self, primitives: &[Self::T]) -> PancakeResult<Vec<u8>>;
}

pub trait ValueCompressor {
  fn compress_atoms(&self, values: &[Value]) -> PancakeResult<Vec<u8>>;
  fn compress(&self, values: &[FieldValue], meta: &ColumnMeta) -> PancakeResult<Vec<u8>>;
}

impl<C, T> ValueCompressor for C where C: Compressor<T=T>, T: Primitive {
  fn compress_atoms(&self, values: &[Value]) -> PancakeResult<Vec<u8>> {
    let mut primitives = Vec::new();
    for v in values {
      primitives.push(T::try_from_value(v)?);
    }
    self.compress_primitives(&primitives)
  }

  fn compress(&self, values: &[FieldValue], meta: &ColumnMeta) -> PancakeResult<Vec<u8>> {
    let mut res = compress_repetition_levels(values, meta)?;
    let atoms = get_multi_atoms(values);
    res.extend(self.compress_atoms(&atoms)?);
    Ok(res)
  }
}

// pub struct ValueCompressorWrapper<T: Primitive, C: Compressor<T>> {
//   compressor: C
// }
//
// impl<T, C> From<C> for ValueCompressorWrapper<T, C> where T: Primitive, C: Compressor<T> {
//   fn from(compressor: C) -> ValueCompressorWrapper<T, C> {
//     ValueCompressorWrapper { compressor }
//   }
// }
//
// impl ValueCompressor for ValueCompressorWrapper<T: Primitive, C: Compressor<T>> {
//   fn compress_atoms(&self, values: &[Value]) -> PancakeResult<Vec<u8>> {
//     let mut primitives = Vec::new();
//     for v in values {
//       primitives.push(T::try_from_value(v)?);
//     }
//     self.compress_primitives(&primitives)
//   }
//
//   fn compress(&self, values: &[FieldValue], meta: &ColumnMeta) -> PancakeResult<Vec<u8>> {
//     let mut res = compress_repetition_levels(values, meta)?;
//     let atoms = get_multi_atoms(values);
//     res.extend(self.compress_atoms(&atoms)?);
//     Ok(res)
//   }
// }

struct AtomNester {
  rep_levels: Vec<u8>,
  atoms: Vec<Value>,
  schema_depth: u8,
  i: usize,
  j: usize,
}

impl AtomNester {
  pub fn from_levels_and_atoms(rep_levels: Vec<u8>, atoms: Vec<Value>, schema_depth: u8) -> Self {
    AtomNester {
      rep_levels,
      atoms,
      schema_depth,
      i: 0,
      j: 0,
    }
  }

  fn nested_field_value(&mut self, traverse_depth: u8) -> PancakeResult<FieldValue> {
    let mut level = self.rep_levels[self.i];
    if traverse_depth == 0 && level == 0 {
      //null
      self.i += 1;
      Ok(FieldValue::new())
    } else if traverse_depth < self.schema_depth {
      //list
      let mut res = Vec::new();
      while level != traverse_depth + 1 {
        res.push(self.nested_field_value(traverse_depth + 1)?);
        level = self.rep_levels[self.i];
      }

      self.i += 1;
      Ok(FieldValue {
        value: Some(Value::list_val(RepeatedFieldValue {
          vals: res,
          ..Default::default()
        })),
        ..Default::default()
      })
    } else if level == self.schema_depth + 1 {
      self.i += 1;
      let value = self.atoms[self.j].clone();
      self.j += 1;
      Ok(FieldValue {
        value: Some(value),
        ..Default::default()
      })
    } else {
      Err(PancakeError::internal("invalid repetition level found"))
    }
  }

  pub fn nested_field_values(&mut self) -> PancakeResult<Vec<FieldValue>> {
    let mut res = Vec::new();
    while self.i < self.rep_levels.len() {
      res.push(self.nested_field_value(0)?);
    }
    Ok(res)
  }
}

pub trait Decompressor {
  type T: Primitive;
  fn decompress_primitives(&self, bytes: &[u8], meta: &ColumnMeta) -> PancakeResult<Vec<Self::T>>;
}

pub trait ValueDecompressor {
  fn decompress_atoms(&self, bytes: &[u8], meta: &ColumnMeta) -> PancakeResult<Vec<Value>>;
  fn decompress(&self, bytes: Vec<u8>, meta: &ColumnMeta) -> PancakeResult<Vec<FieldValue>>;
}

impl<D, T> ValueDecompressor for D where D: Decompressor<T=T>, T: Primitive {
  fn decompress_atoms(&self, bytes: &[u8], meta: &ColumnMeta) -> PancakeResult<Vec<Value>> {
    self.decompress_primitives(bytes, meta)
      .map(|ps| {
        ps.iter()
          .map(|p| p.to_value())
          .collect()
      })
  }

  fn decompress(&self, bytes: Vec<u8>, meta: &ColumnMeta) -> PancakeResult<Vec<FieldValue>> {
    let mut bit_reader = BitReader::from(bytes);
    let bit_reader_ptr = &mut bit_reader;
    let rep_level_decompressor = U32Decompressor::from_reader(bit_reader_ptr)?;
    let rep_levels = rep_level_decompressor.decompress(bit_reader_ptr)
      .iter()
      .map(|&l| l as u8)
      .collect();

    let remaining_bytes = bit_reader.drain_bytes()?.to_vec();
    drop(bit_reader);

    let atoms = self.decompress_atoms(&remaining_bytes, meta)?;
    let mut nester = AtomNester::from_levels_and_atoms(
      rep_levels,
      atoms,
      meta.nested_list_depth as u8,
    );
    nester.nested_field_values()
  }
}