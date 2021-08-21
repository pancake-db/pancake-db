use pancake_db_idl::dml::{FieldValue, RepeatedFieldValue};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::schema::ColumnMeta;
use q_compress::{BitReader, U32Compressor, U32Decompressor};

use crate::errors::{PancakeError, PancakeResult};

mod bools;
mod floats;
mod ints;
mod q_codec;
mod string_like;
mod zstd_codec;

pub const Q_COMPRESS: &str = "q_compress";
pub const ZSTD: &str = "zstd";
const REPETITION_LEVEL_Q_COMPRESSION_LEVEL: u32 = 6;

pub trait Primitive: 'static {
  fn try_from_value(v: &Value) -> PancakeResult<Self> where Self: Sized;
  fn to_value(&self) -> Value;
  fn new_codec(codec: &str) -> Option<Box<dyn Codec<T=Self>>>;
  fn new_value_codec(codec: &str) -> Option<Box<dyn ValueCodec>> where Self: Sized {
    Self::new_codec(codec).map(|c| {
      let c: Box<dyn ValueCodec> = Box::new(c);
      c
    })
  }
}

pub fn new_codec(
  dtype: DataType,
  codec: &str,
) -> PancakeResult<Box<dyn ValueCodec>> {
  let maybe_res: Option<Box<dyn ValueCodec>> = match dtype {
    DataType::STRING => String::new_value_codec(codec),
    DataType::INT64 => i64::new_value_codec(codec),
    DataType::BYTES => Vec::<u8>::new_value_codec(codec),
    DataType::BOOL => bool::new_value_codec(codec),
    DataType::FLOAT64 => f64::new_value_codec(codec),
  };

  match maybe_res {
    Some(res) => Ok(res),
    None => Err(PancakeError::invalid(&format!(
      "compression codec {} unavailable for data type {:?}",
      codec,
      dtype,
    )))
  }
}

pub fn choose_codec(dtype: DataType) -> String {
  match dtype {
    DataType::INT64 => Q_COMPRESS.to_string(),
    DataType::STRING => ZSTD.to_string(),
    DataType::BYTES => ZSTD.to_string(),
    DataType::FLOAT64 => Q_COMPRESS.to_string(),
    DataType::BOOL => Q_COMPRESS.to_string(),
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
  let compressor = U32Compressor::train(
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

pub trait Codec {
  type T: Primitive;

  fn compress_primitives(&self, primitives: &[Self::T]) -> PancakeResult<Vec<u8>>;

  fn decompress_primitives(&self, bytes: &[u8], meta: &ColumnMeta) -> PancakeResult<Vec<Self::T>>;
}

impl<T: Primitive> ValueCodec for Box<dyn Codec<T=T>> {
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

pub trait ValueCodec {
  fn compress_atoms(&self, values: &[Value]) -> PancakeResult<Vec<u8>>;
  fn compress(&self, values: &[FieldValue], meta: &ColumnMeta) -> PancakeResult<Vec<u8>>;

  fn decompress_atoms(&self, bytes: &[u8], meta: &ColumnMeta) -> PancakeResult<Vec<Value>>;
  fn decompress(&self, bytes: Vec<u8>, meta: &ColumnMeta) -> PancakeResult<Vec<FieldValue>>;
}

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

