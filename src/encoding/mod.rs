use std::convert::TryInto;

use anyhow::{anyhow, Result};
use pancake_db_idl::dml::{FieldValue, RepeatedFieldValue};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::schema::ColumnMeta;

use crate::utils;

const ESCAPE_BYTE: u8 = 255;
const COUNT_BYTE: u8 = 254;
const NULL_BYTE: u8 = 253;
const TOP_NEST_LEVEL_BYTE: u8 = 252;

//TODO bit packing for booleans?
//TODO use counts instead of null bytes for long runs of nulls?
pub fn encode(values: &[FieldValue], depth: u8) -> Result<Vec<u8>> {
  let mut res = Vec::new();

  for maybe_value in values {
    let mut err: Option<anyhow::Error> = None;
    match &maybe_value.value {
      Some(value) => {
        let bytes = value_bytes(value, depth, depth);
        match bytes {
          Ok(actual_bytes) => {
            res.extend(actual_bytes);
          },
          Err(e) => {
            err = Some(anyhow!(e));
          },
        }
      },
      None => {
        res.push(NULL_BYTE);
      }
    };
    if let Some(e) = err {
      return Err(e);
    }
  }
  Ok(res)
}

fn value_bytes(v: &Value, traverse_depth: u8, escape_depth: u8) -> Result<Vec<u8>> {
  if traverse_depth == 0 {
    Ok(escape_bytes(&atomic_value_bytes(v)?, escape_depth))
  } else {
    match v {
      Value::list_val(l) => {
        let mut res = Vec::new();
        let mut err: Option<anyhow::Error> = None;
        for val in &l.vals {
          match value_bytes(val.value.as_ref().unwrap(), traverse_depth - 1, escape_depth) {
            Ok(bytes) => res.extend(bytes),
            Err(e) => {
              err = Some(e);
            }
          }
        }

        match err {
          Some(e) => Err(e),
          None => {
            res.push(TOP_NEST_LEVEL_BYTE - escape_depth + traverse_depth);
            Ok(res)
          }
        }
      },
      _ => Err(anyhow!("expected a list to traverse but found atomic type"))
    }
  }
}

pub fn atomic_value_bytes(v: &Value) -> Result<Vec<u8>> {
  match v {
    Value::string_val(x) => {
      let tail = x.clone().into_bytes();
      let mut res = (tail.len() as u16).to_be_bytes().to_vec();
      res.extend(tail);
      Ok(res)
    },
    Value::int64_val(x) => Ok(x.to_be_bytes().to_vec()),
    Value::list_val(_) => Err(anyhow!("expected to traverse down to atomic elements but found list"))
  }
}

fn escape_bytes(bytes: &[u8], depth: u8) -> Vec<u8> {
  let mut res = Vec::new();
  for &b in bytes {
    if b > TOP_NEST_LEVEL_BYTE - depth {
      res.push(ESCAPE_BYTE);
    }
    res.push(b);
  }
  res
}

struct ByteReader<'a> {
  bytes: &'a [u8],
  i: usize,
  nested_list_depth: u8,
}

impl<'a> ByteReader<'a> {
  pub fn complete(&self) -> bool {
    self.i >= self.bytes.len()
  }

  pub fn back_one(&mut self) {
    self.i -= 1;
  }

  pub fn read_one(&mut self) -> Result<u8> {
    if self.i >= self.bytes.len() {
      return Err(anyhow!("read_one out of bytes"));
    }
    let res = self.bytes[self.i];
    self.i += 1;
    Ok(res)
  }

  pub fn unescaped_read_one(&mut self) -> Result<u8> {
    let b = self.read_one()?;
    if b == ESCAPE_BYTE {
      self.read_one()
    } else if b > TOP_NEST_LEVEL_BYTE - self.nested_list_depth {
      Err(anyhow!("unexpected unescaped byte"))
    } else {
      Ok(b)
    }
  }

  pub fn unescaped_read_n(&mut self, n: usize) -> Result<Vec<u8>> {
    let mut res = Vec::with_capacity(n);
    for _ in 0..n {
      res.push(self.unescaped_read_one()?);
    }
    Ok(res)
  }

  pub fn read_n(&mut self, n: usize) -> Result<&'a [u8]> {
    if self.i + n >= self.bytes.len() {
      return Err(anyhow!("read_n out of bytes"));
    }
    let res = &self.bytes[self.i..self.i+n];
    self.i += n;
    Ok(res)
  }
}

pub fn decode(bytes: &[u8], meta: &ColumnMeta) -> Result<Vec<FieldValue>> {
  let mut res = Vec::new();
  let mut reader = ByteReader { bytes, i: 0, nested_list_depth: meta.nested_list_depth as u8 };
  while !reader.complete() {
    let b0 = reader.read_one()?;
    if b0 == NULL_BYTE {
      res.push(FieldValue::new());
    } else if b0 == COUNT_BYTE {
      let count_bytes = utils::try_byte_array::<4>(reader.read_n(4)?)?;
      let count = u32::from_be_bytes(count_bytes) as usize;
      if res.is_empty() {
        for _ in 0..count {
          res.push(FieldValue::new());
        }
      } else if res.len() != count {
        return Err(anyhow!("in-file count did not match number of decoded entries"));
      }
    } else {
      reader.back_one();
      let v = decode_value(&mut reader, meta, 0)?;
      res.push(v);
    }
  }
  Ok(res)
}

fn decode_value(reader: &mut ByteReader, meta: &ColumnMeta, current_depth: u8) -> Result<FieldValue> {
  if current_depth == meta.nested_list_depth as u8 {
    match meta.dtype.unwrap() {
      DataType::STRING => {
        let len_bytes = utils::try_byte_array::<2>(&reader.unescaped_read_n(2)?)?;
        let len = u16::from_be_bytes(len_bytes) as usize;
        let x = String::from_utf8(reader.unescaped_read_n(len)?)?;
        Ok(FieldValue {
          value: Some(Value::string_val(x)),
          ..Default::default()
        })
      },
      DataType::INT64 => {
        let num_bytes = utils::try_byte_array::<8>(&reader.unescaped_read_n(8)?)?;
        let x = i64::from_be_bytes(num_bytes);
        Ok(FieldValue {
          value: Some(Value::int64_val(x)),
          ..Default::default()
        })
      }
    }
  } else {
    let terminal_byte = TOP_NEST_LEVEL_BYTE - current_depth;
    let mut fields = Vec::new();
    loop {
      let b = reader.read_one()?;
      if b <= TOP_NEST_LEVEL_BYTE && b >= terminal_byte {
        if b > terminal_byte {
          reader.back_one()
        }
        break
      } else {
        let child = decode_value(reader, meta, current_depth + 1)?;
        fields.push(child);
      }
    }
    Ok(FieldValue {
      value: Some(Value::list_val(RepeatedFieldValue { vals: fields, ..Default::default() })),
      ..Default::default()
    })
  }
}
