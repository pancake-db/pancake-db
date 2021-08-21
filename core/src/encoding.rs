use pancake_db_idl::dml::{FieldValue, RepeatedFieldValue};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::schema::ColumnMeta;

use crate::utils;
use crate::errors::{PancakeResult, PancakeError};
use std::fmt::{Debug, Formatter};
use std::fmt;
use std::convert::Infallible;
use std::string::FromUtf8Error;

const ESCAPE_BYTE: u8 = 255;
const COUNT_BYTE: u8 = 254;
const NULL_BYTE: u8 = 253;
const TOP_NEST_LEVEL_BYTE: u8 = 252;
//e.g. for nesting level 2, <encoded v0>253<encoded v1>251<encoded v2>252...
//should encode [[<decoded v0>]], null, [[<decoded v1>], [<decoded v2>]], ...

pub trait StringLike {
  type Error: std::error::Error;
  fn into_bytes(&self) -> Vec<u8>;
  fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error> where Self: Sized;
}

impl StringLike for String {
  type Error = FromUtf8Error;
  fn into_bytes(&self) -> Vec<u8> {
    self.as_bytes().to_vec()
  }
  fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error> {
    String::from_utf8(bytes)
  }
}

impl StringLike for Vec<u8> {
  type Error = Infallible;
  fn into_bytes(&self) -> Vec<u8> {
    self.clone()
  }

  fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error> {
    Ok(bytes)
  }
}

//TODO bit packing for booleans?
//TODO use counts instead of null bytes for long runs of nulls?
pub fn encode(values: &[FieldValue], depth: u8) -> PancakeResult<Vec<u8>> {
  let mut res = Vec::new();

  for maybe_value in values {
    let mut maybe_err: PancakeResult<()> = Ok(());
    match &maybe_value.value {
      Some(value) => {
        let bytes = value_bytes(value, 0, depth);
        match bytes {
          Ok(actual_bytes) => {
            res.extend(actual_bytes);
          },
          Err(e) => {
            maybe_err = Err(e);
          },
        }
      },
      None => {
        res.push(NULL_BYTE);
      }
    };
    maybe_err?;
  }
  Ok(res)
}

fn value_bytes(v: &Value, traverse_depth: u8, escape_depth: u8) -> PancakeResult<Vec<u8>> {
  if traverse_depth == escape_depth {
    Ok(escape_bytes(&atomic_value_bytes(v)?, escape_depth))
  } else {
    match v {
      Value::list_val(l) => {
        let mut res = Vec::new();
        let mut maybe_err: Option<PancakeError> = None;
        for val in &l.vals {
          match value_bytes(val.value.as_ref().unwrap(), traverse_depth + 1, escape_depth) {
            Ok(bytes) => res.extend(bytes),
            Err(e) => {
              maybe_err = Some(e);
            }
          }
        }

        match maybe_err {
          Some(e) => Err(e),
          None => {
            let terminal_byte = TOP_NEST_LEVEL_BYTE - traverse_depth;
            // TODO: some terminal bytes can be skipped, since they are redundant with the next
            // repetition level. Gotta be careful though.
            res.push(terminal_byte);
            Ok(res)
          }
        }
      },
      _ => Err(PancakeError::invalid("expected a list to traverse but found atomic type"))
    }
  }
}

pub fn string_like_atomic_value_bytes<T: StringLike>(x: &T) -> Vec<u8> {
  let tail = x.into_bytes();
  let mut res = (tail.len() as u16).to_be_bytes().to_vec();
  res.extend(tail);
  res
}

pub fn atomic_value_bytes(v: &Value) -> PancakeResult<Vec<u8>> {
  match v {
    Value::list_val(_) => Err(PancakeError::invalid("expected to traverse down to atomic elements but found list")),
    Value::string_val(x) => Ok(string_like_atomic_value_bytes(x)),
    Value::int64_val(x) => Ok(x.to_be_bytes().to_vec()),
    Value::bool_val(x) => Ok(vec![*x as u8]),
    Value::bytes_val(x) => Ok(string_like_atomic_value_bytes(x)),
    Value::float64_val(x) => Ok(x.to_be_bytes().to_vec()),
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

impl<'a> Debug for ByteReader<'a> {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "ByteReader at {}; prev: {:?} next: {:?}",
      self.i,
      &self.bytes[0.max(self.i - 10)..self.i],
      &self.bytes[self.i..self.bytes.len().min(self.i + 10)],
    )
  }
}

impl<'a> ByteReader<'a> {
  pub fn new(bytes: &'a [u8], nested_list_depth: u8) -> Self {
    ByteReader {
      bytes,
      i: 0,
      nested_list_depth,
    }
  }
  pub fn complete(&self) -> bool {
    self.i >= self.bytes.len()
  }

  pub fn back_one(&mut self) {
    self.i -= 1;
  }

  pub fn read_one(&mut self) -> PancakeResult<u8> {
    if self.i >= self.bytes.len() {
      return Err(PancakeError::internal("read_one out of bytes"));
    }
    let res = self.bytes[self.i];
    self.i += 1;
    Ok(res)
  }

  pub fn unescaped_read_one(&mut self) -> PancakeResult<u8> {
    let b = self.read_one()?;
    if b == ESCAPE_BYTE {
      self.read_one()
    } else if b > TOP_NEST_LEVEL_BYTE - self.nested_list_depth {
      Err(PancakeError::internal(&format!("unexpected unescaped byte at {}", self.i)))
    } else {
      Ok(b)
    }
  }

  pub fn unescaped_read_n(&mut self, n: usize) -> PancakeResult<Vec<u8>> {
    let mut res = Vec::with_capacity(n);
    for _ in 0..n {
      res.push(self.unescaped_read_one()?);
    }
    Ok(res)
  }

  pub fn read_n(&mut self, n: usize) -> PancakeResult<&'a [u8]> {
    if self.i + n >= self.bytes.len() {
      return Err(PancakeError::internal("read_n out of bytes"));
    }
    let res = &self.bytes[self.i..self.i+n];
    self.i += n;
    Ok(res)
  }
}

pub fn decode(bytes: &[u8], meta: &ColumnMeta) -> PancakeResult<Vec<FieldValue>> {
  decode_limited(bytes, meta, usize::MAX)
}

pub fn decode_limited(
  bytes: &[u8],
  meta: &ColumnMeta,
  limit: usize
) -> PancakeResult<Vec<FieldValue>> {
  let mut res = Vec::new();
  let mut reader = ByteReader::new(bytes, meta.nested_list_depth as u8);
  while !reader.complete() && res.len() < limit {
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
        return Err(PancakeError::internal("in-file count did not match number of decoded entries"));
      }
    } else {
      reader.back_one();
      let v = decode_value(&mut reader, meta, 0)?;
      res.push(v);
    }
  }
  Ok(res)
}

pub fn decode_string_likes<T>(bytes: &[u8]) -> PancakeResult<Vec<T>> where T: StringLike {
  let mut reader = ByteReader::new(bytes, 0);
  let mut res = Vec::new();
  while !reader.complete() {
    res.push(decode_string_like::<T>(&mut reader)?);
  }
  Ok(res)
}

fn decode_string_like<T>(reader: &mut ByteReader) -> PancakeResult<T> where T: StringLike {
  let len_bytes = utils::try_byte_array::<2>(&reader.unescaped_read_n(2)?)?;
  let len = u16::from_be_bytes(len_bytes) as usize;
  Ok(T::try_from_bytes(reader.unescaped_read_n(len)?)?)
}

fn decode_value(reader: &mut ByteReader, meta: &ColumnMeta, current_depth: u8) -> PancakeResult<FieldValue> {
  if current_depth == meta.nested_list_depth as u8 {
    let value: PancakeResult<Value> = match meta.dtype.unwrap() {
      DataType::STRING => {
        Ok(Value::string_val(decode_string_like(reader)?))
      },
      DataType::INT64 => {
        let num_bytes = utils::try_byte_array::<8>(&reader.unescaped_read_n(8)?)?;
        let x = i64::from_be_bytes(num_bytes);
        Ok(Value::int64_val(x))
      },
      DataType::BOOL => {
        Ok(Value::bool_val(reader.unescaped_read_one()? > 0))
      },
      DataType::BYTES => {
        Ok(Value::bytes_val(decode_string_like(reader)?))
      },
      DataType::FLOAT64 => {
        let num_bytes = utils::try_byte_array::<8>(&reader.unescaped_read_n(8)?)?;
        let x = f64::from_be_bytes(num_bytes);
        Ok(Value::float64_val(x))
      },
    };
    Ok(FieldValue {
      value: Some(value?),
      ..Default::default()
    })
  } else {
    let terminal_byte = TOP_NEST_LEVEL_BYTE - current_depth;
    let mut fields = Vec::new();
    loop {
      let b = reader.read_one()?;
      if b <= TOP_NEST_LEVEL_BYTE && b >= terminal_byte {
        if b != terminal_byte {
          reader.back_one()
        }
        break
      } else {
        reader.back_one();
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

#[cfg(test)]
mod tests {
  use pancake_db_idl::dml::FieldValue;
  use pancake_db_idl::dml::field_value::Value;
  use crate::errors::PancakeResult;
  use super::*;
  use protobuf::ProtobufEnumOrUnknown;

  fn build_list_val(l: Vec<Value>) -> Value {
    Value::list_val(RepeatedFieldValue {
      vals: l.iter().map(|x| FieldValue {
        value: Some(x.clone()),
        ..Default::default()
      }).collect(),
      ..Default::default()
    })
  }

  #[test]
  fn test_strings() -> PancakeResult<()> {
    let strings = vec![
      Some("azAZ09﹝ﾂﾂﾂ﹞ꗽꗼ".to_string()),  // characters that use bytes 0xff, 0xfe, 0xfd, 0xfc
      None,
      Some("".to_string()),
      Some("z".repeat(2081))
    ];
    let column_meta = ColumnMeta {
      dtype: ProtobufEnumOrUnknown::new(DataType::STRING),
      nested_list_depth: 0,
      ..Default::default()
    };

    let values = strings.iter()
      .map(|maybe_s| FieldValue {
        value: maybe_s.as_ref().map(|s| Value::string_val(s.to_string())),
        ..Default::default()
      })
      .collect::<Vec<FieldValue>>();

    let encoded = encode(&values, 0)?;
    let decoded = decode(&encoded, &column_meta)?;
    let recovered = decoded.iter()
      .map(|fv| if fv.has_string_val() {Some(fv.get_string_val().to_string())} else {None})
      .collect::<Vec<Option<String>>>();

    assert_eq!(recovered, strings);
    Ok(())
  }

  #[test]
  fn test_ints() -> PancakeResult<()> {
    let ints: Vec<Option<i64>> = vec![
      Some(i64::MIN),
      Some(i64::MAX),
      None,
      Some(0),
      Some(-1),
    ];

    let column_meta = ColumnMeta {
      dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
      nested_list_depth: 0,
      ..Default::default()
    };

    let values = ints.iter()
      .map(|maybe_x| FieldValue {
        value: maybe_x.map(|x| Value::int64_val(x)),
        ..Default::default()
      })
      .collect::<Vec<FieldValue>>();

    let encoded = encode(&values, 0)?;
    let decoded = decode(&encoded, &column_meta)?;
    let recovered = decoded.iter()
      .map(|fv| if fv.has_int64_val() {Some(fv.get_int64_val())} else {None})
      .collect::<Vec<Option<i64>>>();

    assert_eq!(recovered, ints);
    Ok(())
  }

  #[test]
  fn test_nested_strings() -> PancakeResult<()> {
    let strings = vec![
      Some(vec![
        vec!["azAZ09﹝ﾂﾂﾂ﹞ꗽꗼ".to_string(), "abc".to_string()],
        vec!["/\\''!@#$%^&*()".to_string()],
      ]),  // characters that use bytes 0xff, 0xfe, 0xfd, 0xfc
      None,
      Some(vec![
        vec!["".to_string()],
        vec!["z".repeat(2)],
        vec!["null".to_string()]
      ]),
      Some(vec![vec![]]),
      Some(vec![])
    ];

    let column_meta = ColumnMeta {
      dtype: ProtobufEnumOrUnknown::new(DataType::STRING),
      nested_list_depth: 2,
      ..Default::default()
    };

    let values = strings.iter()
      .map(|maybe_x| FieldValue {
        value: maybe_x.as_ref().map(|x0| build_list_val(
          x0.iter().map(|x1| build_list_val(
            x1.iter().map(|x2| Value::string_val(x2.to_string())).collect()
          )).collect()
        )),
        ..Default::default()
      })
      .collect::<Vec<FieldValue>>();

    let encoded = encode(&values, 2)?;
    let decoded = decode(&encoded, &column_meta)?;
    let recovered = decoded.iter()
      .map(|fv| if fv.has_list_val() {
        Some(fv.get_list_val()
          .vals
          .iter()
          .map(|x1| x1.get_list_val()
            .vals
            .iter()
            .map(|x2| x2.get_string_val().to_string())
            .collect())
          .collect()
        )
      } else {
        None
      })
      .collect::<Vec<Option<Vec<Vec<String>>>>>();

    assert_eq!(recovered, strings);
    Ok(())
  }
}
