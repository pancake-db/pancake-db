use pancake_db_idl::dml::{FieldValue, RepeatedFieldValue};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;

use crate::utils;
use crate::errors::{CoreResult, CoreError};
use crate::primitives::{StringLike, Primitive};
use std::marker::PhantomData;
use std::fmt::{Debug, Formatter};
use std::fmt;
use q_compress::TimestampNs;

const ESCAPE_BYTE: u8 = 255;
const COUNT_BYTE: u8 = 254;
const NULL_BYTE: u8 = 253;
const TOP_NEST_LEVEL_BYTE: u8 = 252;
//e.g. for nesting level 2, <encoded v0>253<encoded v1>251<encoded v2>252...
//should encode [[<decoded v0>]], null, [[<decoded v1>], [<decoded v2>]], ...

pub fn new_encoder_decoder(dtype: DataType, nested_list_depth: u8) -> Box<dyn EncoderDecoder> {
  match dtype {
    DataType::INT64 => Box::new(EncoderDecoderImpl::<i64>::new(nested_list_depth)),
    DataType::STRING => Box::new(EncoderDecoderImpl::<String>::new(nested_list_depth)),
    DataType::FLOAT64 => Box::new(EncoderDecoderImpl::<f64>::new(nested_list_depth)),
    DataType::BYTES => Box::new(EncoderDecoderImpl::<Vec<u8>>::new(nested_list_depth)),
    DataType::BOOL => Box::new(EncoderDecoderImpl::<bool>::new(nested_list_depth)),
    DataType::TIMESTAMP_NS => Box::new(EncoderDecoderImpl::<TimestampNs>::new(nested_list_depth)),
  }
}

pub trait EncoderDecoder {
  fn encode(&self, values: &[FieldValue]) -> CoreResult<Vec<u8>>;
  fn decode_limited(&self, bytes: &[u8], limit: usize) -> CoreResult<Vec<FieldValue>>;

  fn decode(&self, bytes: &[u8]) -> CoreResult<Vec<FieldValue>> {
    self.decode_limited(bytes, usize::MAX)
  }
}

struct EncoderDecoderImpl<T: Primitive> {
  escape_depth: u8,
  _phantom: PhantomData<T>,
}

impl<T: Primitive> EncoderDecoder for EncoderDecoderImpl<T> {
  fn encode(&self, values: &[FieldValue]) -> CoreResult<Vec<u8>> {
    let mut res = Vec::new();

    for maybe_value in values {
      let mut maybe_err: CoreResult<()> = Ok(());
      match &maybe_value.value {
        Some(value) => {
          let bytes = self.value_bytes(value, 0);
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

  fn decode_limited(
    &self,
    bytes: &[u8],
    limit: usize
  ) -> CoreResult<Vec<FieldValue>> {
    let mut res = Vec::new();
    let mut reader = ByteReader::new(bytes, self.escape_depth);
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
          return Err(CoreError::corrupt("in-file count did not match number of decoded entries"));
        }
      } else {
        reader.back_one();
        let v = self.decode_value(&mut reader, 0)?;
        res.push(v);
      }
    }
    Ok(res)
  }

}

impl<T: Primitive> EncoderDecoderImpl<T> {
  pub fn new(escape_depth: u8) -> Self {
    Self {
      escape_depth,
      _phantom: PhantomData,
    }
  }
  // TODO bit packing for booleans?
  // TODO use counts instead of null bytes for long runs of nulls?
  fn value_bytes(&self, v: &Value, traverse_depth: u8) -> CoreResult<Vec<u8>> {
    if traverse_depth == self.escape_depth {
      Ok(escape_bytes(&self.atomic_value_bytes(v)?, self.escape_depth))
    } else {
      match v {
        Value::list_val(l) => {
          let mut res = Vec::new();
          let mut maybe_err: Option<CoreError> = None;
          for val in &l.vals {
            match self.value_bytes(val.value.as_ref().unwrap(), traverse_depth + 1) {
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
        _ => Err(CoreError::invalid("expected a list to traverse but found atomic type"))
      }
    }
  }

  pub fn atomic_value_bytes(&self, v: &Value) -> CoreResult<Vec<u8>> {
    Ok(T::try_from_value(v)?.encode())
  }

  fn decode_value(&self, reader: &mut ByteReader, current_depth: u8) -> CoreResult<FieldValue> {
    if current_depth == self.escape_depth as u8 {
      let value = T::decode(reader)?.to_value();
      Ok(FieldValue {
        value: Some(value),
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
          let child = self.decode_value(reader, current_depth + 1)?;
          fields.push(child);
        }
      }
      Ok(FieldValue {
        value: Some(Value::list_val(RepeatedFieldValue { vals: fields, ..Default::default() })),
        ..Default::default()
      })
    }
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

pub struct ByteReader<'a> {
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

  pub fn read_one(&mut self) -> CoreResult<u8> {
    if self.i >= self.bytes.len() {
      return Err(CoreError::corrupt("read_one out of bytes"));
    }
    let res = self.bytes[self.i];
    self.i += 1;
    Ok(res)
  }

  pub fn unescaped_read_one(&mut self) -> CoreResult<u8> {
    let b = self.read_one()?;
    if b == ESCAPE_BYTE {
      self.read_one()
    } else if b > TOP_NEST_LEVEL_BYTE - self.nested_list_depth {
      Err(CoreError::corrupt(&format!("unexpected unescaped byte at {}", self.i)))
    } else {
      Ok(b)
    }
  }

  pub fn unescaped_read_n(&mut self, n: usize) -> CoreResult<Vec<u8>> {
    let mut res = Vec::with_capacity(n);
    for _ in 0..n {
      res.push(self.unescaped_read_one()?);
    }
    Ok(res)
  }

  pub fn read_n(&mut self, n: usize) -> CoreResult<&'a [u8]> {
    if self.i + n >= self.bytes.len() {
      return Err(CoreError::corrupt("read_n out of bytes"));
    }
    let res = &self.bytes[self.i..self.i+n];
    self.i += n;
    Ok(res)
  }
}

pub fn string_like_atomic_value_bytes<T: StringLike>(x: &T) -> Vec<u8> {
  let tail = x.into_bytes();
  let mut res = (tail.len() as u16).to_be_bytes().to_vec();
  res.extend(tail);
  res
}

pub fn decode_string_likes<T>(bytes: &[u8]) -> CoreResult<Vec<T>> where T: StringLike {
  let mut reader = ByteReader::new(bytes, 0);
  let mut res = Vec::new();
  while !reader.complete() {
    res.push(decode_string_like::<T>(&mut reader)?);
  }
  Ok(res)
}

pub fn decode_string_like<T>(reader: &mut ByteReader) -> CoreResult<T> where T: StringLike {
  let len_bytes = utils::try_byte_array::<2>(&reader.unescaped_read_n(2)?)?;
  let len = u16::from_be_bytes(len_bytes) as usize;
  let res = T::try_from_bytes(reader.unescaped_read_n(len)?)
    .map_err(|e| {
      CoreError::corrupt(&e.to_string())
    })?;
  Ok(res)
}


#[cfg(test)]
mod tests {
  use pancake_db_idl::dml::FieldValue;
  use pancake_db_idl::dml::field_value::Value;
  use crate::errors::CoreResult;
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
  fn test_strings() -> CoreResult<()> {
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
  fn test_ints() -> CoreResult<()> {
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
  fn test_nested_strings() -> CoreResult<()> {
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
