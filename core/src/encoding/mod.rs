use pancake_db_idl::dml::FieldValue;
use pancake_db_idl::dtype::DataType;
use q_compress::TimestampNs;

pub use decoder::Decoder;
pub use decoder::DecoderImpl;
pub use encoder::Encoder;
pub use encoder::EncoderImpl;

use crate::primitives::Primitive;

mod byte_reader;
mod decoder;
mod encoder;

const ESCAPE_BYTE: u8 = 255;
const COUNT_BYTE: u8 = 254;
const NULL_BYTE: u8 = 253;

fn encoder_for<P: Primitive>(nested_list_depth: u8) -> Box<dyn Encoder> {
  Box::new(EncoderImpl::<P>::new(nested_list_depth))
}

fn field_value_decoder_for<P: Primitive>(nested_list_depth: u8) -> Box<dyn Decoder<FieldValue>> {
  Box::new(DecoderImpl::<P, FieldValue>::new(nested_list_depth))
}

pub fn new_encoder(dtype: DataType, nested_list_depth: u8) -> Box<dyn Encoder> {
  match dtype {
    DataType::INT64 => encoder_for::<i64>(nested_list_depth),
    DataType::STRING => encoder_for::<String>(nested_list_depth),
    DataType::FLOAT64 => encoder_for::<f64>(nested_list_depth),
    DataType::BYTES => encoder_for::<Vec<u8>>(nested_list_depth),
    DataType::BOOL => encoder_for::<bool>(nested_list_depth),
    DataType::TIMESTAMP_NS => encoder_for::<TimestampNs>(nested_list_depth),
  }
}

pub fn new_field_value_decoder(dtype: DataType, nested_list_depth: u8) -> Box<dyn Decoder<FieldValue>> {
  match dtype {
    DataType::INT64 => field_value_decoder_for::<i64>(nested_list_depth),
    DataType::STRING => field_value_decoder_for::<String>(nested_list_depth),
    DataType::FLOAT64 => field_value_decoder_for::<f64>(nested_list_depth),
    DataType::BYTES => field_value_decoder_for::<Vec<u8>>(nested_list_depth),
    DataType::BOOL => field_value_decoder_for::<bool>(nested_list_depth),
    DataType::TIMESTAMP_NS => field_value_decoder_for::<TimestampNs>(nested_list_depth),
  }
}

#[cfg(test)]
mod tests {
  use pancake_db_idl::dml::{FieldValue, RepeatedFieldValue};
  use pancake_db_idl::dml::field_value::Value;

  use crate::errors::CoreResult;
  use crate::primitives::Primitive;

  use super::*;

  fn build_list_val(l: Vec<Value>) -> Value {
    Value::list_val(RepeatedFieldValue {
      vals: l.iter().map(|x| FieldValue {
        value: Some(x.clone()),
        ..Default::default()
      }).collect(),
      ..Default::default()
    })
  }

  fn encode<P: Primitive>(fvs: &[FieldValue], escape_depth: u8) -> CoreResult<Vec<u8>> {
    let encoder = EncoderImpl::<P>::new(escape_depth);
    encoder.encode(fvs)
  }

  fn decode<P: Primitive>(encoded: &[u8], escape_depth: u8) -> CoreResult<Vec<FieldValue>> {
    let decoder = DecoderImpl::<P, FieldValue>::new(escape_depth);
    decoder.decode(encoded)
  }

  #[test]
  fn test_bytess() -> CoreResult<()> {
    let bytess = vec![
      Some(vec![0_u8, 255, 255, 254, 253]), // some bytes that need escaping
      None,
      Some(vec![]),
      Some(vec![77].repeat(2081))
    ];

    let values = bytess.iter()
      .map(|maybe_bytes| FieldValue {
        value: maybe_bytes.as_ref().map(|bytes| Value::bytes_val(bytes.to_vec())),
        ..Default::default()
      })
      .collect::<Vec<FieldValue>>();

    let encoded = encode::<Vec<u8>>(&values, 0)?;
    let decoded = decode::<Vec<u8>>(&encoded, 0)?;
    let recovered = decoded.iter()
      .map(|fv| if fv.has_bytes_val() {Some(fv.get_bytes_val().to_vec())} else {None})
      .collect::<Vec<Option<Vec<u8>>>>();

    assert_eq!(recovered, bytess);
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

    let values = ints.iter()
      .map(|maybe_x| FieldValue {
        value: maybe_x.map(|x| Value::int64_val(x)),
        ..Default::default()
      })
      .collect::<Vec<FieldValue>>();

    let encoded = encode::<i64>(&values, 0)?;
    let decoded = decode::<i64>(&encoded, 0)?;
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

    let encoded = encode::<String>(&values, 2)?;
    let decoded = decode::<String>(&encoded, 2)?;
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
