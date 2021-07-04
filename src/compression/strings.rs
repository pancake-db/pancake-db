use pancake_db_idl::dml::field_value::Value;

use crate::storage::compaction::CompressionParams;

use super::{Compressor, Decompressor};

const STR_DELIM: u8 = 0x00;

pub struct StringCompressor {}

impl Compressor for StringCompressor {
  fn from_parameters(parameters: Option<&CompressionParams>) -> StringCompressor {
    return StringCompressor{};
  }

  fn encode(&mut self, elem: &Value) -> Vec<u8> {
    return match elem {
      Value::string_val(x) => {
        let mut res = x.clone().into_bytes();
        res.push(STR_DELIM);
        res
      }
      _ => panic!("unexpected data type")
    };
  }

  fn terminate(&mut self) -> Option<u8> {
    return None;
  }
}

pub struct StringDecompressor{}

impl Decompressor for StringDecompressor {
  fn from_parameters(parameters: Option<&CompressionParams>) -> StringDecompressor {
    return StringDecompressor{};
  }

  fn decode(&mut self, bytes: &Vec<u8>) -> Vec<Value> {
    let chunks = bytes.split(|byte| *byte == STR_DELIM);
    return chunks
      .map(|chunk| Value::string_val(String::from_utf8(chunk.to_vec()).unwrap()))
      .collect();
  }
}
