use std::convert::TryInto;

use pancake_db_idl::dml::field_value::Value;

use crate::storage::compaction::CompressionParams;

use super::{Compressor, Decompressor};

pub struct Int64Compressor {
  quantiles: Vec<i64>,
  percentile_left_buffer: f64,
  percentile_right_buffer: f64,
  trailing_bits: Vec<bool>,
}

impl Compressor for Int64Compressor {
  fn from_parameters(parameters: Option<&CompressionParams>) -> Int64Compressor {
    return Int64Compressor{
      quantiles: vec![i64::MIN, i64::MAX],
      percentile_left_buffer: 0.0,
      percentile_right_buffer: 0.0,
      trailing_bits: Vec::new(),
    };
  }

  fn encode(&mut self, value: &Value) -> Vec<u8> {
    return match value {
      Value::int64_val(x) => x.to_le_bytes().to_vec(),
      _ => panic!("unexpected data type"),
    };
    // let bits = Vec::new();
    // let n_buckets = self.quantiles.len();
    // let mut fmin = -self.percentile_left_buffer;
    // let mut fmax = self.quantiles.len() as f64 + self.percentile_right_buffer;
    // // terminate either when exact integer is identified (returning)
    // // or when quantile bucket is identified
    // let bucket;
    // loop {
    //   let lower_bucket = fmin.floor() as usize;
    //   let upper_bucket = fmax.ceil() as usize;
    //   if lower_bucket == upper_bucket - 1 {
    //     bucket = lower_bucket;
    //     break;
    //   }
    //   if lower_bucket >= 0 && upper_bucket <= n_buckets - 1 && self.quantiles[lower_bucket] == self.quantiles[upper_bucket] {
    //     return bits;
    //   }
    //   let mid_bucket = ((fmin + fmax) / 2.0);
    //   let mid_x;
    //   if mid_bucket < 0 {
    //     mid_x = i64::MIN + ;
    //   }
    //   if mid_x < x {
    //
    //   }
    //   bits.push()
    // }
    // return x.to_le_bytes().to_vec();
  }

  fn terminate(&mut self) -> Option<u8> {
    return None;
  }
}

pub struct Int64Decompressor{}

impl Decompressor for Int64Decompressor {
  fn from_parameters(parameters: Option<&CompressionParams>) -> Int64Decompressor {
    return Int64Decompressor{};
  }

  fn decode(&mut self, bytes: &Vec<u8>) -> Vec<Value> {
    if bytes.len() % 8 != 0 {
      println!("WARNING EXTRA INT BYTES")
    }
    let n = bytes.len() / 8;
    let mut result = Vec::new();
    for i in 0..n {
      let start = i * 8;
      let end = start + 8;
      let slice = &bytes[start..end];
      let val = i64::from_le_bytes(slice.try_into().unwrap());
      result.push(Value::int64_val(val));
    }
    return result;
  }
}
