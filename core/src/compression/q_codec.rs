use q_compress::{BitReader, TimestampNs};
use q_compress::compressor::Compressor as RawQCompressor;
use q_compress::decompressor::Decompressor as RawQDecompressor;
use q_compress::types::NumberLike;

use crate::compression::Codec;
use crate::errors::CoreResult;
use crate::primitives::Primitive;

const Q_MAX_DEPTH: u32 = 7;

pub trait QCodec {
  type T: Primitive + NumberLike;
}

macro_rules! qcompressor {
  ($struct_name:ident, $primitive_type:ty) => {
    #[derive(Clone, Debug)]
    pub struct $struct_name {}

    impl Codec for $struct_name {
      type P = $primitive_type;

      fn compress_atoms(&self, primitives: &[$primitive_type]) -> CoreResult<Vec<u8>> {
        let nums = primitives.to_vec();
        let compressor = RawQCompressor::<$primitive_type>::train(
          nums,
          Q_MAX_DEPTH
        )?;
        Ok(compressor.compress(&primitives)?)
      }

      fn decompress_atoms(&self, bytes: &[u8]) -> CoreResult<Vec<$primitive_type>> {
        let mut bit_reader = BitReader::from(bytes.to_vec());
        let decompressor = RawQDecompressor::<$primitive_type>::from_reader(&mut bit_reader)?;
        Ok(decompressor.decompress(&mut bit_reader))
      }
    }
  }
}

qcompressor!(I64QCodec, i64);
qcompressor!(BoolQCodec, bool);
qcompressor!(F64QCodec, f64);
qcompressor!(TimestampNsQCodec, TimestampNs);
