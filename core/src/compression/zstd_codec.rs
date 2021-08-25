use crate::primitives::Primitive;
use crate::encoding;
use crate::primitives::StringLike;
use crate::errors::CoreResult;

use super::Codec;

const ZSTD_LEVEL: i32 = 5;

pub trait ZstdCodec {
  type T: Primitive + StringLike;
}

macro_rules! zstdcodec {
  ($struct_name:ident, $primitive_type:ty) => {
    #[derive(Clone, Debug)]
    pub struct $struct_name {}

    impl Codec for $struct_name {
      type T = $primitive_type;

      fn compress_primitives(&self, values: &[$primitive_type]) -> CoreResult<Vec<u8>> {
        let raw_bytes = values.iter()
          .flat_map(|p| encoding::string_like_atomic_value_bytes(p))
          .collect::<Vec<u8>>();
        Ok(zstd::encode_all(&*raw_bytes, ZSTD_LEVEL)?)
      }

      fn decompress_primitives(&self, bytes: &[u8]) -> CoreResult<Vec<$primitive_type>> {
        let decompressed_bytes = zstd::decode_all(bytes)?;
        encoding::decode_string_likes(&decompressed_bytes)
      }
    }
  }
}

zstdcodec!(StringZstdCodec, String);
zstdcodec!(BytesZstdCodec, Vec<u8>);
