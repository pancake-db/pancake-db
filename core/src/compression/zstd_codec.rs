use crate::primitives::Primitive;
use crate::errors::CoreResult;

use super::Codec;
use std::marker::PhantomData;

const ZSTD_LEVEL: i32 = 5;

#[derive(Clone, Debug, Default)]
pub struct ZstdCodec<P: Primitive> {
  _phantom: PhantomData<P>,
}

impl<P: Primitive<A=u8>> Codec for ZstdCodec<P> {
  type P = P;

  fn compress_atoms(&self, atoms: &[u8]) -> CoreResult<Vec<u8>> {
    Ok(zstd::encode_all(atoms, ZSTD_LEVEL)?)
  }

  fn decompress_atoms(&self, bytes: &[u8]) -> CoreResult<Vec<u8>> {
    Ok(zstd::decode_all(bytes)?)
  }
}
