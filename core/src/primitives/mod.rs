use pancake_db_idl::dml::field_value::Value;

use crate::compression::{Codec, ValueCodec};
use crate::errors::PancakeResult;
use crate::encoding::ByteReader;
use pancake_db_idl::dtype::DataType;

mod bools;
mod floats;
mod ints;
mod string_likes;

pub trait StringLike {
  type Error: std::error::Error;
  fn into_bytes(&self) -> Vec<u8>;
  fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error> where Self: Sized;
}

pub trait Primitive: 'static {
  const DTYPE: DataType;
  fn try_from_value(v: &Value) -> PancakeResult<Self> where Self: Sized;
  fn to_value(&self) -> Value;
  fn new_codec(codec: &str) -> Option<Box<dyn Codec<T=Self>>>;
  fn encode(&self) -> Vec<u8>;
  fn decode(reader: &mut ByteReader) -> PancakeResult<Self> where Self: Sized;

  fn new_value_codec(codec: &str) -> Option<Box<dyn ValueCodec>> where Self: Sized {
    Self::new_codec(codec).map(|c| {
      let c: Box<dyn ValueCodec> = Box::new(c);
      c
    })
  }
}
