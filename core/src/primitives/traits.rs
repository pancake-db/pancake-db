use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;

use crate::compression::{Codec, ValueCodec};
use crate::encoding::ByteReader;
use crate::errors::CoreResult;
use std::fmt::Debug;

pub trait StringLike {
  type Error: std::error::Error;
  fn into_bytes(&self) -> Vec<u8>;
  fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error> where Self: Sized;
}

pub trait Atom: 'static + Copy + Debug + Default {}

pub trait Primitive: 'static + Default {
  type A: Atom;

  const DTYPE: DataType;
  const IS_ATOMIC: bool;

  fn to_value(&self) -> Value;
  fn try_from_value(v: &Value) -> CoreResult<Self> where Self: Sized;

  fn to_atoms(&self) -> Vec<Self::A>;
  fn try_from_atoms(atoms: &[Self::A]) -> CoreResult<Self> where Self: Sized;

  fn encode(&self) -> Vec<u8>;
  fn decode(reader: &mut ByteReader) -> CoreResult<Self> where Self: Sized;

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<P=Self>>>;
  fn new_value_codec(codec: &str) -> Option<Box<dyn ValueCodec>> where Self: Sized {
    Self::new_codec(codec).map(|c| {
      let c: Box<dyn ValueCodec> = Box::new(c);
      c
    })
  }
}
