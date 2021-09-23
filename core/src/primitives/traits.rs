use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dtype::DataType;

use crate::compression::{Codec, ValueCodec};
use crate::errors::CoreResult;
use std::fmt::Debug;

pub trait Atom: 'static + Copy + Debug + Default {
  const BYTE_SIZE: usize;

  fn to_bytes(&self) -> Vec<u8>;
  fn try_from_bytes(bytes: &[u8]) -> CoreResult<Self> where Self: Sized;
}

pub trait Primitive: 'static + Default {
  type A: Atom;

  const DTYPE: DataType;
  const IS_ATOMIC: bool;

  fn to_value(&self) -> Value;
  fn try_from_value(v: &Value) -> CoreResult<Self> where Self: Sized;

  fn to_atoms(&self) -> Vec<Self::A>;
  fn try_from_atoms(atoms: &[Self::A]) -> CoreResult<Self> where Self: Sized;

  fn new_codec(codec: &str) -> Option<Box<dyn Codec<P=Self>>>;
  fn new_value_codec(codec: &str) -> Option<Box<dyn ValueCodec>> where Self: Sized {
    Self::new_codec(codec).map(|c| {
      let c: Box<dyn ValueCodec> = Box::new(c);
      c
    })
  }
}
