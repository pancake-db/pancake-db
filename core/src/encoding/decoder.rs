use std::marker::PhantomData;

use pancake_db_idl::dml::{FieldValue, RepeatedFieldValue};
use pancake_db_idl::dml::field_value::Value;

use crate::encoding::byte_reader::ByteReader;
use crate::errors::{CoreError, CoreResult};
use crate::primitives::{Atom, Primitive};
use crate::utils;
use super::{NULL_BYTE, COUNT_BYTE};
use crate::rep_levels::RepLevelsAndAtoms;

pub trait Decodable<P: Primitive> {
  fn handle_atoms(atoms: Vec<P::A>, depth: u8) -> CoreResult<Self> where Self: Sized;
  fn handle_null() -> Self where Self: Sized;
  fn combine(outputs: Vec<Self>, depth: u8) -> Self where Self: Sized;
}

impl<P: Primitive> Decodable<P> for FieldValue {
  fn handle_atoms(atoms: Vec<P::A>, _: u8) -> CoreResult<FieldValue> {
    let value = P::try_from_atoms(&atoms)?.to_value();
    Ok(FieldValue {
      value: Some(value),
      ..Default::default()
    })
  }

  fn handle_null() -> FieldValue {
    FieldValue::new()
  }

  fn combine(outputs: Vec<FieldValue>, _: u8) -> FieldValue {
    let repeated = RepeatedFieldValue {
      vals: outputs,
      ..Default::default()
    };
    FieldValue {
      value: Some(Value::list_val(repeated)),
      ..Default::default()
    }
  }
}

impl<P: Primitive> Decodable<P> for RepLevelsAndAtoms<P::A> {
  fn handle_atoms(atoms: Vec<P::A>, depth: u8) -> CoreResult<RepLevelsAndAtoms<P::A>> {
    let levels = if P::IS_ATOMIC {
      vec![depth + 1]
    } else {
      let mut res = vec![depth + 2; atoms.len()];
      res.push(depth + 1);
      res
    };
    Ok(RepLevelsAndAtoms {
      levels,
      atoms,
    })
  }

  fn handle_null() -> RepLevelsAndAtoms<P::A> {
    RepLevelsAndAtoms {
      levels: vec![0],
      atoms: vec![],
    }
  }

  fn combine(outputs: Vec<RepLevelsAndAtoms<P::A>>, depth: u8) -> RepLevelsAndAtoms<P::A> {
    let mut res = RepLevelsAndAtoms::<P::A>::default();
    for output in &outputs {
      res.extend(output);
    }
    res.levels.push(depth + 1);
    res
  }
}

pub trait Decoder<Output> {
  fn decode_limited(&self, bytes: &[u8], limit: usize) -> CoreResult<Vec<Output>>;
  fn decode(&self, bytes: &[u8]) -> CoreResult<Vec<Output>> {
    self.decode_limited(bytes, usize::MAX)
  }
}

pub struct DecoderImpl<P: Primitive, H> where H: Decodable<P> {
  nested_list_depth: u8,
  _phantom_p: PhantomData<P>,
  _phantom_h: PhantomData<H>,
}

impl<P: Primitive, H> Decoder<H> for DecoderImpl<P, H> where H: Decodable<P> {
  fn decode_limited(
    &self,
    bytes: &[u8],
    limit: usize
  ) -> CoreResult<Vec<H>> {
    let mut res = Vec::new();
    let mut reader = ByteReader::new(bytes);
    while !reader.complete() && res.len() < limit {
      let b0 = reader.read_one()?;
      if b0 == NULL_BYTE {
        res.push(H::handle_null());
      } else if b0 == COUNT_BYTE {
        let count_bytes = utils::try_byte_array::<4>(reader.read_n(4)?)?;
        let count = u32::from_be_bytes(count_bytes) as usize;
        if res.is_empty() {
          for _ in 0..count {
            res.push(H::handle_null());
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

impl<P: Primitive, H> DecoderImpl<P, H> where H: Decodable<P> {
  pub fn new(escape_depth: u8) -> Self {
    Self {
      nested_list_depth: escape_depth,
      _phantom_p: PhantomData,
      _phantom_h: PhantomData,
    }
  }

  fn decode_value(&self, reader: &mut ByteReader, current_depth: u8) -> CoreResult<H> {
    if current_depth == self.nested_list_depth {
      let atoms = if P::IS_ATOMIC {
        let bytes = reader.unescaped_read_n(P::A::BYTE_SIZE)?;
        vec![P::A::try_from_bytes(&bytes)?]
      } else {
        let len = reader.unescaped_read_u16()? as usize;
        let mut atoms = Vec::with_capacity(len);
        for _ in 0..len {
          let bytes = reader.unescaped_read_n(P::A::BYTE_SIZE)?;
          atoms.push(P::A::try_from_bytes(&bytes)?);
        }
        atoms
      };
      H::handle_atoms(atoms, current_depth)
    } else {
      let len = reader.unescaped_read_u16()? as usize;
      let mut outputs = Vec::with_capacity(len);
      for _ in 0..len {
        let child = self.decode_value(reader, current_depth + 1)?;
        outputs.push(child);
      }
      Ok(H::combine(outputs, current_depth))
    }
  }
}
