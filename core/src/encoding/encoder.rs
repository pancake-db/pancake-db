use std::marker::PhantomData;

use pancake_db_idl::dml::FieldValue;
use pancake_db_idl::dml::field_value::Value;

use crate::errors::{CoreResult, CoreError};
use crate::primitives::{Atom, Primitive};
use super::{NULL_BYTE, ESCAPE_BYTE};

pub trait Encoder: Send + Sync {
  fn encode(&self, values: &[FieldValue]) -> CoreResult<Vec<u8>>;
}

pub struct EncoderImpl<P: Primitive> {
  nested_list_depth: u8,
  _phantom: PhantomData<P>,
}

fn escape_bytes(bytes: &[u8]) -> Vec<u8> {
  let mut res = Vec::new();
  for &b in bytes {
    if b >= NULL_BYTE {
      res.push(ESCAPE_BYTE);
      // we must avoid using the count byte at all so that we can easily read the end
      // of the file without decoding the whole thing, so instead of pushing the byte
      // we escaped, we push its complement
      res.push(!b);
    } else {
      res.push(b);
    }
  }
  res
}

impl<P: Primitive> Encoder for EncoderImpl<P> {
  fn encode(&self, fvs: &[FieldValue]) -> CoreResult<Vec<u8>> {
    let mut res = Vec::new();

    for fv in fvs {
      let maybe_err: CoreResult<()> = match &fv.value {
        Some(value) => {
          let bytes = self.value_bytes(value, 0)?;
          res.extend(bytes);
          Ok(())
        },
        None => {
          res.push(NULL_BYTE);
          Ok(())
        }
      };
      maybe_err?;
    }
    Ok(res)
  }
}

impl<P: Primitive> EncoderImpl<P> {
  pub fn new(escape_depth: u8) -> Self {
    Self {
      nested_list_depth: escape_depth,
      _phantom: PhantomData,
    }
  }

  fn value_bytes(&self, v: &Value, traverse_depth: u8) -> CoreResult<Vec<u8>> {
    if traverse_depth == self.nested_list_depth {
      let atoms = P::try_from_value(v)?.to_atoms();
      if P::IS_ATOMIC {
        Ok(escape_bytes(&atoms[0].to_bytes()))
      } else {
        let mut res = Vec::with_capacity(2 + P::A::BYTE_SIZE * atoms.len());
        res.extend((atoms.len() as u16).to_be_bytes());
        for atom in &atoms {
          res.extend(atom.to_bytes());
        }
        Ok(escape_bytes(&res))
      }
    } else {
      match v {
        Value::list_val(l) => {
          let mut res = Vec::new();
          res.extend(escape_bytes(&(l.vals.len() as u16).to_be_bytes()));
          for val in &l.vals {
            let bytes = self.value_bytes(val.value.as_ref().unwrap(), traverse_depth + 1)?;
            res.extend(bytes);
          }
          Ok(res)
        },
        _ => Err(CoreError::invalid("expected a list to traverse but found atomic type"))
      }
    }
  }
}
