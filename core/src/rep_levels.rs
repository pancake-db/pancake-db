use pancake_db_idl::dml::{FieldValue, RepeatedFieldValue};
use pancake_db_idl::dml::field_value::Value;
use q_compress::U32Compressor;

use crate::errors::{CoreError, CoreResult};
use crate::primitives::{Atom, Primitive};

const REPETITION_LEVEL_Q_COMPRESSION_LEVEL: u32 = 6;

#[derive(Default)]
pub struct RepLevelsAndAtoms<A: Atom> {
  pub levels: Vec<u8>,
  pub atoms: Vec<A>,
}

impl<A: Atom> RepLevelsAndAtoms<A> {
  pub fn extend(&mut self, other: &RepLevelsAndAtoms<A>) {
    self.levels.extend(&other.levels);
    self.atoms.extend(&other.atoms);
  }
}

pub struct RepLevelsAndBytes {
  pub levels: Vec<u8>,
  pub remaining_bytes: Vec<u8>,
}

pub fn extract_levels_and_atoms<P: Primitive>(
  fvs: &[FieldValue],
  schema_depth: u8,
) -> CoreResult<RepLevelsAndAtoms<P::A>> {
  let mut res = RepLevelsAndAtoms::<P::A>::default();
  for fv in fvs {
    let sub_levels_and_atoms = extract_single_levels_and_atoms::<P>(fv, schema_depth, 0)?;
    res.extend(&sub_levels_and_atoms);
  }
  Ok(res)
}

pub fn extract_single_levels_and_atoms<P: Primitive>(
  fv: &FieldValue,
  schema_depth: u8,
  traverse_depth: u8
) -> CoreResult<RepLevelsAndAtoms<P::A>> {
  match &fv.value {
    None => {
      if traverse_depth != 0 {
        return Err(CoreError::invalid("null value found in nested position"));
      }

      Ok(RepLevelsAndAtoms {
        levels: vec![0],
        atoms: vec![],
      })
    },
    Some(Value::list_val(repeated)) => {
      if traverse_depth >= schema_depth {
        return Err(CoreError::invalid("traversed to deeper than schema depth"));
      }

      let mut res = RepLevelsAndAtoms::default();
      for fv in &repeated.vals {
        let sub_levels_and_atoms = extract_single_levels_and_atoms::<P>(
          fv,
          traverse_depth + 1,
          schema_depth
        )?;
        res.extend(&sub_levels_and_atoms);
      }
      res.levels.push(traverse_depth + 1);
      Ok(res)
    },
    Some(v) => {
      if traverse_depth != schema_depth {
        return Err(CoreError::invalid(
          &format!(
            "traverse depth of {} does not match schema depth of {}",
            traverse_depth,
            schema_depth
          )
        ))
      }

      let atoms = P::try_from_value(v)?.to_atoms();
      let res = if P::IS_ATOMIC {
        RepLevelsAndAtoms {
          levels: vec![schema_depth + 1],
          atoms: vec![atoms[0]],
        }
      } else {
        let mut levels = atoms.iter()
          .map(|_| schema_depth + 2)
          .collect::<Vec<u8>>();
        levels.push(schema_depth + 1);
        RepLevelsAndAtoms {
          levels,
          atoms
        }
      };

      Ok(res)
    },
  }
}

// fn get_repetition_levels(
//   value: &FieldValue,
//   traverse_depth: u8,
//   schema_depth: u8
// ) -> CoreResult<Vec<u8>> {
//   match &value.value {
//     None => {
//       if traverse_depth != 0 {
//         return Err(CoreError::invalid("null value found in nested position"));
//       }
//
//       Ok(vec![0])
//     },
//     Some(Value::list_val(repeated)) => {
//       if traverse_depth >= schema_depth {
//         return Err(CoreError::invalid("traversed to deeper than schema depth"));
//       }
//
//       let mut res = Vec::new();
//       for fv in &repeated.vals {
//         res.extend(get_repetition_levels(fv, traverse_depth + 1, schema_depth)?)
//       }
//       res.push(traverse_depth + 1);
//       Ok(res)
//     },
//     Some(v) => {
//       if traverse_depth != schema_depth {
//         return Err(CoreError::invalid(
//           &format!(
//             "traverse depth of {} does not match schema depth of {}",
//             traverse_depth,
//             schema_depth
//           )
//         ))
//       }
//
//       Ok(match v {
//         // handle inherently nested types
//         Value::string_val(x) => {
//           let mut res = vec![traverse_depth + 2].repeat(x.len());
//           res.push(traverse_depth + 1);
//           res
//         },
//         Value::bytes_val(x) => {
//           let mut res = vec![traverse_depth + 2].repeat(x.len());
//           res.push(traverse_depth + 1);
//           res
//         },
//         // all others are inherently atomic types
//         Value::int64_val(_) => vec![schema_depth + 1],
//         Value::bool_val(_) => vec![schema_depth + 1],
//         Value::float64_val(_) => vec![schema_depth + 1],
//         Value::timestamp_val(_) => vec![schema_depth + 1],
//       })
//     }
//   }
// }
//
// // 0 for null,
// // 1..n+1 for "next field value", "next field value in list", "... in sublist", ..., "next atom"
// fn get_multi_repetition_levels(values: &[FieldValue], schema_depth: u8) -> CoreResult<Vec<u32>> {
//   let mut res = Vec::new();
//   for fv in values {
//     res.extend(
//       get_repetition_levels(fv, 0, schema_depth)?.iter()
//         .map(|&l| l as u32)
//     )
//   }
//   Ok(res)
// }

pub fn compress_rep_levels(rep_levels: Vec<u8>) -> CoreResult<Vec<u8>> {
  let rep_levels = rep_levels.iter().map(|&l| l as u32).collect::<Vec<u32>>();
  let compressor = U32Compressor::train(
    rep_levels.clone(),
    REPETITION_LEVEL_Q_COMPRESSION_LEVEL
  )?;
  Ok(compressor.compress(&rep_levels)?)
}

// fn get_values(fv: &FieldValue) -> Vec<Value> {
//   match &fv.value {
//     None => Vec::new(),
//     Some(Value::list_val(repeated)) => {
//       repeated.vals.iter()
//         .flat_map(get_values)
//         .collect()
//     },
//     _ => vec![fv.value.clone().unwrap()]
//   }
// }
//
// fn get_multi_values(values: &[FieldValue]) -> Vec<Value> {
//   values.iter()
//     .flat_map(get_values)
//     .collect()
// }

pub struct AtomNester<P: Primitive> {
  rep_levels: Vec<u8>,
  atoms: Vec<P::A>,
  schema_depth: u8,
  i: usize,
  j: usize,
}

impl<P: Primitive> AtomNester<P> {
  pub fn from_levels_and_values(rep_levels: Vec<u8>, atoms: Vec<P::A>, schema_depth: u8) -> Self {
    AtomNester {
      rep_levels,
      atoms,
      schema_depth,
      i: 0,
      j: 0,
    }
  }

  fn nested_field_value(&mut self, traverse_depth: u8) -> CoreResult<FieldValue> {
    let mut level = self.rep_levels[self.i];
    if traverse_depth == 0 && level == 0 {
      //null
      self.i += 1;
      Ok(FieldValue::new())
    } else if traverse_depth < self.schema_depth {
      //list
      let mut res = Vec::new();
      while level != traverse_depth + 1 {
        res.push(self.nested_field_value(traverse_depth + 1)?);
        level = self.rep_levels[self.i];
      }

      self.i += 1;
      Ok(FieldValue {
        value: Some(Value::list_val(RepeatedFieldValue {
          vals: res,
          ..Default::default()
        })),
        ..Default::default()
      })
    } else if traverse_depth == self.schema_depth {
      let start = self.j;
      if P::IS_ATOMIC {
        self.i += 1;
        self.j += 1;
      } else {
        while level != self.schema_depth + 1 {
          // maybe we should check level == schema_depth + 2
          self.i += 1;
          self.j += 1;
          level = self.rep_levels[self.i];
        }
        self.i += 1;
      };
      let atoms = &self.atoms[start..self.j];
      let value = P::try_from_atoms(atoms)?.to_value();
      Ok(FieldValue {
        value: Some(value),
        ..Default::default()
      })
    } else {
      Err(CoreError::corrupt("invalid repetition level found"))
    }
  }

  pub fn nested_field_values(&mut self) -> CoreResult<Vec<FieldValue>> {
    let mut res = Vec::new();
    while self.i < self.rep_levels.len() {
      res.push(self.nested_field_value(0)?);
    }
    Ok(res)
  }
}
