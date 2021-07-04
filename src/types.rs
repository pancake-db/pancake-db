use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq)]
pub enum DataElem {
  Str(String),
  Int64(i64),
}

impl fmt::Display for DataElem {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let dtype_str;
    let val_str;
    match self {
      DataElem::Str(x) => {
        dtype_str = "Str";
        val_str = x.clone();
      },
      DataElem::Int64(x) => {
        dtype_str = "Int64";
        val_str = x.to_string();
      }
    }
    return write!(f, "{}({})", dtype_str, val_str);
  }
}
