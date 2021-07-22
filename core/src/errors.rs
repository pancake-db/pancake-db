use std::fmt;
use std::fmt::{Display, Formatter};

use std::error::Error;

#[derive(Clone, Debug)]
pub struct PancakeError {
  pub kind: PancakeErrorKind,
}

#[derive(Clone, Debug)]
pub enum PancakeErrorKind {
  DoesNotExist {entity_name: &'static str, value: String},
  Invalid {explanation: String},
  Internal {explanation: String},
}

impl PancakeError {
  pub fn does_not_exist(entity_name: &'static str, value: &str) -> PancakeError {
    PancakeError {
      kind: PancakeErrorKind::DoesNotExist {
        entity_name,
        value: value.to_string(),
      }
    }
  }

  pub fn invalid(explanation: &str) -> PancakeError {
    PancakeError {
      kind: PancakeErrorKind::Invalid {
        explanation: explanation.to_string()
      }
    }
  }

  pub fn internal(explanation: &str) -> PancakeError {
    PancakeError {
      kind: PancakeErrorKind::Internal {
        explanation: explanation.to_string()
      }
    }
  }
}

impl Display for PancakeError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    match &self.kind {
      PancakeErrorKind::DoesNotExist {entity_name, value} => write!(
        f,
        "{} for {} does not exist",
        entity_name,
        value
      ),
      PancakeErrorKind::Invalid {explanation} => write!(
        f,
        "invalid request; {}",
        explanation
      ),
      PancakeErrorKind::Internal { explanation } => write!(
        f,
        "internal error; {}",
        explanation
      )
    }
  }
}

impl<E> From<E> for PancakeError where E: Error {
  fn from(reason: E) -> Self {
    PancakeError {
      kind: PancakeErrorKind::Internal { explanation: reason.to_string() }
    }
  }
}

pub type PancakeResult<T> = Result<T, PancakeError>;
