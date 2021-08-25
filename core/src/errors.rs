use std::fmt;
use std::fmt::{Display, Formatter};
use std::error::Error;
use std::array::TryFromSliceError;
use q_compress::errors::QCompressError;

pub trait OtherUpcastable: Error {}
impl OtherUpcastable for TryFromSliceError {}
impl OtherUpcastable for std::io::Error {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreErrorKind {
  Invalid,
  Other,
  Corrupt,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreError {
  message: String,
  pub kind: CoreErrorKind,
}

impl Error for CoreError {}

impl CoreError {
  fn create(explanation: &str, kind: CoreErrorKind) -> CoreError {
    CoreError {
      message: explanation.to_string(),
      kind,
    }
  }

  pub fn invalid(explanation: &str) -> CoreError {
    CoreError::create(explanation, CoreErrorKind::Invalid)
  }

  pub fn corrupt(explanation: &str) -> CoreError {
    CoreError::create(explanation, CoreErrorKind::Corrupt)
  }
}

impl Display for CoreError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    match &self.kind {
      CoreErrorKind::Invalid => write!(
        f,
        "invalid input; {}",
        self.message
      ),
      CoreErrorKind::Other => write!(
        f,
        "{}",
        self.message
      ),
      CoreErrorKind::Corrupt => write!(
        f,
        "corrupt data or incorrect decoder/decompressor; {}",
        self.message
      )
    }
  }
}

impl<T> From<T> for CoreError where T: OtherUpcastable {
  fn from(e: T) -> CoreError {
    CoreError {
      message: e.to_string(),
      kind: CoreErrorKind::Other,
    }
  }
}

impl From<QCompressError> for CoreError {
  fn from(e: QCompressError) -> CoreError {
    let kind = match e {
      QCompressError::HeaderDtypeError {header_byte: _, decompressor_byte: _} => CoreErrorKind::Corrupt,
      QCompressError::MagicHeaderError {header: _} => CoreErrorKind::Corrupt,
      _ => CoreErrorKind::Other,
    };
    CoreError {
      message: e.to_string(),
      kind,
    }
  }
}

pub type CoreResult<T> = Result<T, CoreError>;
