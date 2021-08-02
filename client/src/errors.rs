use hyper::StatusCode;
use std::fmt;
use std::fmt::{Display, Formatter};
use protobuf::json::{ParseError, PrintError};
use hyper::http::uri::InvalidUri;
use std::string::FromUtf8Error;
use pancake_db_core::errors::PancakeError;

pub trait OtherUpcastable: std::error::Error {}
impl OtherUpcastable for FromUtf8Error {}
impl OtherUpcastable for hyper::Error {}
impl OtherUpcastable for InvalidUri {}
impl OtherUpcastable for hyper::http::Error {}
impl OtherUpcastable for serde_json::Error {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
  pub message: String,
  pub kind: ErrorKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
  Http { status: StatusCode },
  Other,
}

impl Display for ErrorKind {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let s = match &self {
      ErrorKind::Http {status} => format!("HTTP error {}", status),
      ErrorKind::Other => format!("client-side error"),
    };
    f.write_str(&s)
  }
}

impl Error {
  pub fn http(status: StatusCode, message: &str) -> Self {
    Error {
      message: message.to_string(),
      kind: ErrorKind::Http {status}
    }
  }
}

impl Display for Error {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "PancakeDB {}, message: {}",
      self.kind,
      self.message,
    )
  }
}

impl<T> From<T> for Error where T: OtherUpcastable {
  fn from(e: T) -> Error {
    Error {
      message: e.to_string(),
      kind: ErrorKind::Other,
    }
  }
}

impl From<PrintError> for Error {
  fn from(e: PrintError) -> Error {
    Error {
      message: format!("{:?}", e),
      kind: ErrorKind::Other,
    }
  }
}

impl From<ParseError> for Error {
  fn from(e: ParseError) -> Error {
    Error {
      message: format!("{:?}", e),
      kind: ErrorKind::Other,
    }
  }
}

impl From<PancakeError> for Error {
  fn from(e: PancakeError) -> Error {
    Error {
      message: format!("{}", e),
      kind: ErrorKind::Other,
    }
  }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
