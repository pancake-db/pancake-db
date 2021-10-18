use std::fmt;
use std::fmt::{Display, Formatter};
use std::string::FromUtf8Error;

use hyper::http::uri::InvalidUri;
use hyper::StatusCode;
use protobuf::json::{ParseError, PrintError};

use pancake_db_core::errors::CoreError;

pub trait OtherUpcastable: std::error::Error {}
impl OtherUpcastable for FromUtf8Error {}
impl OtherUpcastable for hyper::Error {}
impl OtherUpcastable for InvalidUri {}
impl OtherUpcastable for hyper::http::Error {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientError {
  pub message: String,
  pub kind: ClientErrorKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientErrorKind {
  Http { status: StatusCode },
  Other,
}

impl Display for ClientErrorKind {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let s = match &self {
      ClientErrorKind::Http {status} => format!("HTTP error {}", status),
      ClientErrorKind::Other => format!("client-side error"),
    };
    f.write_str(&s)
  }
}

impl ClientError {
  pub fn http(status: StatusCode, message: &str) -> Self {
    ClientError {
      message: message.to_string(),
      kind: ClientErrorKind::Http {status}
    }
  }

  pub fn other(message: String) -> Self {
    ClientError {
      message,
      kind: ClientErrorKind::Other,
    }
  }
}

impl Display for ClientError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "PancakeDB {}, message: {}",
      self.kind,
      self.message,
    )
  }
}

impl<T> From<T> for ClientError where T: OtherUpcastable {
  fn from(e: T) -> ClientError {
    ClientError {
      message: e.to_string(),
      kind: ClientErrorKind::Other,
    }
  }
}

impl From<PrintError> for ClientError {
  fn from(e: PrintError) -> ClientError {
    ClientError {
      message: format!("{:?}", e),
      kind: ClientErrorKind::Other,
    }
  }
}

impl From<ParseError> for ClientError {
  fn from(e: ParseError) -> ClientError {
    ClientError {
      message: format!("{:?}", e),
      kind: ClientErrorKind::Other,
    }
  }
}

impl From<CoreError> for ClientError {
  fn from(e: CoreError) -> ClientError {
    ClientError {
      message: e.to_string(),
      kind: ClientErrorKind::Other,
    }
  }
}

impl std::error::Error for ClientError {}

pub type ClientResult<T> = Result<T, ClientError>;
