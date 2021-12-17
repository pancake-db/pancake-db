use std::fmt;
use std::fmt::{Display, Formatter};
use std::io;

use warp::http::StatusCode;
use pancake_db_core::errors::CoreErrorKind;
use protobuf::ProtobufError;

#[derive(Clone, Debug)]
pub struct ServerError {
  message: String,
  contexts: Vec<String>,
  pub kind: ServerErrorKind,
}

#[derive(Clone, Copy, Debug)]
pub enum ServerErrorKind {
  Invalid, // 400
  DoesNotExist, // 404
  TooManyRequests, // 429
  Internal, // 500
  Corrupt, // 500
}

impl ServerErrorKind {
  pub fn warp_status_code(&self) -> StatusCode {
    match &self {
      ServerErrorKind::Invalid => StatusCode::BAD_REQUEST,
      ServerErrorKind::DoesNotExist => StatusCode::NOT_FOUND,
      ServerErrorKind::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
      ServerErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
      ServerErrorKind::Corrupt => StatusCode::INTERNAL_SERVER_ERROR,
    }
  }
}

impl Display for ServerErrorKind {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let string = match self {
      ServerErrorKind::Invalid => "invalid request",
      ServerErrorKind::DoesNotExist => "missing",
      ServerErrorKind::TooManyRequests => "too many requests",
      ServerErrorKind::Internal => "internal error",
      ServerErrorKind::Corrupt => "corrupt internal data",
    };
    write!(f, "{}", string)
  }
}

impl ServerError {
  fn new(message: impl AsRef<str>, kind: ServerErrorKind) -> ServerError {
    ServerError {
      message: message.as_ref().to_string(),
      contexts: Vec::new(),
      kind,
    }
  }

  pub fn does_not_exist(entity_name: &'static str, value: impl AsRef<str>) -> ServerError {
    ServerError::new(
      format!("{} with name {} does not exist", entity_name, value.as_ref()),
      ServerErrorKind::DoesNotExist,
    )
  }

  pub fn invalid(explanation: impl AsRef<str>) -> ServerError {
    ServerError::new(
      explanation,
      ServerErrorKind::Invalid
    )
  }

  pub fn internal(explanation: impl AsRef<str>) -> ServerError {
    ServerError::new(
      explanation,
      ServerErrorKind::Internal
    )
  }

  pub fn corrupt(explanation: impl AsRef<str>) -> ServerError {
    ServerError::new(
      explanation,
      ServerErrorKind::Corrupt
    )
  }

  pub fn to_client_string(&self) -> String {
    // we want to obscure internal errors for security or something
    match self.kind {
      ServerErrorKind::Internal => "internal error".to_string(),
      _ => format!("{}; {}", self.kind, self.message),
    }
  }

  pub fn add_context(&mut self, context: impl AsRef<str>) {
    self.contexts.push(context.as_ref().to_string());
  }

  pub fn with_context(mut self, context: impl AsRef<str>) -> Self {
    self.add_context(context);
    self
  }
}

impl Display for ServerError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let context_string = if self.contexts.is_empty() {
      "".to_string()
    } else {
      let mut res = "\n\t".to_string();
      res.push_str(&self.contexts.join("\n\t"));
      res
    };
    write!(f, "{}; {}{}", self.kind, self.message, context_string)
  }
}

pub trait ServerUpcastableError: Display {
  fn kind(&self) -> ServerErrorKind;
}

impl<E> From<E> for ServerError where E: ServerUpcastableError {
  fn from(error: E) -> Self {
    ServerError {
      message: error.to_string(),
      contexts: Vec::new(),
      kind: error.kind(),
    }
  }
}

// TODO stop automatically casting this
// instead deal with on case-by-case basis
impl ServerUpcastableError for uuid::Error {
  fn kind(&self) -> ServerErrorKind {
    ServerErrorKind::Corrupt
  }
}

impl ServerUpcastableError for io::Error {
  fn kind(&self) -> ServerErrorKind {
    match self.raw_os_error() {
      Some(24) => ServerErrorKind::TooManyRequests,
      _ => ServerErrorKind::Internal,
    }
  }
}

impl ServerUpcastableError for protobuf::json::ParseError {
  fn kind(&self) -> ServerErrorKind {
    ServerErrorKind::Internal
  }
}

impl ServerUpcastableError for serde_json::Error {
  fn kind(&self) -> ServerErrorKind {
    ServerErrorKind::Internal
  }
}

impl ServerUpcastableError for pancake_db_core::errors::CoreError {
  fn kind(&self) -> ServerErrorKind {
    match self.kind {
      CoreErrorKind::Corrupt => ServerErrorKind::Corrupt,
      CoreErrorKind::Invalid => ServerErrorKind::Internal,
      CoreErrorKind::Other => ServerErrorKind::Internal,
    }
  }
}

impl ServerUpcastableError for ProtobufError {
  fn kind(&self) -> ServerErrorKind {
    ServerErrorKind::Internal
  }
}

impl ServerUpcastableError for std::num::ParseIntError {
  fn kind(&self) -> ServerErrorKind {
    ServerErrorKind::Internal
  }
}

pub type ServerResult<T> = Result<T, ServerError>;

pub trait Contextable {
  fn with_context<StringRef, F>(self, context_fn: F) -> Self
  where StringRef: AsRef<str>, F: FnOnce() -> StringRef;
}

impl<T> Contextable for ServerResult<T> {
  fn with_context<StringRef, F>(mut self, context_fn: F) -> Self
  where StringRef: AsRef<str>, F: FnOnce() -> StringRef {
    match &mut self {
      Ok(_) => (),
      Err(e) => {
        e.add_context(context_fn());
      },
    }
    self
  }
}
