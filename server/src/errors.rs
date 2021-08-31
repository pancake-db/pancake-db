use std::fmt;
use std::fmt::{Display, Formatter};

use std::error::Error;
use warp::http::StatusCode;

#[derive(Clone, Debug)]
pub struct ServerError {
  message: String,
  pub kind: ServerErrorKind,
}

#[derive(Clone, Debug)]
pub enum ServerErrorKind {
  Invalid, // 400
  DoesNotExist, // 404
  Internal, // 500
}

impl ServerErrorKind {
  pub fn warp_status_code(&self) -> StatusCode {
    match &self {
      ServerErrorKind::Invalid => StatusCode::BAD_REQUEST,
      ServerErrorKind::DoesNotExist => StatusCode::NOT_FOUND,
      ServerErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
  }
}

impl ServerError {
  pub fn does_not_exist(entity_name: &'static str, value: &str) -> ServerError {
    ServerError {
      message: format!("{} with name {} does not exist", entity_name, value),
      kind: ServerErrorKind::DoesNotExist,
    }
  }

  pub fn invalid(explanation: &str) -> ServerError {
    ServerError {
      message: explanation.to_string(),
      kind: ServerErrorKind::Invalid,
    }
  }

  pub fn internal(explanation: &str) -> ServerError {
    ServerError {
      message: explanation.to_string(),
      kind: ServerErrorKind::Internal,
    }
  }
}

impl Display for ServerError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let prefix = match &self.kind {
      ServerErrorKind::Invalid => "invalid request",
      ServerErrorKind::DoesNotExist => "missing",
      ServerErrorKind::Internal => "internal error",
    };
    write!(f, "{}; {}", prefix, self.message)
  }
}

impl<E> From<E> for ServerError where E: Error {
  fn from(reason: E) -> Self {
    ServerError {
      message: reason.to_string(),
      kind: ServerErrorKind::Internal,
    }
  }
}

pub type ServerResult<T> = Result<T, ServerError>;
