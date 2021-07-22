use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};

use warp::Reply;
use warp::http::StatusCode;
use warp::reject::Reject;

use serde::Serialize;

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

// impl Error for PancakeError {}

impl Reject for PancakeError {}

impl<E> From<E> for PancakeError where E: Error {
  fn from(reason: E) -> Self {
    PancakeError {
      kind: PancakeErrorKind::Internal { explanation: reason.to_string() }
    }
  }
}

pub type PancakeResult<T> = Result<T, PancakeError>;

#[derive(Serialize)]
struct ErrorResponse {
  pub message: String,
}

pub fn pancake_result_into_warp<T: Serialize>(res: PancakeResult<T>) -> Result<Box<dyn Reply>, Infallible> {
  match res {
    Ok(x) => Ok(Box::new(warp::reply::json(&x))),
    Err(e) => {
      let reply = warp::reply::json(&ErrorResponse {
        message: e.to_string(),
      });
      let status_code = match e.kind {
        PancakeErrorKind::DoesNotExist {entity_name: _, value: _} => StatusCode::NOT_FOUND,
        PancakeErrorKind::Invalid {explanation: _} => StatusCode::BAD_REQUEST,
        PancakeErrorKind::Internal {explanation: _} => StatusCode::INTERNAL_SERVER_ERROR,
      };
      Ok(Box::new(warp::reply::with_status(
        reply,
        status_code,
      )))
    }
  }
}

// pub fn warp_recover(r: Rejection) -> Result<impl Reply, Infallible> {
//   let empty_reply = warp::reply();
//   if let Some(e) = r.find::<PancakeError>() {
//     Ok(match e.kind {
//       PancakeErrorKind::DoesNotExist => warp::reply::with_status(
//         empty_reply,
//         StatusCode::NOT_FOUND,
//       ),
//       PancakeErrorKind::Invalid => warp::reply::with_status(
//         empty_reply,
//         StatusCode::BAD_REQUEST,
//       ),
//       PancakeErrorKind::Internal => warp::reply::with_status(
//         empty_reply,
//         StatusCode::INTERNAL_SERVER_ERROR,
//       )
//     })
//   } else {
//     Ok(warp::reply::with_status(
//       empty_reply,
//       StatusCode::INTERNAL_SERVER_ERROR,
//     ))
//   }
// }
