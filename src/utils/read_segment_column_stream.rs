use pancake_db_idl::dml::{ReadSegmentColumnRequest, ReadSegmentColumnResponse};
use futures::StreamExt;
use futures::stream::BoxStream;
use tonic::Status;

use crate::Server;
use crate::ops::read_segment_column::{ReadSegmentColumnOp, SegmentColumnContinuation};
use crate::ops::traits::ServerOp;

pub type ReadSegmentColumnStream = BoxStream<'static, Result<ReadSegmentColumnResponse, Status>>;

pub fn create_stream(req: ReadSegmentColumnRequest, server: Server) -> ReadSegmentColumnStream {
  let state = ReadSegmentColumnState::new(req, server);
  futures::stream::unfold(state, |mut state: ReadSegmentColumnState| async {
    if state.done {
      return None;
    }

    let op = ReadSegmentColumnOp {
      req: state.req.clone(),
      continuation: state.continuation.clone(),
    };
    let resp = op.execute(&state.server).await;

    let grpc_resp = match &resp {
      Ok(ok_resp) => Ok(ok_resp.resp.clone()),
      Err(e) => Err(e.clone().into()),
    };
    if let Ok(resp) = resp {
      let maybe_continuation = resp.continuation;
      match maybe_continuation {
        Some(continuation) => {
          state.continuation = Some(continuation);
        },
        None => {
          state.continuation = None;
          state.done = true;
        },
      }
    }
    Some((grpc_resp, state))
  }).boxed()
}

struct ReadSegmentColumnState {
  pub server: Server,
  pub req: ReadSegmentColumnRequest,
  pub continuation: Option<SegmentColumnContinuation>,
  pub done: bool,
}

impl ReadSegmentColumnState {
  pub fn new(req: ReadSegmentColumnRequest, server: Server) -> Self {
    Self {
      server,
      req,
      continuation: None,
      done: false,
    }
  }
}
