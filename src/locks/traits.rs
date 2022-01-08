use async_trait::async_trait;

use crate::errors::ServerResult;
use crate::ops::traits::ServerOp;
use crate::server::Server;


#[async_trait]
pub trait ServerOpLocks: Send {
  type Key;

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> where Self: Sized;
}

#[async_trait]
pub trait ServerWriteOpLocks: ServerOpLocks {
  async fn traverse(server: &Server) -> Vec<Self::Key>;
}

