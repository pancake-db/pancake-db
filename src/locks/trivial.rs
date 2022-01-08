use async_trait::async_trait;

use crate::errors::ServerResult;
use crate::locks::traits::ServerOpLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;

pub struct TrivialLocks;

#[async_trait]
impl ServerOpLocks for TrivialLocks {
  type Key = ();

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> where Self: Sized {
    let locks = TrivialLocks {};
    op.execute_with_locks(server, locks).await
  }
}