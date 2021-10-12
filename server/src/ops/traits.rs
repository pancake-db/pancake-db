use async_trait::async_trait;

use crate::errors::ServerResult;
use crate::locks::traits::ServerOpLocks;
use crate::server::Server;

#[async_trait]
pub trait ServerOp<Locks: ServerOpLocks> {
  type Response;

  fn get_key(&self) -> <Locks as ServerOpLocks>::Key;
  async fn execute_with_locks(&self, server: &Server, locks: Locks) -> ServerResult<Self::Response>;

  async fn execute(&self, server: &Server) -> ServerResult<Self::Response> {
    Locks::execute(server, &self).await
  }
}

#[async_trait]
pub trait ServerWriteOp<Locks: ServerOpLocks>: ServerOp<Locks> {
  async fn recover(server: &Server, locks: Locks) -> ServerResult<()>;
}
