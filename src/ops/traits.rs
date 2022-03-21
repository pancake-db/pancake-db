use async_trait::async_trait;
use serde::de::DeserializeOwned;

use crate::{Server, ServerResult};
use crate::locks::traits::ServerOpLocks;

#[async_trait]
pub trait ServerOp: Sync {
  type Locks: ServerOpLocks;
  type Response;

  fn get_key(&self) -> ServerResult<<Self::Locks as ServerOpLocks>::Key>;
  async fn execute_with_locks(
    &self,
    server: &Server,
    locks: Self::Locks,
  ) -> ServerResult<Self::Response> where Self::Locks: 'async_trait;

  async fn execute(&self, server: &Server) -> ServerResult<Self::Response> where Self: Sized {
    <Self::Locks as ServerOpLocks>::execute(server, self).await
  }
}

#[async_trait]
pub trait ServerWriteOp: ServerOp<Locks=<Self as ServerWriteOp>::Locks> {
  type Locks: ServerOpLocks;

  async fn recover(server: &Server, locks: <Self as ServerWriteOp>::Locks) -> ServerResult<()>;
}

#[async_trait::async_trait]
pub trait RestRoute: ServerOp + Send + Sync {
  type Req: DeserializeOwned + Send + Sync;

  const ROUTE_NAME: &'static str;

  fn new_op(req: Self::Req) -> Self;
}
