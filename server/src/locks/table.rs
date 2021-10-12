use async_trait::async_trait;
use pancake_db_idl::schema::Schema;
use tokio::sync::RwLockReadGuard;

use crate::errors::{ServerResult, ServerError};
use crate::locks::traits::{ServerOpLocks, ServerWriteOpLocks};
use crate::server::Server;
use crate::ops::traits::ServerOp;

pub struct TableReadLocks {
  pub schema: Schema,
}

pub struct TableWriteLocks<'a> {
  pub maybe_schema: &'a mut Option<Schema>,
}

#[async_trait]
impl ServerOpLocks for TableReadLocks {
  type Key = String;

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> where Self: Sized {
    let table_name = op.get_key();
    let schema_lock = server.schema_cache.get_lock(&table_name).await?;
    let guard = schema_lock.read().await;
    let maybe_schema = guard.clone();
    if maybe_schema.is_none() {
      return Err(ServerError::does_not_exist("schema", &table_name))
    }

    let locks = TableReadLocks {
      schema: maybe_schema.unwrap(),
    };
    op.execute_with_locks(server, locks).await
  }
}

#[async_trait]
impl<'a> ServerOpLocks for TableWriteLocks<'a> {
  type Key = String;

  async fn execute<Op: ServerOp<Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> {
    let table_name = op.get_key();
    let schema_lock = server.schema_cache.get_lock(&table_name).await?;
    let mut guard = schema_lock.write().await;
    let locks = TableWriteLocks {
      maybe_schema: &mut *guard,
    };
    op.execute_with_locks(server, locks).await
  }
}
