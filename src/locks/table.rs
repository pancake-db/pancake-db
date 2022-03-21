use async_trait::async_trait;
use tokio::sync::OwnedRwLockWriteGuard;

use crate::errors::{ServerError, ServerResult};
use crate::locks::traits::ServerOpLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::metadata::table::TableMetadata;
use crate::metadata::global::GlobalMetadata;

pub struct GlobalTableReadLocks {
  pub global_meta: GlobalMetadata,
  pub table_meta: TableMetadata,
}

pub struct TableReadLocks {
  pub table_meta: TableMetadata,
}

pub struct TableWriteLocks {
  pub maybe_table_guard: OwnedRwLockWriteGuard<Option<TableMetadata>>,
}

#[async_trait]
impl ServerOpLocks for GlobalTableReadLocks {
  type Key = String;

  async fn execute<Op: ServerOp<Locks=Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> where Self: Sized {
    let locks = GlobalTableReadLocks::obtain(server, &op.get_key()?).await?;
    op.execute_with_locks(server, locks).await
  }
}

impl GlobalTableReadLocks {
  pub async fn obtain(server: &Server, key: &String) -> ServerResult<Self> {
    let global_guard = server.global_metadata_lock.read().await;

    let lock = server.table_metadata_cache.get_lock(key).await?;
    let guard = lock.read().await;
    let maybe_table = guard.clone();
    if maybe_table.is_none() {
      return Err(ServerError::does_not_exist("table", key))
    }

    Ok(GlobalTableReadLocks {
      global_meta: global_guard.clone(),
      table_meta: maybe_table.unwrap(),
    })
  }
}

#[async_trait]
impl ServerOpLocks for TableReadLocks {
  type Key = String;

  async fn execute<Op: ServerOp<Locks=Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> where Self: Sized {
    let table_name = op.get_key()?;
    let lock = server.table_metadata_cache.get_lock(&table_name).await?;
    let guard = lock.read().await;
    let maybe_table = guard.clone();
    if maybe_table.is_none() {
      return Err(ServerError::does_not_exist("table", &table_name))
    }

    let locks = TableReadLocks {
      table_meta: maybe_table.unwrap(),
    };
    op.execute_with_locks(server, locks).await
  }
}

#[async_trait]
impl ServerOpLocks for TableWriteLocks {
  type Key = String;

  async fn execute<Op: ServerOp<Locks=Self>>(
    server: &Server,
    op: &Op,
  ) -> ServerResult<Op::Response> {
    let table_name = op.get_key()?;
    let lock = server.table_metadata_cache.get_lock(&table_name).await?;
    let guard = lock.write_owned().await;
    let locks = TableWriteLocks {
      maybe_table_guard: guard,
    };
    op.execute_with_locks(server, locks).await
  }
}
