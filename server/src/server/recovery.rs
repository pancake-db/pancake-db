use tokio::fs;

use crate::constants::TABLE_METADATA_FILENAME;
use crate::errors::ServerResult;
use crate::ops::drop_table::DropTableOp;
use crate::server::Server;
use crate::storage::Metadata;
use crate::storage::table::TableMetadata;
use crate::utils;

struct TableInfo {
  pub name: String,
  pub meta: TableMetadata,
}

impl Server {
  async fn list_tables(&self) -> ServerResult<Vec<TableInfo>> {
    let mut res = Vec::new();
    let mut read_dir = fs::read_dir(&self.opts.dir).await?;
    while let Ok(Some(entry)) = read_dir.next_entry().await {
      if !entry.file_type().await?.is_dir() {
        continue;
      }
      let path = entry.path();
      let possible_meta_path = path.join(TABLE_METADATA_FILENAME);
      if utils::file_exists(&possible_meta_path).await? {
        for name in path
          .file_name()
          .and_then(|s| s.to_str()) {
          let name = name.to_string();
          let meta = TableMetadata::load(
            &self.opts.dir,
            &name,
          ).await?.unwrap();
          res.push(TableInfo {
            name,
            meta,
          })
        }
      }
    }
    Ok(res)
  }

  pub async fn recover(&self) -> ServerResult<()> {
    log::info!("recovering to clean state");
    for table_info in self.list_tables().await? {
      log::debug!("recovering table {}", table_info.name);
      // 1. Dropped tables
      let TableInfo {
        name: table_name,
        meta: table_meta
      } = table_info;
      if table_meta.dropped {
        DropTableOp::recover(&self, &table_name, &table_meta).await?;
        continue;
      }

      // 2. Compactions

      // 3. Flushes

      // 4. Writes
    }

    Ok(())
  }
}