use tokio::fs;

use crate::storage::flush::FlushMetadata;
use crate::dirs;
use crate::utils;

use super::Server;
use std::collections::HashMap;
use pancake_db_idl::schema::{ColumnMeta, Schema};
use crate::storage::compaction::{Compaction, CompactionKey, CompressionParams};
use crate::compression;

const MIN_COMPACT_SIZE: usize = 10;

impl Server {
  pub async fn compact_if_needed(&self, table_name: &str) -> Result<(), &'static str> {
    let table_name_string = String::from(table_name);
    let metadata = self.flush_metadata_cache.get(&table_name_string).await;
    let schema = self.schema_cache
      .get_option(&table_name_string)
      .await
      .expect("schema missing for compact");

    if metadata.write_versions.len() > 1  || metadata.n < MIN_COMPACT_SIZE {
      //already compacting
      return Ok(());
    }

    let existing_compaction = self.compaction_cache.get(CompactionKey {
      table_name: table_name.to_string(),
      version: metadata.read_version,
    }).await;
    if existing_compaction.compacted_n * 2 > metadata.n {
      return Ok(());
    }

    return self.compact(table_name, schema, metadata).await;
  }

  async fn plan_compaction(&self, table_name: &str, schema: &Schema, metadata: &FlushMetadata) -> Result<Compaction, &'static str> {
    let mut col_compression_params = HashMap::new();
    let old_compaction: Compaction = self.compaction_cache.get(CompactionKey {
      table_name: String::from(table_name),
      version: metadata.read_version,
    }).await;

    for col in &schema.columns {
      let compression_params = old_compaction.col_compressor_names.get(&col.name);
      let data = self.read_col(table_name, col, metadata, compression_params, metadata.n).await;
      col_compression_params.insert(
        col.name.clone(),
        compression::compute_compression_params(data, &col.dtype.unwrap())
      );
    }

    return Ok(Compaction {
      compacted_n: metadata.n,
      col_compressor_names: col_compression_params,
    });
  }

  async fn execute_col_compaction(
    &self,
    table_name: &str,
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    compression_params: Option<&CompressionParams>,
    new_version: u64,
  ) {
    let mut compressor = compression::get_compressor(
      &col.dtype.unwrap(),
      compression_params,
    );
    let elems = self.read_col(
      table_name,
      col,
      metadata,
      compression_params,
      metadata.n,
    ).await;
    let bytes: Vec<u8> = elems
      .iter()
      .flat_map(|m| compressor.encode(m))
      .map(|e| e.to_owned())
      .collect();
    utils::append_to_file(
      &dirs::compact_col_file(&self.dir, table_name, new_version, &col.name),
      bytes.as_slice(),
    ).await.expect("could not write to compaction data");  //TODO optimize, error if file exists
  }

  async fn execute_compaction(
    &self,
    table_name: &str,
    schema: &Schema,
    metadata: &FlushMetadata,
    compaction: &Compaction,
    new_version: u64,
  ) {
    for col in &schema.columns {
      let compression_params = compaction.col_compressor_names
        .get(&col.name);
      self.execute_col_compaction(
        table_name,
        col,
        metadata,
        compression_params,
        new_version,
      ).await;
    }
  }

  async fn compact(&self, table_name: &str, schema: Schema, metadata: FlushMetadata) -> Result<(), &'static str> {
    let new_version = metadata.read_version + 1;

    let compaction: Compaction = self.plan_compaction(table_name, &schema, &metadata)
      .await
      .expect("could not plan");

    match fs::create_dir(dirs::version_dir(&self.dir, table_name, new_version)).await {
      Ok(_) => Ok(()),
      Err(_) => Err("create dir error")
    }?;

    let table_name_string = String::from(table_name);
    let compaction_key = CompactionKey {
      table_name: table_name.to_string(),
      version: new_version,
    };
    self.compaction_cache.save(compaction_key, compaction.clone()).await?;
    self.flush_metadata_cache.add_write_version(&table_name_string, new_version).await?;

    self.execute_compaction(table_name, &schema, &metadata, &compaction, new_version).await;

    self.flush_metadata_cache.set_read_version(&table_name_string, new_version).await?;
    Ok(())
  }
}
