use std::collections::HashMap;

use crate::dirs;
use pancake_db_idl::dml::Row;
use crate::{utils, compression};

use super::Server;
use crate::storage::flush::FlushMetadata;
use crate::storage::compaction::CompactionKey;
use pancake_db_idl::schema::Schema;

impl Server {
  pub async fn write_row(&self, table_name: &String, row: Row) -> Result<(), &'static str> {
    // validate data matches schema
    let schema: Schema = self.schema_cache
      .get_option(table_name)
      .await
      .expect("no schema");
    let mut col_map = HashMap::new();
    for col in &schema.columns {
      col_map.insert(&col.name, col);
    }
    for field in &row.fields {
      let mut maybe_err: Option<&'static str> = None;
      match col_map.get(&field.name) {
        Some(col) => {
          if !utils::dtype_matches_elem(&col.dtype.unwrap(), &field) {
            maybe_err = Some("wrong dtype");
          }
        },
        _ => {
          maybe_err = Some("unknown column");
        },
      };

      if maybe_err.is_some() {
        return Err(maybe_err.unwrap());
      }
    }

    self.staged.add_row(table_name, row).await;
    // here we should really wait for flush
    return Ok(());
  }

  pub async fn flush(&self, table_name: &str) -> Result<(), &'static str> {
    let maybe_rows = self.staged.pop_rows(table_name).await;
    if maybe_rows.is_none() {
      return Ok(());
    }
    let rows = maybe_rows.unwrap();

    let table_name_string = String::from(table_name);
    let schema_future = self.schema_cache
      .get_option(&table_name_string);
    let metadata_future = self.flush_metadata_cache
      .get_option(&table_name_string);

    let metadata = metadata_future
      .await
      .unwrap_or(FlushMetadata::default());
    let mut compaction_futures = HashMap::new();
    for write_version in &metadata.write_versions {
      let compaction_key = CompactionKey {
        table_name: table_name.to_string(),
        version: *write_version,
      };
      compaction_futures.insert(
        write_version,
        Box::pin(self.compaction_cache.get(compaction_key)),
      );
    }
    let schema = schema_future
      .await
      .expect("schema missing for flush");


    let mut field_maps = Vec::new();
    for row in &rows {
      field_maps.push(row.field_map());
    }

    for version in &metadata.write_versions {
      let compaction = compaction_futures.get_mut(version).expect("unreachable").await;
      // let compaction: Compaction = compaction_futures.get_mut(version).expect("unreachable").await;
      for col in &schema.columns {
        let mut compressor = compression::get_compressor(
          &col.dtype.unwrap(),
          compaction.col_compression_params.get(&col.name),
        );

        let bytes: Vec<u8> = field_maps
            .iter()
            .flat_map(|m| compressor.encode(&m[&col.name].value.0.clone().unwrap().value.unwrap()))
            .map(|e| e.to_owned())
            .collect();
        utils::append_to_file(
          &dirs::flush_col_file(&self.dir, table_name, version.clone(), &col.name),
          bytes.as_slice(),
        ).await?;  //TODO optimize
      }
    }

    return self.flush_metadata_cache.increment_n(&String::from(table_name), rows.len()).await;
  }
}
