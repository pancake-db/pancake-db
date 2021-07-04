use pancake_db_idl::schema::ColumnMeta;
use pancake_db_idl::dml::{Field, FieldValue, Row};
use pancake_db_idl::dml::field_value::Value;
use tokio::fs;

use crate::compression;
use crate::dirs;
use crate::storage::compaction::{Compaction, CompactionKey, CompressionParams};
use crate::storage::flush::FlushMetadata;

use super::Server;
use protobuf::MessageField;

fn field_from_elem(name: &str, elem: &Value) -> Field {
  let mut field_value = FieldValue {
    ..Default::default()
  };
  match elem {
    Value::string_val(x) => field_value.set_string_val(x.to_string()),
    Value::int64_val(x) => field_value.set_int64_val(*x),
    Value::list_val(x) => field_value.set_list_val(x.clone()),
  }
  Field {
    name: name.to_string(),
    value: MessageField::some(field_value),
    ..Default::default()
  }
}

impl Server {
  pub async fn read_col(
    &self,
    table_name: &str,
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    compression_params: Option<&CompressionParams>,
    limit: usize,
  ) -> Vec<Value> {
    // TODO stream, include index
    let mut decompressor = compression::get_decompressor(&col.dtype.unwrap(), compression_params);
    let mut result = Vec::new();
    for col_file in &dirs::col_files(&self.dir, table_name, metadata.read_version, &col.name) {
      match fs::read(col_file).await {
        Ok(bytes) => {
          let end = limit - result.len();
          let decoded = decompressor.decode(&bytes);
          let limited;
          if end < decoded.len() {
            limited = Vec::from(&decoded[0..end]);
          } else {
            limited = decoded;
          }
          result.extend(limited);
        },
        Err(_) => (),
      }
    }
    return result;
  }

  pub async fn read(&self, table_name: &str) -> Vec<Row> {
    // TODO parallel schema/flush read
    let table_name_string = String::from(table_name);
    let schema_future = self.schema_cache
      .get_option(&table_name_string);
    let flush_meta_future = self.flush_metadata_cache
      .get(&table_name_string);

    let schema = schema_future
      .await
      .expect("table does not exist");
    let flush_meta = flush_meta_future
      .await;

    let compaction: Compaction = self.compaction_cache
      .get(CompactionKey {
        table_name: table_name.to_string(),
        version: flush_meta.read_version
      })
      .await;

    let n = flush_meta.n;

    let mut rows = Vec::with_capacity(n);
    if n == 0 {
      return rows;
    }

    for _ in 0..n {
      rows.push(Row { fields: Vec::new(), ..Default::default() });
    }

    for col in &schema.columns {
      let values = self.read_col(
        table_name,
        col,
        &flush_meta,
        compaction.col_compression_params.get(&col.name),
        n,
      ).await;
      println!("VALUE LEN {} vs ROWS LEN {}", values.len(), rows.len());
      for i in 0..n {
        rows[i].fields.push(field_from_elem(&col.name, &values[i]));
      }
    }

    return rows;
  }
}