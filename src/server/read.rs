use pancake_db_idl::dml::{Field, FieldValue, ReadSegmentColumnRequest, ReadSegmentColumnResponse, Row};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::schema::ColumnMeta;
use protobuf::MessageField;
use tokio::fs;
use warp::{Filter, Rejection, Reply};

use crate::compression;
use crate::dirs;
use crate::storage::compaction::{Compaction, CompactionKey, CompressionParams};
use crate::storage::flush::FlushMetadata;
use crate::utils;

use super::Server;

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
        compaction.col_compressor_names.get(&col.name),
        n,
      ).await;
      println!("VALUE LEN {} vs ROWS LEN {}", values.len(), rows.len());
      for i in 0..n {
        rows[i].fields.push(field_from_elem(&col.name, &values[i]));
      }
    }

    return rows;
  }

  pub fn read_segment_column_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::get()
      .and(warp::path("rest"))
      .and(warp::path("read_segment_column"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::json())
      .and_then(Self::read_segment_column)
  }

  async fn read_segment_column(server: Server, req: ReadSegmentColumnRequest) -> Result<impl Reply, Rejection> {
    let col_name = req.column_name;
    let schema_future = server.schema_cache
      .get_option(&req.table_name);
    let flush_meta_future = server.flush_metadata_cache
      .get(&req.table_name);

    let schema = schema_future
      .await
      .expect("table does not exist");
    let flush_meta = flush_meta_future
      .await;

    let mut valid_col = false;
    for col_meta_item in &schema.columns {
      if col_meta_item.name == col_name {
        valid_col = true;
      }
    }

    if valid_col {
      let compaction = server.compaction_cache
        .get(CompactionKey {
          table_name: req.table_name.clone(),
          version: flush_meta.read_version
        })
        .await;

      // TODO: error handling
      // TODO: pagination
      let compressed_filename = dirs::compact_col_file(
        &server.dir,
        &req.table_name,
        flush_meta.read_version,
        &col_name,
      );
      let uncompressed_filename = dirs::flush_col_file(
        &server.dir,
        &req.table_name,
        flush_meta.read_version,
        &col_name,
      );

      let compressor_name = compaction.col_compressor_names
        .get(&col_name)
        .map(|c| c.clone() as String)
        .unwrap_or("".to_string());

      Ok(warp::reply::json(&ReadSegmentColumnResponse {
        compressor_name,
        compressed_data: utils::read_if_exists(compressed_filename).await.unwrap_or(Vec::new()),
        uncompressed_data: utils::read_if_exists(uncompressed_filename).await.unwrap_or(Vec::new()),
        ..Default::default()
      }))
    } else {
      Err(warp::reject())
    }
  }
}