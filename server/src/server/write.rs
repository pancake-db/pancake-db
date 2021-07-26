use std::collections::HashMap;
use std::convert::Infallible;

use pancake_db_core::encoding;
use pancake_db_core::errors::{PancakeError, PancakeResult};
use pancake_db_idl::dml::{FieldValue, WriteToPartitionRequest, WriteToPartitionResponse};
use warp::{Filter, Rejection, Reply};

use crate::{dirs, utils};
use crate::types::{CompactionKey, NormalizedPartition, PartitionKey, SegmentKey};

use super::Server;
use hyper::body::Bytes;

impl Server {
  pub async fn write_to_partition(&self, req: WriteToPartitionRequest) -> PancakeResult<WriteToPartitionResponse> {
    let table_name = &req.table_name;
    // validate data matches schema
    let schema = self.schema_cache.get_result(table_name).await?;

    // normalize partition (order fields correctly)
    let partition = NormalizedPartition::full(&schema, &req.partition)?;

    // validate rows
    let mut col_map = HashMap::new();
    for col in &schema.columns {
      col_map.insert(&col.name, col);
    }
    for row in &req.rows {
      for field in &row.fields {
        let mut maybe_err: PancakeResult<()> = Ok(());
        match col_map.get(&field.name) {
          Some(col) => {
            if !utils::dtype_matches_field(&col.dtype.unwrap(), &field) {
              maybe_err = Err(PancakeError::invalid("wrong dtype"));
            }
          },
          _ => {
            maybe_err = Err(PancakeError::invalid("unknown column"));
          },
        };

        maybe_err?;
      }
    }

    let table_partition = PartitionKey {
      table_name: table_name.clone(),
      partition,
    };
    self.staged.add_rows(table_partition, &req.rows).await;
    // here we should really wait for flush
    Ok(WriteToPartitionResponse::new())
  }

  pub async fn flush(&self, partition_key: &PartitionKey) -> PancakeResult<()> {
    let table_name = &partition_key.table_name;
    let schema = self.schema_cache.get_result(table_name)
      .await?;

    utils::create_if_new(&dirs::partition_dir(&self.dir, partition_key)).await?;
    let segments_meta = self.segments_metadata_cache.get_or_create(partition_key)
      .await;

    let segment_id = segments_meta.write_segment_id;

    let maybe_rows = self.staged.pop_rows_for_flush(partition_key, &segment_id)
      .await;
    if maybe_rows.is_none() {
      return Ok(());
    }

    let rows = maybe_rows.unwrap();

    let segment_key = SegmentKey {
      table_name: table_name.clone(),
      partition: partition_key.partition.clone(),
      segment_id: segment_id.clone(),
    };
    let flush_meta = self.flush_metadata_cache
      .get(&segment_key)
      .await;

    let mut compaction_futures = HashMap::new();
    for write_version in &flush_meta.write_versions {
      let compaction_key = CompactionKey {
        table_name: table_name.clone(),
        partition: partition_key.partition.clone(),
        segment_id: segment_id.clone(),
        version: *write_version,
      };
      compaction_futures.insert(
        write_version,
        Box::pin(self.compaction_cache.get(compaction_key)),
      );
    }

    let mut field_maps = Vec::new();
    for row in &rows {
      field_maps.push(row.field_map());
    }

    for version in &flush_meta.write_versions {
      let compaction_key = segment_key.compaction_key(*version);
      for col in &schema.columns {
        let field_values = field_maps
          .iter()
          .map(|m| m.get(&col.name).map(|f| f.value.clone().unwrap()).unwrap_or_default())
          .collect::<Vec<FieldValue>>();
        let bytes = encoding::encode(&field_values, col.nested_list_depth as u8)?;
        utils::append_to_file(
          &dirs::flush_col_file(&self.dir, &compaction_key, &col.name),
          &bytes,
        ).await?;
      }
    }

    self.flush_metadata_cache.increment_n(&segment_key, rows.len()).await?;
    Ok(())
  }

  pub fn write_to_partition_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path("write_to_partition"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::warp_write_to_partition)
  }

  async fn write_to_partition_from_bytes(&self, body: Bytes) -> PancakeResult<WriteToPartitionResponse> {
    let req = utils::parse_pb::<WriteToPartitionRequest>(body)?;
    self.write_to_partition(req).await
  }

  async fn warp_write_to_partition(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    utils::pancake_result_into_warp(server.write_to_partition_from_bytes(body).await)
  }
}
