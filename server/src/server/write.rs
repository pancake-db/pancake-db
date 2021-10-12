use std::convert::Infallible;

use hyper::body::Bytes;
use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse};
use warp::{Filter, Rejection, Reply};

use crate::utils;
use crate::errors::ServerResult;
use crate::locks::segment::SegmentLockKey;
use crate::ops::flush::FlushOp;
use crate::ops::traits::ServerOp;
use crate::ops::write_to_partition::WriteToPartitionOp;

use super::Server;

impl Server {
  pub async fn write_to_partition(&self, req: WriteToPartitionRequest) -> ServerResult<WriteToPartitionResponse> {
    WriteToPartitionOp { req }.execute(&self).await
    // let table_name = &req.table_name;
    // utils::validate_entity_name_for_read("table name", table_name)?;
    // // validate data matches schema
    // let schema = self.schema_cache.get_or_err(table_name).await?;
    //
    // // normalize partition (order fields correctly)
    // let partition = NormalizedPartition::full(&schema, &req.partition)?;
    //
    // utils::validate_rows(&schema, &req.rows)?;
    //
    // let table_partition = PartitionKey {
    //   table_name: table_name.clone(),
    //   partition,
    // };
    // self.staged.add_rows(table_partition, &req.rows).await;
    // // here we should really wait for flush
    // Ok(WriteToPartitionResponse::new())
  }

  pub async fn flush(&self, segment_lock_key: SegmentLockKey) -> ServerResult<()> {
    FlushOp { segment_lock_key }.execute(&self).await
    // let table_name = &partition_key.table_name;
    // let schema = self.schema_cache.get_or_err(table_name)
    //   .await?;
    //
    //
    // utils::create_if_new(&dirs::partition_dir(&self.opts.dir, partition_key)).await?;
    // let segments_meta = self.partition_metadata_cache.get_or_create(partition_key)
    //   .await?;
    //
    // let segment_id = segments_meta.write_segment_id;
    //
    // let maybe_rows = self.staged.pop_rows_for_flush(partition_key, &segment_id)
    //   .await;
    // if maybe_rows.is_none() {
    //   return Ok(());
    // }
    //
    // let rows = maybe_rows.unwrap();
    // log::debug!(
    //   "flushing {} rows for partition {} segment {}",
    //   rows.len(),
    //   partition_key,
    //   segment_id
    // );
    //
    // let segment_key = SegmentKey {
    //   table_name: table_name.clone(),
    //   partition: partition_key.partition.clone(),
    //   segment_id: segment_id.clone(),
    // };
    // let flush_meta = self.segment_metadata_cache
    //   .get(&segment_key)
    //   .await;
    //
    // if flush_meta.n + rows.len() >= self.opts.default_rows_per_segment {
    //   self.partition_metadata_cache.start_new_write_segment(partition_key, &segment_id).await?;
    // }
    //
    // let mut compaction_futures = HashMap::new();
    // for write_version in &flush_meta.write_versions {
    //   let compaction_key = CompactionKey {
    //     table_name: table_name.clone(),
    //     partition: partition_key.partition.clone(),
    //     segment_id: segment_id.clone(),
    //     version: *write_version,
    //   };
    //   compaction_futures.insert(
    //     write_version,
    //     Box::pin(self.compaction_cache.get(compaction_key)),
    //   );
    // }
    //
    // let mut field_maps = Vec::new();
    // for row in &rows {
    //   field_maps.push(row.field_map());
    // }
    //
    // for version in &flush_meta.write_versions {
    //   let compaction_key = segment_key.compaction_key(*version);
    //   for col in &schema.columns {
    //     let field_values = field_maps
    //       .iter()
    //       .map(|m| m.get(&col.name).map(|f| f.value.clone().unwrap()).unwrap_or_default())
    //       .collect::<Vec<FieldValue>>();
    //     let dtype = utils::unwrap_dtype(col.dtype)?;
    //     let encoder = encoding::new_encoder(dtype, col.nested_list_depth as u8);
    //     let bytes = encoder.encode(&field_values)?;
    //     utils::append_to_file(
    //       &dirs::flush_col_file(&self.opts.dir, &compaction_key, &col.name),
    //       &bytes,
    //     ).await?;
    //   }
    // }
    //
    // self.segment_metadata_cache.increment_n(&segment_key, rows.len()).await?;
    // Ok(())
  }

  pub fn write_to_partition_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::post()
      .and(warp::path("write_to_partition"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::bytes())
      .and_then(Self::warp_write_to_partition)
  }

  async fn write_to_partition_from_bytes(&self, body: Bytes) -> ServerResult<WriteToPartitionResponse> {
    let req = utils::parse_pb::<WriteToPartitionRequest>(body)?;
    self.write_to_partition(req).await
  }

  async fn warp_write_to_partition(server: Server, body: Bytes) -> Result<impl Reply, Infallible> {
    utils::pancake_result_into_warp(server.write_to_partition_from_bytes(body).await)
  }
}
