use std::convert::Infallible;

use pancake_db_core::compression;
use pancake_db_core::encoding;
use pancake_db_core::errors::{PancakeError, PancakeResult};
use pancake_db_idl::dml::{FieldValue, ListSegmentsRequest, ListSegmentsResponse, PartitionField, ReadSegmentColumnRequest, ReadSegmentColumnResponse, Segment};
use pancake_db_idl::schema::{ColumnMeta, PartitionMeta};
use protobuf::MessageField;
use tokio::fs;
use warp::{Filter, Rejection, Reply};

use crate::dirs;
use crate::storage::compaction::CompressionParams;
use crate::storage::flush::FlushMetadata;
use crate::types::{NormalizedPartition, PartitionKey, SegmentKey};
use crate::utils;

use super::Server;

impl Server {
  pub async fn read_compact_col(
    &self,
    segment_key: &SegmentKey,
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    compression_params: Option<&CompressionParams>,
    limit: usize,
  ) -> PancakeResult<Vec<FieldValue>> {
    let compaction_key = segment_key.compaction_key(metadata.read_version);
    let path = dirs::compact_col_file(&self.dir, &compaction_key, &col.name);
    let bytes = utils::read_or_empty(&path).await?;
    if bytes.is_empty() {
      Ok(Vec::new())
    } else {
      let decompressor = compression::get_decompressor(col.dtype.unwrap(), compression_params)?;
      let decoded = decompressor.decompress(&bytes, col)?;
      let limited= if limit < decoded.len() {
        Vec::from(&decoded[0..limit])
      } else {
        decoded
      };
      Ok(limited)
    }
  }

  pub async fn read_flush_col(
    &self,
    segment_key: &SegmentKey,
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    limit: usize,
  ) -> PancakeResult<Vec<FieldValue>> {
    let compaction_key = segment_key.compaction_key(metadata.read_version);
    let path = dirs::flush_col_file(&self.dir, &compaction_key, &col.name);
    let bytes = utils::read_or_empty(&path).await?;
    if bytes.is_empty() {
      Ok(Vec::new())
    } else {
      let decoded = encoding::decode(&bytes, col)?;
      let limited;
      if limit < decoded.len() {
        limited = Vec::from(&decoded[0..limit]);
      } else {
        limited = decoded;
      }
      Ok(limited)
    }
  }

  pub async fn read_col(
    &self,
    segment_key: &SegmentKey,
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    compression_params: Option<&CompressionParams>,
    limit: usize,
  ) -> PancakeResult<Vec<FieldValue>> {
    let mut values = self.read_compact_col(
      segment_key,
      col,
      metadata,
      compression_params,
      limit
    ).await?;
    if values.len() < limit {
      values.extend(self.read_flush_col(
        segment_key,
        col,
        metadata,
        limit - values.len()
      ).await?);
    }
    Ok(values)
  }

  async fn list_subpartitions(
    &self,
    table_name: &str,
    parent: &[PartitionField],
    meta: &PartitionMeta,
  ) -> PancakeResult<Vec<PartitionField>> {
    let dir = dirs::partition_dir(
      &self.dir,
      &PartitionKey {
        table_name: table_name.to_string(),
        partition: NormalizedPartition::partial(parent)?
      }
    );
    let mut res = Vec::new();
    let mut read_dir = fs::read_dir(&dir).await?;
    while let Ok(Some(entry)) = read_dir.next_entry().await {
      if !entry.file_type().await.unwrap().is_dir() {
        continue;
      }

      let fname = entry.file_name();
      let parts = fname
        .to_str()
        .expect("how can os string not be str")
        .split('=')
        .collect::<Vec<&str>>();

      if parts.len() != 2 {
        continue;
      }
      if *parts[0] != meta.name {
        continue;
      }
      let parsed = utils::partition_field_from_string(
        &meta.name,
        parts[1],
        meta.dtype.unwrap(),
      )?;
      res.push(parsed);
    }
    Ok(res)
  }

  async fn list_segments(&self, req: ListSegmentsRequest) -> PancakeResult<ListSegmentsResponse> {
    let schema = self.schema_cache
      .get_result(&req.table_name)
      .await?;

    let mut partitions: Vec<Vec<PartitionField>> = vec![vec![]];
    for meta in &schema.partitioning {
      let mut new_partitions: Vec<Vec<PartitionField>> = Vec::new();
      for partition in &partitions {
        let subpartitions = self.list_subpartitions(&req.table_name, partition, meta)
          .await?;

        for leaf in subpartitions {
          let mut new_partition = partition.clone();
          new_partition.push(leaf);
          if utils::satisfies_filters(&new_partition, &req.partition_filter) {
            new_partitions.push(new_partition);
          }
        }
      }
      partitions = new_partitions;
    }

    let mut segments = Vec::new();
    for partition in &partitions {
      let normalized_partition = NormalizedPartition::partial(partition)?;
      let partition_key = PartitionKey {
        table_name: req.table_name.clone(),
        partition: normalized_partition,
      };
      let segments_meta = self.segments_metadata_cache
        .get_result(&partition_key)
        .await?;
      for segment_id in &segments_meta.segment_ids {
        segments.push(Segment {
          partition: partition.clone(),
          segment_id: segment_id.clone(),
          ..Default::default()
        });
      }
    }

    Ok(ListSegmentsResponse {
      schema: MessageField::some(schema),
      segments,
      continuation_token: "".to_string(),
      ..Default::default()
    })
  }

  pub fn list_segments_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::get()
      .and(warp::path("list_segments"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::json())
      .and_then(Self::warp_list_segments)
  }

  async fn warp_list_segments(server: Server, req: ListSegmentsRequest) -> Result<impl Reply, Infallible> {
    utils::pancake_result_into_warp(server.list_segments(req).await)
  }

  pub fn read_segment_column_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::get()
      .and(warp::path("read_segment_column"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::json())
      .and_then(Self::warp_read_segment_column)
  }

  async fn read_segment_column(&self, req: ReadSegmentColumnRequest) -> PancakeResult<ReadSegmentColumnResponse> {
    let col_name = req.column_name;
    let schema = self.schema_cache
      .get_result(&req.table_name)
      .await?;
    let partition = NormalizedPartition::full(&schema, &req.partition)?;
    let segment_key = SegmentKey {
      table_name: req.table_name.clone(),
      partition,
      segment_id: req.segment_id.clone(),
    };
    let flush_meta_future = self.flush_metadata_cache
      .get(&segment_key);

    let flush_meta = flush_meta_future
      .await;

    let mut valid_col = false;
    for col_meta_item in &schema.columns {
      if col_meta_item.name == col_name {
        valid_col = true;
      }
    }

    if !valid_col {
      return Err(PancakeError::does_not_exist("column", &col_name));
    }

    let compaction_key = segment_key.compaction_key(flush_meta.read_version);
    let compaction = self.compaction_cache
      .get(compaction_key)
      .await;

    // TODO: error handling
    // TODO: pagination
    let compaction_key = segment_key.compaction_key(flush_meta.read_version);
    let compressed_filename = dirs::compact_col_file(
      &self.dir,
      &compaction_key,
      &col_name,
    );
    let uncompressed_filename = dirs::flush_col_file(
      &self.dir,
      &compaction_key,
      &col_name,
    );

    let compressor_name = compaction.col_compression_params
      .get(&col_name)
      .map(|c| c.clone() as String)
      .unwrap_or_default();

    let compressed_data = utils::read_if_exists(compressed_filename).await.unwrap_or_default();
    let uncompressed_data = utils::read_if_exists(uncompressed_filename).await.unwrap_or_default();

    Ok(ReadSegmentColumnResponse {
      compressor_name,
      compressed_data,
      uncompressed_data,
      ..Default::default()
    })
  }

  async fn warp_read_segment_column(server: Server, req: ReadSegmentColumnRequest) -> Result<impl Reply, Infallible> {
    utils::pancake_result_into_warp(server.read_segment_column(req).await)
  }
}