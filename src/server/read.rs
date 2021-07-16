use pancake_db_idl::dml::{FieldValue, ListSegmentsRequest, ListSegmentsResponse, PartitionField, ReadSegmentColumnRequest, ReadSegmentColumnResponse, Segment};
use pancake_db_idl::schema::{ColumnMeta, PartitionMeta};
use protobuf::MessageField;
use tokio::fs;
use warp::{Filter, Rejection, Reply};

use crate::compression;
use crate::dirs;
use crate::encoding;
use crate::storage::compaction::CompressionParams;
use crate::storage::flush::FlushMetadata;
use crate::types::{NormalizedPartition, PartitionKey, SegmentKey};
use crate::utils;

use super::Server;
use tokio::io::ErrorKind;

impl Server {
  pub async fn read_compact_col(
    &self,
    segment_key: &SegmentKey,
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    compression_params: Option<&CompressionParams>,
    limit: usize,
  ) -> Result<Vec<FieldValue>, &'static str> {
    let compaction_key = segment_key.compaction_key(metadata.read_version);
    let path = dirs::compact_col_file(&self.dir, &compaction_key, &col.name);
    match fs::read(path).await {
      Ok(bytes) => {
        let decompressor = compression::get_decompressor(col.dtype.unwrap(), compression_params)?;
        let decoded = decompressor.decompress(&bytes, col)?;
        let limited= if limit < decoded.len() {
          Vec::from(&decoded[0..limit])
        } else {
          decoded
        };
        Ok(limited)
      },
      Err(e) => match e.kind() {
        ErrorKind::NotFound => Ok(Vec::new()),
        _ => Err("could not read compaction file bytes"),
      },
    }
  }

  pub async fn read_flush_col(
    &self,
    segment_key: &SegmentKey,
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    limit: usize,
  ) -> Result<Vec<FieldValue>, &'static str> {
    let compaction_key = segment_key.compaction_key(metadata.read_version);
    let path = dirs::flush_col_file(&self.dir, &compaction_key, &col.name);
    match fs::read(path).await {
      Ok(bytes) => {
        let decoded = encoding::decode(&bytes, col)?;
        let limited;
        if limit < decoded.len() {
          limited = Vec::from(&decoded[0..limit]);
        } else {
          limited = decoded;
        }
        Ok(limited)
      },
      Err(_) => Err("could not decode compaction file"),
    }
  }

  pub async fn read_col(
    &self,
    segment_key: &SegmentKey,
    col: &ColumnMeta,
    metadata: &FlushMetadata,
    compression_params: Option<&CompressionParams>,
    limit: usize,
  ) -> Result<Vec<FieldValue>, &'static str> {
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
    // let mut decompressor = compression::get_decompressor(&col.dtype.unwrap(), compression_params);
    // let mut result = Vec::new();
    // for col_file in &dirs::col_files(
    //   &self.dir,
    //   &segment_key.compaction_key(metadata.read_version),
    //   &col.name,
    // ) {
    //   match fs::read(col_file).await {
    //     Ok(bytes) => {
    //       let end = limit - result.len();
    //       let decoded = decompressor.decode(&bytes);
    //       let limited;
    //       if end < decoded.len() {
    //         limited = Vec::from(&decoded[0..end]);
    //       } else {
    //         limited = decoded;
    //       }
    //       result.extend(limited);
    //     },
    //     Err(_) => (),
    //   }
    // }
    // return result;
  }

  async fn list_subpartitions(
    &self,
    table_name: &str,
    parent: &[PartitionField],
    meta: &PartitionMeta,
  ) -> Result<Vec<PartitionField>, &'static str> {
    let dir = dirs::partition_dir(
      &self.dir,
      &PartitionKey {
        table_name: table_name.to_string(),
        partition: NormalizedPartition::partial(parent)?
      }
    );
    let mut res = Vec::new();
    let mut read_dir = fs::read_dir(&dir).await.expect("could not read dir");
    while let Ok(Some(entry)) = read_dir.next_entry().await {
      if !entry.file_type().await.unwrap().is_dir() {
        continue;
      }

      let fname = entry.file_name();
      let parts = fname
        .to_str()
        .expect("how can os string not be str")
        .split("=")
        .collect::<Vec<&str>>();

      if parts.len() != 2 {
        continue;
      }
      if parts[0].to_string() != meta.name {
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

  pub fn list_segments_filter() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::get()
      .and(warp::path("rest"))
      .and(warp::path("list_segments"))
      .and(warp::filters::ext::get::<Server>())
      .and(warp::filters::body::json())
      .and_then(Self::list_segments)
  }

  pub async fn list_segments(server: Server, req: ListSegmentsRequest) -> Result<impl Reply, Rejection> {
    let schema = server.schema_cache
      .get_option(&req.table_name)
      .await
      .expect("table does not exist");
    let mut partitions: Vec<Vec<PartitionField>> = vec![vec![]];
    for meta in &schema.partitioning {
      let mut new_partitions: Vec<Vec<PartitionField>> = Vec::new();
      for partition in &partitions {
        for leaf in server.list_subpartitions(&req.table_name, partition, meta).await.or(Err(warp::reject()))? {
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
      let partition_key = PartitionKey {
        table_name: req.table_name.clone(),
        partition: NormalizedPartition::partial(partition).or(Err(warp::reject()))?
      };
      let segments_meta = server.segments_metadata_cache
        .get_option(&partition_key)
        .await
        .ok_or(warp::reject())?;
      for segment_id in &segments_meta.segment_ids {
        segments.push(Segment {
          partition: partition.clone(),
          segment_id: segment_id.clone(),
          ..Default::default()
        });
      }
    }

    Ok(warp::reply::json(&ListSegmentsResponse {
      schema: MessageField::some(schema),
      segments,
      continuation_token: "".to_string(),
      ..Default::default()
    }))
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
    let schema = server.schema_cache
      .get_option(&req.table_name)
      .await
      .expect("table does not exist");
    let partition = NormalizedPartition::full(&schema, &req.partition)
      .expect("partition read fail");
    let segment_key = SegmentKey {
      table_name: req.table_name.clone(),
      partition,
      segment_id: req.segment_id.clone(),
    };
    let flush_meta_future = server.flush_metadata_cache
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
      return Err(warp::reject());
    }

    let compaction_key = segment_key.compaction_key(flush_meta.read_version);
    let compaction = server.compaction_cache
      .get(compaction_key)
      .await;

    // TODO: error handling
    // TODO: pagination
    let compaction_key = segment_key.compaction_key(flush_meta.read_version);
    let compressed_filename = dirs::compact_col_file(
      &server.dir,
      &compaction_key,
      &col_name,
    );
    let uncompressed_filename = dirs::flush_col_file(
      &server.dir,
      &compaction_key,
      &col_name,
    );

    let compressor_name = compaction.col_compression_params
      .get(&col_name)
      .map(|c| c.clone() as String)
      .unwrap_or("".to_string());

    Ok(warp::reply::json(&ReadSegmentColumnResponse {
      compressor_name,
      compressed_data: utils::read_if_exists(compressed_filename).await.unwrap_or(Vec::new()),
      uncompressed_data: utils::read_if_exists(uncompressed_filename).await.unwrap_or(Vec::new()),
      ..Default::default()
    }))
  }
}