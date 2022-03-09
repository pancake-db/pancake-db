use std::path::Path;

use async_trait::async_trait;
use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse, Row, FieldValue};
use tokio::fs;

use crate::errors::{ServerError, ServerResult, Contextable};
use crate::locks::partition::PartitionWriteLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::metadata::PersistentMetadata;
use crate::metadata::segment::SegmentMetadata;
use crate::types::{NormalizedPartition, PartitionKey, SegmentKey};
use crate::utils::{common, navigation};
use crate::utils::dirs;
use crate::constants::{ROW_ID_COLUMN_NAME, WRITTEN_AT_COLUMN_NAME};
use protobuf::well_known_types::Timestamp;
use pancake_db_idl::dml::field_value::Value;
use crate::Opt;

pub struct WriteToPartitionOp {
  pub req: WriteToPartitionRequest,
}

#[async_trait]
impl ServerOp<PartitionWriteLocks> for WriteToPartitionOp {
  type Response = WriteToPartitionResponse;

  fn get_key(&self) -> ServerResult<PartitionKey> {
    let partition = NormalizedPartition::from_raw_fields(&self.req.partition)?;
    Ok(PartitionKey {
      table_name: self.req.table_name.clone(),
      partition,
    })
  }

  async fn execute_with_locks(&self, server: &Server, locks: PartitionWriteLocks) -> ServerResult<WriteToPartitionResponse> {
    common::validate_entity_name_for_read("table name", &self.req.table_name)?;

    let dir = &server.opts.dir;
    let PartitionWriteLocks {
      global_meta: _,
      table_meta,
      mut definitely_partition_guard,
      mut definitely_segment_guard,
      shard_id,
      mut segment_key,
    } = locks;
    let partition_key = segment_key.partition_key();
    let partition_meta = definitely_partition_guard.as_mut().unwrap();
    let mut segment_meta = definitely_segment_guard.as_mut().unwrap();

    common::validate_rows(&table_meta.schema, &self.req.rows)?;

    let mut default_segment_meta = SegmentMetadata::new_from_schema(&table_meta.schema);
    if segment_meta.is_cold {
      let new_segment_id = shard_id.generate_segment_id();
      let key = SegmentKey {
        table_name: segment_key.table_name.clone(),
        partition: segment_key.partition.clone(),
        segment_id: new_segment_id,
      };
      navigation::create_segment_dirs(&dirs::segment_dir(dir, &key)).await?;
      partition_meta.replace_active_segment_id(segment_key.segment_id, new_segment_id);
      partition_meta.overwrite(dir, &partition_key).await?;

      segment_key = key;
      segment_meta = &mut default_segment_meta;
    }

    // add DB columns to rows
    let full_rows = self.full_db_columns(segment_meta);

    let staged_bytes = common::rows_to_staged_bytes(&full_rows)
      .with_context(|| "while writing staged rows to bytes")?;
    common::append_to_file(
      dirs::staged_rows_path(&server.opts.dir, &segment_key),
      &staged_bytes,
    ).await?;

    let n_rows = full_rows.len();
    Self::increment_segment_meta_n_rows(
      n_rows,
      segment_meta,
      &server.opts,
      &segment_key,
    ).await?;

    server.add_flush_candidate(segment_key).await;

    Ok(WriteToPartitionResponse {..Default::default()})
  }
}

impl WriteToPartitionOp {
  fn full_db_columns(
    &self,
    segment_meta: &SegmentMetadata,
  ) -> Vec<Row> {
    let written_at = FieldValue {
      value: Some(Value::timestamp_val(Timestamp::now())),
      ..Default::default()
    };
    let mut res = Vec::with_capacity(self.req.rows.len());
    let mut row_id = segment_meta.all_time_n;
    for row in &self.req.rows {
      let mut full = row.clone();
      let row_id_fv = FieldValue {
        value: Some(Value::int64_val(row_id as i64)),
        ..Default::default()
      };
      full.fields.insert(ROW_ID_COLUMN_NAME.to_string(), row_id_fv);
      full.fields.insert(WRITTEN_AT_COLUMN_NAME.to_string(), written_at.clone());
      res.push(full);

      row_id += 1;
    }
    res
  }

  async fn increment_segment_meta_n_rows(
    n_rows: usize,
    segment_meta: &mut SegmentMetadata,
    opts: &Opt,
    segment_key: &SegmentKey
  ) -> ServerResult<()> {
    if n_rows > 0 {
      segment_meta.all_time_n += n_rows as u32;
      segment_meta.staged_n += n_rows as u32;
      if segment_meta.all_time_n >= opts.default_rows_per_segment + segment_meta.all_time_deleted_n {
        segment_meta.is_cold = true;
      }
      segment_meta.overwrite(&opts.dir, segment_key).await
    } else {
      Ok(())
    }
  }

  pub async fn recover(
    server: &Server,
    segment_key: &SegmentKey,
    segment_meta: &mut SegmentMetadata,
  ) -> ServerResult<()> {
    let dir = &server.opts.dir;
    let staged_rows_path = dirs::staged_rows_path(dir, segment_key);
    let staged_rows = common::staged_bytes_to_rows(&fs::read(&staged_rows_path).await?)?;
    if staged_rows.len() < segment_meta.staged_n as usize {
      return Err(ServerError::internal(format!(
        "segment {} is in an impossible state with fewer rows ({}) in staged file than in metadata ({})",
        segment_key,
        staged_rows.len(),
        segment_meta.staged_n,
      )))
    }
    let n_rows = staged_rows.len() - segment_meta.staged_n as usize;
    if n_rows > 0 {
      log::debug!(
        "identified missing staged rows in segment {} metadata; filling them in",
        segment_key,
      )
    }
    Self::increment_segment_meta_n_rows(
      n_rows,
      segment_meta,
      &server.opts,
      segment_key,
    ).await
  }
}