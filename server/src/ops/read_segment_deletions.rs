use std::str::FromStr;

use async_trait::async_trait;
use pancake_db_idl::dml::{ReadSegmentDeletionsRequest, ReadSegmentDeletionsResponse};
use tokio::fs;
use uuid::Uuid;

use crate::errors::ServerResult;
use crate::locks::deletion::DeletionReadLocks;
use crate::ops::traits::ServerOp;
use crate::server::Server;
use crate::types::{NormalizedPartition, SegmentKey};
use crate::utils::common;
use crate::utils::dirs;
use std::io::ErrorKind;

pub struct ReadSegmentDeletionsOp {
  pub req: ReadSegmentDeletionsRequest,
}

#[async_trait]
impl ServerOp<DeletionReadLocks> for ReadSegmentDeletionsOp {
  type Response = ReadSegmentDeletionsResponse;

  fn get_key(&self) -> ServerResult<SegmentKey> {
    let partition = NormalizedPartition::from_raw_fields(&self.req.partition)?;
    Ok(SegmentKey {
      table_name: self.req.table_name.clone(),
      partition,
      segment_id: Uuid::from_str(&self.req.segment_id)?,
    })
  }

  async fn execute_with_locks(&self, server: &Server, locks: DeletionReadLocks) -> ServerResult<ReadSegmentDeletionsResponse> {
    let req = &self.req;
    common::validate_entity_name_for_read("table name", &req.table_name)?;
    common::validate_segment_id(&req.segment_id)?;

    let DeletionReadLocks {
      table_meta: _,
      maybe_deletion_meta: _,
      segment_meta,
      segment_key,
    } = locks;
    let version = server.correlation_metadata_cache.get_correlated_read_version(
      &req.correlation_id,
      &segment_key,
      segment_meta.read_version
    ).await?;
    let compaction_key = segment_key.compaction_key(version);

    let io_res = fs::read(dirs::post_compaction_deletions_path(
      &server.opts.dir,
      &compaction_key,
      segment_meta.deletion_id,
    )).await;
    let data = match io_res {
      Ok(data) => Ok(data),
      Err(e) if matches!(e.kind(), ErrorKind::NotFound) => Ok(Vec::new()),
      Err(e) => Err(e),
    }?;

    let response = ReadSegmentDeletionsResponse {
      data,
      ..Default::default()
    };

    Ok(response)
  }
}
