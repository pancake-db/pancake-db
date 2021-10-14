use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::errors::ServerResult;

use crate::impl_metadata_serde_json;
use crate::dirs;
use crate::storage::traits::MetadataKey;
use crate::types::SegmentKey;

use super::traits::{CacheData, Metadata};

impl MetadataKey for SegmentKey {
  const ENTITY_NAME: &'static str = "segment";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SegmentMetadata {
  pub all_time_n: u64, // only increases, includes deleted
  pub currently_staged_n: usize, // includes deleted
  pub all_time_n_deleted: u64, // only increases
  pub current_staged_n_deleted: usize,
  pub write_versions: Vec<u64>,
  pub read_version: u64,
  pub read_version_since: DateTime<Utc>,
  pub last_flush_at: DateTime<Utc>,
}

impl_metadata_serde_json!(SegmentMetadata);

impl Metadata<SegmentKey> for SegmentMetadata {
  fn relative_path(key: &SegmentKey) -> PathBuf {
    dirs::relative_segment_dir(key)
      .join("flush_metadata.json")
  }
}

impl Default for SegmentMetadata {
  fn default() -> SegmentMetadata {
    SegmentMetadata {
      all_time_n: 0,
      all_time_n_deleted: 0,
      currently_staged_n: 0,
      current_staged_n_deleted: 0,
      read_version: 0,
      write_versions: vec![0],
      read_version_since: Utc::now(),
      last_flush_at: Utc::now(),
    }
  }
}

pub type SegmentMetadataCache = CacheData<SegmentKey, SegmentMetadata>;
