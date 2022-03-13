use std::collections::HashSet;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use pancake_db_idl::schema::Schema;
use serde::{Deserialize, Serialize};

use crate::constants::{ROW_ID_COLUMN_NAME, WRITTEN_AT_COLUMN_NAME};
use crate::errors::ServerResult;
use crate::impl_metadata_serde_json;
use crate::metadata::traits::MetadataKey;
use crate::types::SegmentKey;
use crate::utils::dirs;

use super::traits::{PersistentCacheData, PersistentMetadata};

impl MetadataKey for SegmentKey {
  const ENTITY_NAME: &'static str = "segment";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SegmentMetadata {
  pub all_time_n: u32, // only increases, includes deleted
  pub all_time_deleted_n: u32, // only increases
  pub all_time_uncompressed_size: u64, // only increases, includes deleted
  pub staged_n: u32, // includes deleted
  pub write_versions: Vec<u64>,
  pub read_version: u64,
  pub read_version_since: DateTime<Utc>,
  pub last_flush_at: DateTime<Utc>,
  pub flushing: bool, // used for recovery purposes
  pub explicit_columns: HashSet<String>,
  pub deletion_id: u64,
  pub is_cold: bool, // future writes will not go to cold segments
}

impl_metadata_serde_json!(SegmentMetadata);

impl PersistentMetadata<SegmentKey> for SegmentMetadata {
  const CACHE_SIZE_LIMIT: usize = 65536;

  fn relative_path(key: &SegmentKey) -> PathBuf {
    dirs::relative_segment_dir(key)
      .join("segment_metadata.json")
  }
}

impl SegmentMetadata {
  pub fn new(explicit_columns: HashSet<String>) -> SegmentMetadata {
    SegmentMetadata {
      all_time_n: 0,
      all_time_deleted_n: 0,
      all_time_uncompressed_size: 0,
      staged_n: 0,
      read_version: 0,
      write_versions: vec![0],
      read_version_since: Utc::now(),
      last_flush_at: Utc::now(),
      flushing: false,
      explicit_columns,
      deletion_id: 0,
      is_cold: false,
    }
  }

  pub fn new_from_schema(schema: &Schema) -> Self {
    let mut explicit_columns: HashSet<_> = schema.columns.keys().cloned().collect();
    explicit_columns.insert(ROW_ID_COLUMN_NAME.to_string());
    explicit_columns.insert(WRITTEN_AT_COLUMN_NAME.to_string());

    Self::new(explicit_columns)
  }
}

pub type SegmentMetadataCache = PersistentCacheData<SegmentKey, SegmentMetadata>;
