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

// impl FlushMetadataCache {
//   pub async fn get(&self, key: &SegmentKey) -> FlushMetadata {
//     self.get_or_err(key)
//       .await
//       .unwrap_or_default()
//   }
//
//   pub async fn increment_n(&self, key: &SegmentKey, incr: usize) -> ServerResult<()> {
//     let mut mux_guard = self.data.write().await;
//     let map = &mut *mux_guard;
//     if !map.contains_key(key) || map.get(key).unwrap().is_none() {
//       map.insert(key.clone(), Some(FlushMetadata::load_or_default(&self.dir, key).await?));
//     }
//     let metadata = map.get_mut(key).unwrap().as_mut().unwrap();
//
//     metadata.n += incr;
//     metadata.last_flush_at = Utc::now();
//     metadata.overwrite(&self.dir, key).await?;
//     Ok(())
//   }
//
//   pub async fn update_read_version(&self, key: &SegmentKey, read_version: u64) -> ServerResult<()> {
//     let mut new_versions = Vec::new();
//
//     let mut mux_guard = self.data.write().await;
//     let map = &mut *mux_guard;
//     if !map.contains_key(key) {
//       map.insert(key.clone(), Some(FlushMetadata::load_or_default(&self.dir, key).await?));
//     }
//     let mut metadata = map.get_mut(key).unwrap().as_mut().unwrap();
//
//     for version in &metadata.write_versions {
//       if *version >= read_version {
//         new_versions.push(*version);
//       }
//     }
//
//     metadata.read_version = read_version;
//     metadata.write_versions = new_versions;
//     metadata.read_version_since = Utc::now();
//     metadata.overwrite(&self.dir, key).await?;
//     Ok(())
//   }
//
//   pub async fn add_write_version(&self, key: &SegmentKey, write_version: u64) -> ServerResult<()>{
//     let mut mux_guard = self.data.write().await;
//     let map = &mut *mux_guard;
//     if !map.contains_key(key) {
//       map.insert(key.clone(), Some(FlushMetadata::load_or_default(&self.dir, key).await?));
//     }
//     let metadata = map.get_mut(key).unwrap().as_mut().unwrap();
//
//     metadata.write_versions.push(write_version);
//     metadata.overwrite(&self.dir, key).await?;
//     Ok(())
//   }
// }
