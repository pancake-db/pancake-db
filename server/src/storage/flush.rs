use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::dirs;
use crate::storage::traits::MetadataKey;
use crate::types::SegmentKey;

use super::traits::{CacheData, Metadata};
use pancake_db_core::errors::PancakeResult;

impl MetadataKey for SegmentKey {
  const ENTITY_NAME: &'static str = "segment";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FlushMetadata {
  pub n: usize,
  pub write_versions: Vec<u64>,
  pub read_version: u64,
}

impl Metadata<SegmentKey> for FlushMetadata {
  fn relative_path(key: &SegmentKey) -> PathBuf {
    dirs::relative_segment_dir(key)
      .join("flush_metadata.json")
  }
}

impl FlushMetadata {
  async fn load_or_default(dir: &Path, key: &SegmentKey) -> FlushMetadata {
    FlushMetadata::load(dir, key)
      .await
      .map(|x| *x)
      .unwrap_or_default()
  }
}

impl Default for FlushMetadata {
  fn default() -> FlushMetadata {
    FlushMetadata {
      n: 0,
      read_version: 0,
      write_versions: vec![0],
    }
  }
}

pub type FlushMetadataCache = CacheData<SegmentKey, FlushMetadata>;

impl FlushMetadataCache {
  pub async fn get(&self, key: &SegmentKey) -> FlushMetadata {
    self.get_result(key)
      .await
      .unwrap_or_default()
  }

  pub async fn increment_n(&self, key: &SegmentKey, incr: usize) -> PancakeResult<()> {
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    if !map.contains_key(key) || map.get(key).unwrap().is_none() {
      map.insert(key.clone(), Some(FlushMetadata::load_or_default(&self.dir, key).await));
    }
    let metadata = map.get_mut(key).unwrap().as_mut().unwrap();

    metadata.n += incr;
    metadata.overwrite(&self.dir, key).await?;
    Ok(())
  }

  pub async fn update_read_version(&self, key: &SegmentKey, read_version: u64) -> PancakeResult<()> {
    let mut new_versions = Vec::new();

    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    if !map.contains_key(key) {
      map.insert(key.clone(), Some(FlushMetadata::load_or_default(&self.dir, key).await));
    }
    let mut metadata = map.get_mut(key).unwrap().as_mut().unwrap();

    for version in &metadata.write_versions {
      if *version >= read_version {
        new_versions.push(*version);
      }
    }

    metadata.read_version = read_version;
    metadata.write_versions = new_versions;
    metadata.overwrite(&self.dir, key).await?;
    Ok(())
  }

  pub async fn add_write_version(&self, key: &SegmentKey, write_version: u64) -> PancakeResult<()>{
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    if !map.contains_key(key) {
      map.insert(key.clone(), Some(FlushMetadata::load_or_default(&self.dir, key).await));
    }
    let metadata = map.get_mut(key).unwrap().as_mut().unwrap();

    metadata.write_versions.push(write_version);
    metadata.overwrite(&self.dir, key).await?;
    Ok(())
  }
}
