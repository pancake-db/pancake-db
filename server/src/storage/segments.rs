use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::dirs;
use crate::errors::{ServerError, ServerResult};
use crate::impl_metadata_serde_json;
use crate::storage::traits::MetadataKey;
use crate::types::PartitionKey;
use crate::utils;

use super::traits::{CacheData, Metadata};

impl MetadataKey for PartitionKey {
  const ENTITY_NAME: &'static str = "partition segments file";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SegmentsMetadata {
  pub write_segment_id: String,
  pub segment_ids: Vec<String>,
  pub start_new_write_segment: bool,
}

impl_metadata_serde_json!(SegmentsMetadata);

impl Metadata<PartitionKey> for SegmentsMetadata {
  fn relative_path(key: &PartitionKey) -> PathBuf {
    dirs::relative_partition_dir(key)
      .join("segmenting_metadata.json")
  }
}

impl SegmentsMetadata {
  pub fn new(segment_id: &str) -> SegmentsMetadata {
    SegmentsMetadata {
      write_segment_id: segment_id.to_string(),
      segment_ids: vec![segment_id.to_string()],
      start_new_write_segment: false,
    }
  }
}

fn new_segment_id() -> String {
  Uuid::new_v4().to_string()
}

pub type SegmentsMetadataCache = CacheData<PartitionKey, SegmentsMetadata>;

impl SegmentsMetadataCache {
  pub async fn start_new_write_segment(&self, key: &PartitionKey, dead_segment_id: &str) -> ServerResult<()> {
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    let maybe_option = map.get(key);

    let option = match maybe_option {
      Some(res) => res.clone(),
      None => {
        let res = SegmentsMetadata::load(&self.dir, key).await?;
        map.insert(key.clone(), res.clone());
        res
      }
    };

    match option {
      Some(mut existing) if existing.write_segment_id == dead_segment_id => {
        existing.start_new_write_segment = true;
        existing.overwrite(&self.dir, key).await?;
        map.insert(key.clone(), Some(existing.clone()));
        Ok(())
      },
      _ => Err(ServerError::internal("unexpected start new write segment on empty table"))
    }
  }

  pub async fn get_or_create(&self, key: &PartitionKey) -> ServerResult<SegmentsMetadata> {
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    let maybe_option = map.get(key);

    let option = match maybe_option {
      Some(res) => res.clone(),
      None => {
        let res = SegmentsMetadata::load(&self.dir, key).await?;
        map.insert(key.clone(), res.clone());
        res
      }
    };

    match option {
      Some(mut res) => {
        if res.start_new_write_segment {
          let segment_id = new_segment_id();
          res.segment_ids.push(segment_id.clone());
          res.write_segment_id = segment_id.clone();
          res.start_new_write_segment = false;
          res.overwrite(&self.dir, key).await?;
          self.provision_segment(key, segment_id).await?;
          map.insert(key.clone(), Some(res.clone()));
        }
        Ok(res)
      },
      None => {
        let segment_id = new_segment_id();
        let meta = SegmentsMetadata::new(&segment_id);
        meta.overwrite(&self.dir, key).await?;
        self.provision_segment(key, segment_id).await?;
        map.insert(key.clone(), Some(meta.clone()));
        Ok(meta)
      }
    }
  }

  pub async fn provision_segment(&self, key: &PartitionKey, segment_id: String) -> ServerResult<()> {
    let segment_key = key.segment_key(segment_id);
    utils::create_if_new(&dirs::segment_dir(
      &self.dir,
      &segment_key,
    )).await?;
    utils::create_if_new(&dirs::version_dir(
      &self.dir,
      &segment_key.compaction_key(0),
    )).await?;
    Ok(())
  }
}
