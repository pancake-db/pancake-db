use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::dirs;
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
}

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
    }
  }
}

pub type SegmentsMetadataCache = CacheData<PartitionKey, SegmentsMetadata>;

impl SegmentsMetadataCache {
  pub async fn get_or_create(&self, key: &PartitionKey) -> SegmentsMetadata {
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    let maybe_option = map.get(key);

    let option = match maybe_option {
      Some(res) => res.clone(),
      None => {
        let res = SegmentsMetadata::load(&self.dir, key).await.map(|x| *x);
        map.insert(key.clone(), res.clone());
        res
      }
    };

    match option {
      Some(res) => res,
      None => {
        let segment_id = Uuid::new_v4().to_string();
        let meta = SegmentsMetadata::new(&segment_id);
        meta.overwrite(&self.dir, key).await.expect("could not overwrite segments meta");
        let segment_key = key.segment_key(segment_id);
        utils::create_if_new(&dirs::segment_dir(
          &self.dir,
          &segment_key,
        )).await.expect("failed to create segment dir");
        utils::create_if_new(&dirs::version_dir(
          &self.dir,
          &segment_key.compaction_key(0),
        )).await.expect("failed to create version dir");
        map.insert(key.clone(), Some(meta.clone()));
        meta
      }
    }
  }
}
