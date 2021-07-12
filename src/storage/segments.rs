use serde::{Deserialize, Serialize};

use super::traits::{Metadata, CacheData};
use crate::dirs;
use crate::types::PartitionKey;
use crate::utils;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone)]
pub struct SegmentsMetadata {
  pub write_segment_id: String,
  pub segment_ids: Vec<String>,
}

impl Metadata<PartitionKey> for SegmentsMetadata {
  fn relative_path(key: &PartitionKey) -> String {
    format!(
      "{}/segmenting_metadata.json",
      dirs::relative_partition_dir(key),
    )
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
      Some(res) => res.clone(),
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

  // async fn start_first_segment(&self, key: &PartitionKey) -> Result<String, &'static str> {
  //   let mut mux_guard = self.data.write().await;
  //   let map = &mut *mux_guard;
  //
  //   if !map.contains_key(key) {
  //     let segment_id = Uuid::new_v4().to_string();
  //     utils::create_if_new(&dirs::segment_dir(&self.dir, key, &segment_id)).await?;
  //     utils::create_if_new(&dirs::version_dir(&self.dir, key, &segment_id, 0)).await?;
  //
  //     let metadata = SegmentsMetadata::new(&segment_id);
  //     map.insert(key.clone(), Some(metadata.clone()));
  //     metadata.overwrite(&self.dir, key).await?;
  //     Ok(segment_id)
  //   } else {
  //     let metadata = map.get(key).unwrap();
  //     Ok(metadata.write_segment_id.clone())
  //   }
  // }
  //
  // async fn start_new_segment(&self, key: &PartitionKey) -> Result<String, &'static str> {
  //   let mut mux_guard = self.data.write().await;
  //   let map = &mut *mux_guard;
  //
  //   let segment_id = Uuid::new_v4().to_string();
  //   utils::create_if_new(&dirs::segment_dir(&self.dir, key, &segment_id)).await?;
  //   utils::create_if_new(&dirs::version_dir(&self.dir, key, &segment_id, 0)).await?;
  //
  //   let metadata = map.get_mut(key).expect("segments metadata not available");
  //   metadata.write_segment_id = segment_id.clone();
  //   metadata.segment_ids.push(segment_id.clone());
  //   Ok(metadata.write_segment_id.clone())
  // }
}
