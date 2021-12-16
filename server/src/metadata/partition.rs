use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::utils::dirs;
use crate::errors::ServerResult;
use crate::impl_metadata_serde_json;
use crate::metadata::traits::MetadataKey;
use crate::types::{PartitionKey, ShardId};

use super::traits::{PersistentCacheData, PersistentMetadata};
use uuid::Uuid;

impl MetadataKey for PartitionKey {
  const ENTITY_NAME: &'static str = "partition segments file";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PartitionMetadata {
  // 1 / (2^sharding_denominator_log) of shards are used to store this
  // partition. So if there are 32 shards and this is 3, we'll use
  // 32 / (2^3) = 4 shards.
  pub sharding_denominator_log: u32,
  pub active_segment_ids: Vec<Uuid>,
}

impl_metadata_serde_json!(PartitionMetadata);

impl PersistentMetadata<PartitionKey> for PartitionMetadata {
  const CACHE_SIZE_LIMIT: usize = 65536;

  fn relative_path(key: &PartitionKey) -> PathBuf {
    dirs::relative_partition_dir(key)
      .join("partition_metadata.json")
  }
}

impl PartitionMetadata {
  pub fn new(n_shards_log: u32) -> PartitionMetadata {
    // A partition will live in 2^n_shards_log / 2^sharding_denominator_log
    // shards.
    // When we first create a partition, make it in only one shard
    // by setting sharding_denominator_log to be n_shards_log.
    PartitionMetadata {
      sharding_denominator_log: n_shards_log,
      active_segment_ids: Vec::new(),
    }
  }

  pub fn get_active_segment_id(&self, shard_id: &ShardId) -> Option<Uuid> {
    for segment_id in &self.active_segment_ids {
      if shard_id.contains_segment_id(*segment_id) {
        return Some(*segment_id);
      }
    }
    None
  }

  pub fn replace_active_segment_id(&mut self, old_id: Uuid, new_id: Uuid) {
    let mut res: Vec<_> = self.active_segment_ids.iter()
      .copied()
      .filter(|&id| id != old_id)
      .collect();
    res.push(new_id);
    self.active_segment_ids = res;
  }
}

pub type PartitionMetadataCache = PersistentCacheData<PartitionKey, PartitionMetadata>;
