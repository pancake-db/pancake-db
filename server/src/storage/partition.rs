use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::dirs;
use crate::errors::ServerResult;
use crate::impl_metadata_serde_json;
use crate::storage::traits::MetadataKey;
use crate::types::PartitionKey;

use super::traits::{CacheData, Metadata};

impl MetadataKey for PartitionKey {
  const ENTITY_NAME: &'static str = "partition segments file";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PartitionMetadata {
  pub write_segment_id: String,
  pub segment_ids: Vec<String>,
  // pub start_new_write_segment: bool,
}

impl_metadata_serde_json!(PartitionMetadata);

impl Metadata<PartitionKey> for PartitionMetadata {
  fn relative_path(key: &PartitionKey) -> PathBuf {
    dirs::relative_partition_dir(key)
      .join("partition_metadata.json")
  }
}

impl PartitionMetadata {
  pub fn new(segment_id: &str) -> PartitionMetadata {
    PartitionMetadata {
      write_segment_id: segment_id.to_string(),
      segment_ids: vec![segment_id.to_string()],
    }
  }
}

pub type PartitionMetadataCache = CacheData<PartitionKey, PartitionMetadata>;
