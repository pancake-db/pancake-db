use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::impl_metadata_serde_json;
use crate::types::SegmentKey;
use crate::utils::dirs;

use super::traits::{CacheData, Metadata};
use crate::errors::ServerResult;

#[derive(Serialize, Deserialize, Clone)]
pub struct DeletionMetadata {}

impl_metadata_serde_json!(DeletionMetadata);

impl Metadata<SegmentKey> for DeletionMetadata {
  fn relative_path(key: &SegmentKey) -> PathBuf {
    dirs::relative_segment_dir(key)
      .join("deletion_metadata.json")
  }
}

pub type DeletionMetadataCache = CacheData<SegmentKey, DeletionMetadata>;
