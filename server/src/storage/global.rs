use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::errors::ServerResult;
use crate::impl_metadata_serde_json;

use super::traits::{CacheData, Metadata, MetadataKey};

impl MetadataKey for () {
  const ENTITY_NAME: &'static str = "global metadata";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GlobalMetadata {
  pub n_shards_log: u32,
}

impl_metadata_serde_json!(GlobalMetadata);

impl Metadata<()> for GlobalMetadata {
  fn relative_path(_: &()) -> PathBuf {
    "global_metadata.json".into()
  }
}

impl Default for GlobalMetadata {
  fn default() -> Self {
    GlobalMetadata {
      n_shards_log: 0,
    }
  }
}

pub type GlobalMetadataCache = CacheData<(), GlobalMetadata>;
