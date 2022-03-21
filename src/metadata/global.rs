use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::impl_metadata_serde_json;
use crate::types::EmptyKey;

use super::traits::{MetadataKey, PersistentMetadata};

impl MetadataKey for EmptyKey {
  const ENTITY_NAME: &'static str = "global metadata";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GlobalMetadata {
  pub n_shards_log: u32,
}

impl_metadata_serde_json!(GlobalMetadata);

impl PersistentMetadata<EmptyKey> for GlobalMetadata {
  const CACHE_SIZE_LIMIT: usize = 1;

  fn relative_path(_: &EmptyKey) -> PathBuf {
    "global_metadata.json".into()
  }
}

#[allow(clippy::derivable_impls)]
impl Default for GlobalMetadata {
  fn default() -> Self {
    GlobalMetadata {
      n_shards_log: 0,
    }
  }
}
