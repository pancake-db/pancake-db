use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::errors::ServerResult;
use crate::impl_metadata_serde_json;
use crate::types::EmptyKey;

use super::traits::{MetadataKey, PersistentMetadata};
use crate::constants::MINOR_VERSION;

impl MetadataKey for EmptyKey {
  const ENTITY_NAME: &'static str = "global metadata";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GlobalMetadata {
  pub n_shards_log: u32,
  pub minor_version: u32,
  pub upgrade_in_progress_minor_version: u32,
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
      minor_version: MINOR_VERSION,
      upgrade_in_progress_minor_version: MINOR_VERSION,
    }
  }
}
