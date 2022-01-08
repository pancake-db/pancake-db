use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::errors::ServerResult;
use crate::impl_metadata_serde_json;
use crate::types::CompactionKey;
use crate::utils::dirs;

use super::traits::{PersistentCacheData, PersistentMetadata, MetadataKey};

impl MetadataKey for CompactionKey {
  const ENTITY_NAME: &'static str = "compaction";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Compaction {
  // total # of rows ever written (including deleted ones that don't show up)
  // covered by this compaction
  pub all_time_compacted_n: u32,
  // total # of rows deleted prior to this compaction (these don't show up in
  // the compacted data)
  pub all_time_omitted_n: u32,
  pub col_codecs: HashMap<String, String>,
}

impl_metadata_serde_json!(Compaction);

#[allow(clippy::derivable_impls)]
impl Default for Compaction {
  fn default() -> Compaction {
    Compaction {
      all_time_compacted_n: 0,
      all_time_omitted_n: 0,
      col_codecs: HashMap::new(),
    }
  }
}

impl PersistentMetadata<CompactionKey> for Compaction {
  const CACHE_SIZE_LIMIT: usize = 65536;

  fn relative_path(key: &CompactionKey) -> PathBuf {
    dirs::relative_version_dir(key).join("compaction.json")
  }
}

pub type CompactionCache = PersistentCacheData<CompactionKey, Compaction>;
