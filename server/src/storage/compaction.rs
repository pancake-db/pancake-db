use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::errors::ServerResult;
use crate::impl_metadata_serde_json;
use crate::types::CompactionKey;
use crate::utils::dirs;

use super::traits::{CacheData, Metadata, MetadataKey};

impl MetadataKey for CompactionKey {
  const ENTITY_NAME: &'static str = "compaction";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Compaction {
  pub compacted_n: usize,
  pub omitted_n: u64, // all time # of rows deleted that don't show up in compaction
  pub col_codecs: HashMap<String, String>,
}

impl_metadata_serde_json!(Compaction);

impl Default for Compaction {
  fn default() -> Compaction {
    Compaction {
      compacted_n: 0,
      omitted_n: 0,
      col_codecs: HashMap::new(),
    }
  }
}

impl Metadata<CompactionKey> for Compaction {
  fn relative_path(key: &CompactionKey) -> PathBuf {
    dirs::relative_version_dir(key).join("compaction.json")
  }
}

pub type CompactionCache = CacheData<CompactionKey, Compaction>;
