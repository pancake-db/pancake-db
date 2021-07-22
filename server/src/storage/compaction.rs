use std::collections::HashMap;
use std::path::PathBuf;

use pancake_db_core::errors::PancakeResult;
use serde::{Deserialize, Serialize};

use crate::dirs;
use crate::types::CompactionKey;

use super::traits::{CacheData, Metadata, MetadataKey};

pub type CompressionParams = String;

impl MetadataKey for CompactionKey {
  const ENTITY_NAME: &'static str = "compaction";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Compaction {
  pub compacted_n: usize,
  pub col_compression_params: HashMap<String, CompressionParams>
}

impl Default for Compaction {
  fn default() -> Compaction {
    Compaction {
      compacted_n: 0,
      col_compression_params: HashMap::new(),
    }
  }
}

impl Metadata<CompactionKey> for Compaction {
  fn relative_path(key: &CompactionKey) -> PathBuf {
    dirs::relative_version_dir(key).join("compaction.json")
  }
}

pub type CompactionCache = CacheData<CompactionKey, Compaction>;

impl CompactionCache {
  pub async fn get(&self, key: CompactionKey) -> Compaction {
    self.get_result(&key)
      .await
      .unwrap_or_default()
  }

  pub async fn save(&self, key: CompactionKey, compaction: Compaction) -> PancakeResult<()> {
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    compaction.overwrite(&self.dir, &key).await?;
    map.insert(key, Some(compaction));
    Ok(())
  }
}
