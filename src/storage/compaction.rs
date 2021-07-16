use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::dirs;
use crate::types::CompactionKey;

use super::traits::{CacheData, Metadata};

pub type CompressionParams = String;

#[derive(Serialize, Deserialize, Clone)]
pub struct Compaction {
  pub compacted_n: usize,
  pub col_compression_params: HashMap<String, CompressionParams>
}

impl Default for Compaction {
  fn default() -> Compaction {
    return Compaction {
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
    return self.get_option(&key)
      .await
      .unwrap_or(Compaction::default());
  }

  pub async fn save(&self, key: CompactionKey, compaction: Compaction) -> Result<(), &'static str> {
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    compaction.overwrite(&self.dir, &key).await?;
    map.insert(key, Some(compaction));
    Ok(())
  }
}
