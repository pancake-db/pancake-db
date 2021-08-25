use std::collections::HashMap;
use std::path::PathBuf;

use crate::errors::ServerResult;
use serde::{Deserialize, Serialize};

use crate::dirs;
use crate::types::CompactionKey;
use crate::impl_metadata_serde_json;

use super::traits::{CacheData, Metadata, MetadataKey};

impl MetadataKey for CompactionKey {
  const ENTITY_NAME: &'static str = "compaction";
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Compaction {
  pub compacted_n: usize,
  pub col_codecs: HashMap<String, String>
}

impl_metadata_serde_json!(Compaction);

impl Default for Compaction {
  fn default() -> Compaction {
    Compaction {
      compacted_n: 0,
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

impl CompactionCache {
  pub async fn get(&self, key: CompactionKey) -> Compaction {
    self.get_result(&key)
      .await
      .unwrap_or_default()
  }

  pub async fn save(&self, key: CompactionKey, compaction: Compaction) -> ServerResult<()> {
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    compaction.overwrite(&self.dir, &key).await?;
    map.insert(key, Some(compaction));
    Ok(())
  }
}
