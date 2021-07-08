use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use super::traits::{Metadata, CacheData};

pub type CompressionParams = String;

#[derive(Hash, PartialEq, Eq, Clone)]
pub struct CompactionKey {
  pub table_name: String,
  pub version: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Compaction {
  pub compacted_n: usize,
  pub col_compressor_names: HashMap<String, CompressionParams>
}

impl Default for Compaction {
  fn default() -> Compaction {
    return Compaction {
      compacted_n: 0,
      col_compressor_names: HashMap::new(),
    }
  }
}

impl Metadata<CompactionKey> for Compaction {
  fn relative_path(key: &CompactionKey) -> String {
    return format!("{}/v{}/compaction.json", key.table_name, key.version);
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
    map.insert(key, compaction);
    Ok(())
  }
}
