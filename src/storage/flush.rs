use serde::{Deserialize, Serialize};

use super::traits::{Metadata, CacheData};

#[derive(Serialize, Deserialize, Clone)]
pub struct FlushMetadata {
  pub n: usize,
  pub write_versions: Vec<u64>,
  pub read_version: u64,
}

impl Metadata<String> for FlushMetadata {
  fn relative_path(table_name: &String) -> String {
    return format!("{}/flush_metadata.json", table_name);
  }
}

impl FlushMetadata {
  async fn load_or_default(dir: &str, table_name: &String) -> FlushMetadata {
    return FlushMetadata::load(dir, table_name)
      .await
      .map(|x| *x)
      .unwrap_or(FlushMetadata::default());
  }
}

impl Default for FlushMetadata {
  fn default() -> FlushMetadata {
    return FlushMetadata {
      n: 0,
      read_version: 0,
      write_versions: vec![0],
    }
  }
}

pub type FlushMetadataCache = CacheData<String, FlushMetadata>;

impl FlushMetadataCache {
  pub async fn get(&self, table_name: &String) -> FlushMetadata {
    return self.get_option(table_name)
      .await
      .unwrap_or(FlushMetadata::default());
  }

  pub async fn increment_n(&self, table_name: &String, incr: usize) -> Result<(), &'static str> {
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    if !map.contains_key(table_name) {
      map.insert(String::from(table_name), FlushMetadata::load_or_default(&self.dir, table_name).await);
    }
    let metadata = map.get_mut(table_name).unwrap();

    metadata.n = metadata.n + incr;
    return metadata.overwrite(&self.dir, table_name).await
  }

  pub async fn set_read_version(&self, table_name: &String, read_version: u64) -> Result<(), &'static str> {
    let mut new_versions = Vec::new();

    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    if !map.contains_key(table_name) {
      map.insert(String::from(table_name), FlushMetadata::load_or_default(&self.dir, table_name).await);
    }
    let metadata = map.get_mut(table_name).unwrap();

    for version in &metadata.write_versions {
      if *version >= read_version {
        new_versions.push(version.clone());
      }
    }

    metadata.read_version = read_version;
    metadata.write_versions = new_versions;
    return metadata.overwrite(&self.dir, table_name).await
  }

  pub async fn add_write_version(&self, table_name: &String, write_version: u64) -> Result<(), &'static str>{
    let mut mux_guard = self.data.write().await;
    let map = &mut *mux_guard;
    if !map.contains_key(table_name) {
      map.insert(String::from(table_name), FlushMetadata::load_or_default(&self.dir, table_name).await);
    }
    let metadata = map.get_mut(table_name).unwrap();

    metadata.write_versions.push(write_version);
    return metadata.overwrite(&self.dir, table_name).await
  }
}
