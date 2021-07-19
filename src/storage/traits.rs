use std::collections::HashMap;
use std::sync::Arc;
use std::hash::Hash;

use anyhow::{anyhow, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::fs;
use tokio::sync::RwLock;
use async_trait::async_trait;

use crate::utils;
use std::path::{PathBuf, Path};

#[async_trait]
pub trait Metadata<K: Sync>: Serialize + DeserializeOwned + Clone + Sync {
  fn relative_path(k: &K) -> PathBuf;

  fn path(dir: &Path, k: &K) -> PathBuf {
    dir.join(Self::relative_path(k))
  }

  async fn load(dir: &Path, k: &K) -> Option<Box<Self>> {
    return match fs::read_to_string(Self::path(dir, k)).await {
      Ok(json_str) => Some(serde_json::from_str(&json_str).unwrap()),
      Err(_) => None,
    }
  }

  async fn overwrite(&self, dir: &Path, k: &K) -> Result<()> {
    let path = Self::path(dir, k);
    let metadata_str = serde_json::to_string(&self).expect("unable to serialize");
    return utils::overwrite_file(&path, metadata_str.as_bytes()).await;
  }
}

#[derive(Clone)]
pub struct CacheData<K, V> where V: Metadata<K> + Send, K: Clone + Eq + Hash + Sync {
  pub dir: PathBuf,
  pub data: Arc<RwLock<HashMap<K, Option<V>>>>
}

impl<K, V> CacheData<K, V> where V: Metadata<K> + Send, K: Clone + Eq + Hash + Sync  {
  pub async fn get_option(&self, k: &K) -> Option<V> {
    let mux_guard = self.data.read().await;
    let map = &*mux_guard;

    let maybe_metadata = map.get(k).cloned();
    drop(mux_guard);

    return match maybe_metadata {
      Some(metadata) => metadata,
      None => {
        let (res_box, mut mux_guard) = futures::future::join(
          V::load(&self.dir, k),
          self.data.write()
        ).await;
        let map = &mut *mux_guard;
        let res = res_box.map(|x| *x);
        map.insert(k.clone(), res.clone());
        res
      },
    };
  }

  pub fn new(dir: &Path) -> Self {
    CacheData {
      dir: dir.to_path_buf(),
      data: Arc::new(RwLock::new(HashMap::new()))
    }
  }
}
