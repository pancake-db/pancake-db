use std::collections::HashMap;
use std::sync::Arc;
use std::hash::Hash;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::fs;
use tokio::sync::RwLock;
use async_trait::async_trait;

use crate::utils;

#[async_trait]
pub trait Metadata<K: Sync>: Serialize + DeserializeOwned + Clone + Sync {
  fn relative_path(k: &K) -> String;

  fn path(dir: &str, k: &K) -> String {
    return format!("{}/{}", dir, Self::relative_path(k));
  }

  async fn load(dir: &str, k: &K) -> Option<Box<Self>> {
    return match fs::read_to_string(Self::path(dir, k)).await {
      Ok(json_str) => Some(serde_json::from_str(&json_str).unwrap()),
      Err(_) => None,
    }
  }

  async fn overwrite(&self, dir: &String, k: &K) -> Result<(), &'static str> {
    let path = Self::path(dir, k);
    let metadata_str = serde_json::to_string(&self).expect("unable to serialize");
    return utils::overwrite_file(&path, metadata_str.as_bytes()).await;
  }
}

#[derive(Clone)]
pub struct CacheData<K, V> where V: Metadata<K> + Send, K: Eq + Hash + Sync {
  pub dir: String,
  pub data: Arc<RwLock<HashMap<K, V>>>
}

impl<K, V> CacheData<K, V> where V: Metadata<K> + Send, K: Eq + Hash + Sync  {
  pub async fn get_option(&self, k: &K) -> Option<V> {
    let mux_guard = self.data.read().await;
    let map = &*mux_guard;

    let maybe_metadata = map.get(k);
    return match maybe_metadata {
      Some(metadata) => Some(metadata.clone()),
      None => V::load(&self.dir, k).await.map(|x| *x),
    };
  }

  pub fn new(dir: &str) -> Self {
    return CacheData {
      dir: String::from(dir),
      data: Arc::new(RwLock::new(HashMap::new()))
    };
  }
}
