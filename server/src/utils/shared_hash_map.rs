use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hasher;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::errors::ServerResult;
use crate::storage::MetadataKey;

const HASH_BUCKETS: usize = 16;

pub struct SharedHashMap<K, V>(Vec<RwLock<HashMap<K, Arc<RwLock<V>>>>>)
  where K: MetadataKey;

impl<K, V> SharedHashMap<K, V> where K: MetadataKey {
  pub fn new() -> Self {
    let mut v = Vec::new();
    for _ in 0..HASH_BUCKETS {
      v.push(RwLock::new(HashMap::<K, Arc<RwLock<V>>>::new()));
    }
    SharedHashMap(v)
  }

  fn hash_bucket(k: &K) -> usize {
    let mut hash = DefaultHasher::new();
    k.hash(&mut hash);
    hash.finish() as usize % HASH_BUCKETS
  }

  pub async fn get_lock(&self, k: &K) -> Option<Arc<RwLock<V>>> {
    let map_lock = &self.0[Self::hash_bucket(k)];
    let map_guard = map_lock.read().await;
    let map = &*map_guard;

    map.get(k).cloned()
  }

  pub async fn get_lock_or<Fut, F>(&self, k: &K, load_fn: F) -> ServerResult<Arc<RwLock<V>>>
  where Fut: Future<Output=ServerResult<V>>, F: FnOnce() -> Fut {
    // first check if it already exists
    if let Some(res) = self.get_lock(k).await {
      return Ok(res);
    }

    // otherwise we need to load value and obtain a write lock
    let v = load_fn().await?;
    let map_lock = &self.0[Self::hash_bucket(k)];
    let mut map_guard = map_lock.write().await;
    let map = &mut *map_guard;
    let res = if let Some(lock) = map.get(k) {
      lock.clone()
    } else {
      let entry = Arc::new(RwLock::new(v));
      map.insert(k.clone(), entry.clone());
      entry
    };

    Ok(res)
  }

  pub async fn prune<F>(&self, f: F)
  where F: Fn(&K) -> bool {
    for map_lock in &self.0 {
      let mut map_guard = map_lock.write().await;
      let map = &mut *map_guard;
      let mut to_remove = Vec::new();
      map.keys().for_each(|k| if f(k) {
        to_remove.push(k.clone())
      });
      for k in &to_remove {
        map.remove(k);
      }
    }
  }
}
