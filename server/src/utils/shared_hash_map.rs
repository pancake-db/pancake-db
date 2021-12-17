use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hasher;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::errors::{ServerResult, Contextable};
use crate::metadata::MetadataKey;
use rand::Rng;

const HASH_BUCKETS: usize = 16;
const PRUNE_P: f64 = 0.5;

pub struct SharedHashMap<K: MetadataKey, V>{
  per_bucket_size_limit: usize,
  maps: Vec<RwLock<HashMap<K, Arc<RwLock<V>>>>>,
}

impl<K, V> SharedHashMap<K, V> where K: MetadataKey {
  pub fn new(size_limit: usize) -> Self {
    let mut maps = Vec::new();
    for _ in 0..HASH_BUCKETS {
      maps.push(RwLock::new(HashMap::<K, Arc<RwLock<V>>>::new()));
    }
    SharedHashMap {
      per_bucket_size_limit: size_limit / HASH_BUCKETS,
      maps,
    }
  }

  fn hash_bucket(k: &K) -> usize {
    let mut hash = DefaultHasher::new();
    k.hash(&mut hash);
    hash.finish() as usize % HASH_BUCKETS
  }

  pub async fn get_lock(&self, k: &K) -> (Option<Arc<RwLock<V>>>, usize) {
    let bucket_idx = Self::hash_bucket(k);
    let map_lock = &self.maps[bucket_idx];
    let map_guard = map_lock.read().await;

    (map_guard.get(k).cloned(), map_guard.len())
  }

  pub async fn get_lock_or<Fut, F>(&self, k: &K, load_fn: F) -> ServerResult<Arc<RwLock<V>>>
  where Fut: Future<Output=ServerResult<V>>, F: FnOnce() -> Fut {
    // first check if it already exists and the cache is of appropriate size
    let (mut maybe_res, bucket_size) = self.get_lock(k).await;

    // if the value is already cached and the bucket size is within bounds, we
    // can stop
    let needs_insert = maybe_res.is_none();
    let needs_prune = bucket_size + (needs_insert as usize) >
      self.per_bucket_size_limit;
    if !needs_insert && !needs_prune {
      return Ok(maybe_res.unwrap());
    }

    // otherwise we need to obtain a write lock and then prune and/or add the
    // loaded value into the cache
    let maybe_loaded_value = if needs_insert {
      Some(load_fn().await.with_context(|| format!(
        "while getting RwLock from {} SharedHashMap",
        K::ENTITY_NAME,
      ))?)
    } else {
      None
    };

    let bucket = Self::hash_bucket(k);
    let map_lock = &self.maps[bucket];
    let mut map = map_lock.write().await;

    // prune if needed
    if needs_prune {
      // If we pruned indiscriminately, we could have the following thread
      // safety issue:
      //   * thread A lets lock_a = map["asdf"]
      //   * thread B prunes by deleting map["asdf"]
      //   * thread A does lock_a.write()
      //   * thread C checks the cache, sees nothing under "asdf", creates
      //     a new entry, and obtains lock_b = map["asdf"] (a different lock
      //     to a different piece of memory)
      //   * thread C does lock_a.read()
      //   * now thread B has a write lock and C has a read lock on different
      //     caches of the same data simultaneously
      // To prevent this, we will leverage the handy `Arc` by only pruning
      // entries with no reference counts (other than the map's strong count of
      // 1). It is safe to do this because we have a write lock on the whole
      // map, and if the reference count is 1, no other thread can increase it.
      let to_prune: Vec<_> = {
        let mut rng = rand::thread_rng();
        map.iter()
          .filter(|(other_k, arc)|
            rng.gen_bool(PRUNE_P) &&
              *other_k != k &&
              Arc::strong_count(arc) <= 1 // the magic
          )
          .map(|(other_k, _)| other_k.clone())
          .collect()
      };
      log::debug!(
        "pruning {}/{} entries from {} cache bucket {}",
        to_prune.len(),
        map.len(),
        K::ENTITY_NAME,
        bucket,
      );
      for other_k in &to_prune {
        map.remove(other_k);
      }
    }

    // insert to cache if needed
    if needs_insert {
      let res = if let Some(lock) = map.get(k) {
        lock.clone()
      } else {
        let entry = Arc::new(RwLock::new(maybe_loaded_value.unwrap()));
        map.insert(k.clone(), entry.clone());
        entry
      };
      maybe_res = Some(res);
    }

    Ok(maybe_res.unwrap())
  }

  pub async fn prune_unsafe<F>(&self, f: F)
  where F: Fn(&K) -> bool {
    for map_lock in &self.maps {
      let mut map_guard = map_lock.write().await;
      let map = &mut *map_guard;
      let to_remove: Vec<_> = map.keys()
        .filter(|k| f(k))
        .cloned()
        .collect();
      for k in &to_remove {
        map.remove(k);
      }
    }
  }
}
