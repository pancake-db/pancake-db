
use std::fmt::Debug;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::fs;
use tokio::sync::RwLock;
use std::io;

use crate::errors::{ServerError, ServerResult};
use crate::utils::common;
use crate::utils::shared_hash_map::SharedHashMap;

pub trait MetadataJson: Clone + Send + Sync {
  fn to_json_string(&self) -> ServerResult<String>;
  fn from_json_str(s: &str) -> ServerResult<Self> where Self: Sized;
}

#[macro_export]
macro_rules! impl_metadata_serde_json {
  ($T:ident) => {
    impl $crate::metadata::traits::MetadataJson for $T {
      fn to_json_string(&self) -> ServerResult<String> {
        Ok(serde_json::to_string(&self)?)
      }
      fn from_json_str(s: &str) -> ServerResult<Self> {
        Ok(serde_json::from_str(s)?)
      }
    }
  }
}

pub trait EphemeralMetadata: Clone + Send + Sync {
  const CACHE_SIZE_LIMIT: usize;
}

#[async_trait]
pub trait PersistentMetadata<K: MetadataKey>: MetadataJson {
  const CACHE_SIZE_LIMIT: usize;

  fn relative_path(k: &K) -> PathBuf;

  fn path(dir: &Path, k: &K) -> PathBuf {
    dir.join(Self::relative_path(k))
  }

  async fn load(dir: &Path, k: &K) -> ServerResult<Option<Self>> {
    return match fs::read_to_string(Self::path(dir, k)).await {
      Ok(json_string) => {
        Ok(Some(Self::from_json_str(&json_string)?))
      },
      Err(e) => {
        match e.kind() {
          io::ErrorKind::NotFound => Ok(None),
          _ => Err(ServerError::internal(&format!(
            "unable to read {}: {:?}",
            K::ENTITY_NAME,
            e,
          )))
        }
      },
    }
  }

  async fn overwrite(&self, dir: &Path, k: &K) -> ServerResult<()> {
    let path = Self::path(dir, k);
    let metadata_str = self.to_json_string()?;
    return Ok(common::overwrite_file(&path, metadata_str.as_bytes()).await?);
  }
}

pub trait MetadataKey: Clone + Debug + Eq + Hash + Send + Sync {
  const ENTITY_NAME: &'static str;
}

#[derive(Clone)]
pub struct PersistentCacheData<K, M> where M: PersistentMetadata<K>, K: MetadataKey {
  dir: PathBuf,
  data: Arc<SharedHashMap<K, Option<M>>>,
}

impl<K, M> PersistentCacheData<K, M> where M: PersistentMetadata<K>, K: MetadataKey  {
  pub fn new(dir: &Path) -> Self {
    PersistentCacheData {
      dir: dir.to_path_buf(),
      data: Arc::new(SharedHashMap::new(M::CACHE_SIZE_LIMIT)),
    }
  }

  pub async fn get_lock(&self, k: &K) -> ServerResult<Arc<RwLock<Option<M>>>> {
    self.data.get_lock_or(k, || {
      M::load(&self.dir, k)
    }).await
  }

  pub async fn prune<F>(&self, f: F)
  where F: Fn(&K) -> bool {
    self.data.prune_unsafe(f).await
  }
}

#[derive(Clone)]
pub struct EphemeralCacheData<K, M> where M: EphemeralMetadata, K: MetadataKey {
  data: Arc<SharedHashMap<K, Option<M>>>,
}

async fn async_none<M: EphemeralMetadata>() -> ServerResult<Option<M>> {
  Ok(None)
}

impl<K, M> EphemeralCacheData<K, M> where M: EphemeralMetadata, K: MetadataKey  {
  pub fn new() -> Self {
    EphemeralCacheData {
      data: Arc::new(SharedHashMap::new(M::CACHE_SIZE_LIMIT)),
    }
  }

  pub async fn get_lock(&self, k: &K) -> ServerResult<Arc<RwLock<Option<M>>>> {
    self.data.get_lock_or(k, async_none).await
  }

  pub async fn prune<F>(&self, f: F)
    where F: Fn(&K) -> bool {
    self.data.prune_unsafe(f).await
  }
}
