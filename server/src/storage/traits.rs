
use std::fmt::Debug;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::fs;
use tokio::sync::RwLock;
use std::io;

use crate::errors::{ServerError, ServerResult};
use crate::utils;
use crate::storage::shared_hash_map::SharedHashMap;

pub trait MetadataJson: Clone + Send + Sync {
  fn to_json_string(&self) -> ServerResult<String>;
  fn from_json_str(s: &str) -> ServerResult<Self> where Self: Sized;
}

#[macro_export]
macro_rules! impl_metadata_serde_json {
  ($T:ident) => {
    impl $crate::storage::traits::MetadataJson for $T {
      fn to_json_string(&self) -> ServerResult<String> {
        Ok(serde_json::to_string(&self)?)
      }
      fn from_json_str(s: &str) -> ServerResult<Self> {
        Ok(serde_json::from_str(s)?)
      }
    }
  }
}

#[async_trait]
pub trait Metadata<K: MetadataKey>: MetadataJson {
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
    return Ok(utils::overwrite_file(&path, metadata_str.as_bytes()).await?);
  }
}

pub trait MetadataKey: Clone + Debug + Eq + Hash + Send + Sync {
  const ENTITY_NAME: &'static str;
}

#[derive(Clone)]
pub struct CacheData<K, M> where M: Metadata<K>, K: MetadataKey {
  pub dir: PathBuf,
  pub data: Arc<SharedHashMap<K, Option<M>>>,
}

impl<K, M> CacheData<K, M> where M: Metadata<K>, K: MetadataKey  {
  pub async fn get_lock(&self, k: &K) -> ServerResult<Arc<RwLock<Option<M>>>> {
    self.data.get_lock_or(k, || {
      M::load(&self.dir, k)
    }).await
  }

  pub fn new(dir: &Path) -> Self {
    CacheData {
      dir: dir.to_path_buf(),
      data: Arc::new(SharedHashMap::new()),
    }
  }
}
