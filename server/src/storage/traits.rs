use std::collections::HashMap;
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

pub trait MetadataJson {
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
pub trait Metadata<K: Sync + MetadataKey>: MetadataJson + Clone + Sync {
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

pub trait MetadataKey: Clone + Debug + Eq + Hash + Sync {
  const ENTITY_NAME: &'static str;
}

#[derive(Clone)]
pub struct CacheData<K, V> where V: Metadata<K> + Send, K: MetadataKey {
  pub dir: PathBuf,
  pub data: Arc<RwLock<HashMap<K, Option<V>>>>
}

impl<K, V> CacheData<K, V> where V: Metadata<K> + Send, K: MetadataKey  {
  pub async fn get_option(&self, k: &K) -> ServerResult<Option<V>> {
    let mux_guard = self.data.read().await;
    let map = &*mux_guard;

    let maybe_metadata = map.get(k).cloned();
    drop(mux_guard);

    match maybe_metadata {
      Some(metadata) => Ok(metadata),
      None => {
        let (value_result, mut mux_guard) = futures::future::join(
          V::load(&self.dir, k),
          self.data.write()
        ).await;
        let value = value_result?;
        let map = &mut *mux_guard;
        map.insert(k.clone(), value.clone());
        Ok(value)
      },
    }
  }

  pub async fn get_or_err(&self, k: &K) -> ServerResult<V> {
    match self.get_option(k).await? {
      Some(metadata) => Ok(metadata),
      None => Err(ServerError::does_not_exist(K::ENTITY_NAME, &format!("{:?}", k)))
    }
  }

  pub fn new(dir: &Path) -> Self {
    CacheData {
      dir: dir.to_path_buf(),
      data: Arc::new(RwLock::new(HashMap::new()))
    }
  }
}
