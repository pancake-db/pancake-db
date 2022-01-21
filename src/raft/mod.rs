use std::convert::TryFrom;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use raft::{Config, RaftState, RawNode, Storage};
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use rand::Rng;

use crate::errors::{ServerError, ServerResult};
use crate::errors::Contextable;
use crate::metadata::raft::RaftStateSerde;
use crate::opt::Opt;
use crate::types::ShardId;
use crate::utils::common;
use crate::utils::dirs;

const STATE_FILENAME: &str = "raft_state.json";

pub struct ShardDiskStorage {
  id: ShardId,
  opts: Opt,
  state_lock: Arc<RwLock<RaftState>>,
}

impl ShardDiskStorage {
  fn load(dir: &Path, id: ShardId, opts: &Opt) -> ServerResult<Self> {
    let state_path = dirs::shard_dir(dir, &id).join(STATE_FILENAME);
    let bytes = fs::read(state_path)?;
    let content = String::try_from(bytes)?;
    let state = RaftState::from_json_str(&content)?;
    Ok(Self {
      id,
      opts: opts.clone(),
      state_lock: Arc::new(RwLock::new(state)),
    })
  }

  fn dir(&self) -> PathBuf {
    dirs::shard_dir(&self.opts.dir, &self.id)
  }

  fn state_path(&self) -> PathBuf {
    self.dir().join(STATE_FILENAME)
  }
}

impl Storage for ShardDiskStorage {
  fn initial_state(&self) -> raft::Result<RaftState> {
    Ok(self.state_lock.read().unwrap().clone())
  }

  fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> raft::Result<Vec<Entry>> {
    todo!()
  }
  fn term(&self, idx: u64) -> raft::Result<u64> {
    todo!()
  }
  fn first_index(&self) -> raft::Result<u64> {
    todo!()
  }
  fn last_index(&self) -> raft::Result<u64> {
    todo!()
  }
  fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
    todo!()
  }
}

pub async fn new(node_id: u64, shard_id: ShardId, opts: &Opt) -> ServerResult<RawNode<ShardDiskStorage>> {
  let config = Config {
    id: node_id,
    ..Default::default()
  };
  let store = ShardDiskStorage::load(
    &opts.dir,
    shard_id,
    opts,
  ).with_context(|| format!("while loading shard {} raft state", shard_id))?;
  RawNode::new(&config, store, &log::logger())
    .map_err(|e| ServerError::from(e).with_context(format!()))
}