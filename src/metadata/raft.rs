use std::convert::TryFrom;
use std::path::PathBuf;

use pancake_db_idl::schema::Schema;
use protobuf::json;
use protobuf::json::{ParseError, PrintError};
use raft::{RawNode, RaftState};
use serde::{Deserialize, Serialize};

use crate::constants::TABLE_METADATA_FILENAME;
use crate::errors::{ServerError, ServerResult};
use crate::metadata::traits::{MetadataJson, MetadataKey, PersistentCacheData, PersistentMetadata};
use crate::types::ShardId;
use crate::utils::dirs;

#[derive(Debug, Serialize, Deserialize)]
pub struct RaftStateSerde {
  pub conf_state_string: String,
  pub hard_state_string: String,
}

impl TryFrom<RaftState> for RaftStateSerde {
  type Error = PrintError;

  fn try_from(value: RaftState) -> Result<Self, Self::Error> {
    let conf_state_string = json::print_to_string(&value.conf_state)?;
    let hard_state_string = json::print_to_string(&value.hard_state)?;
    Ok(RaftStateSerde {
      conf_state_string,
      hard_state_string,
    })
  }
}

impl TryFrom<RaftStateSerde> for RaftState {
  type Error = ParseError;

  fn try_from(value: RaftStateSerde) -> Result<Self, Self::Error> {
    let hard_state = json::parse_from_str(&value.hard_state_string)?;
    let conf_state = json::parse_from_str(&value.conf_state_string)?;
    Ok(RaftState {
      hard_state,
      conf_state,
    })
  }
}

impl MetadataJson for RaftState {
  fn to_json_string(&self) -> ServerResult<String> {
    let raft_state_serde = RaftStateSerde::try_from(self.clone())
      .map_err(|_| ServerError::internal("unable to print schema to json string"))?;
    Ok(serde_json::to_string(&raft_state_serde)?)
  }

  fn from_json_str(s: &str) -> ServerResult<Self> {
    let raft_state_serde: RaftStateSerde = serde_json::from_str::<'_, RaftStateSerde>(s)?;
    Ok(RaftState::try_from(raft_state_serde)?)
  }
}

impl PersistentMetadata<ShardId> for RaftState {
  const CACHE_SIZE_LIMIT: usize = 1;

  fn relative_path(key: &ShardId) -> PathBuf {
    dirs::relative_shard_dir(key).join("raft_state.json")
  }
}

pub type RaftStateCache = PersistentCacheData<ShardId, RaftState>;
