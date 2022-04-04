use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::{Debug, Formatter, Display};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use pancake_db_idl::dml::partition_field_value::Value;
use pancake_db_idl::dml::PartitionFieldValue;
use pancake_db_idl::schema::Schema;
use prost_types::Timestamp;
use rand::Rng;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::constants::SHARD_ID_BYTE_LENGTH;
use crate::errors::{ServerError, ServerResult};
use crate::metadata::table::TableMetadata;
use crate::utils::{common, sharding};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EmptyKey;

impl Display for EmptyKey {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "(null key)"
    )
  }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct PartitionMinute {
  pub minutes: i64,
}

impl TryFrom<&Timestamp> for PartitionMinute {
  type Error = ServerError;

  fn try_from(t: &Timestamp) -> ServerResult<PartitionMinute> {
    if t.nanos != 0 {
      Err(ServerError::invalid(format!(
        "whole minute expected but {} nanoseconds found in timestamp",
        t.nanos,
      )))
    } else if t.seconds % 60 != 0 {
      Err(ServerError::invalid(format!(
        "whole minute expected but {} extra seconds found in timestamp",
        t.seconds,
      )))
    } else {
      Ok(PartitionMinute {
        minutes: t.seconds.div_euclid(60)
      })
    }
  }
}

// we use our own type instead of idl partition_field.Value so we can
// have Hash, among other things
#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum NormalizedPartitionValue {
  String(String),
  Int64(i64),
  Bool(bool),
  Minute(PartitionMinute),
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NormalizedPartitionField {
  pub name: String,
  pub value: NormalizedPartitionValue,
}

impl Display for NormalizedPartitionField {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let value_str = match &self.value {
      NormalizedPartitionValue::String(x) => x.clone(),
      NormalizedPartitionValue::Int64(x) => x.to_string(),
      NormalizedPartitionValue::Bool(x) => if *x {"true"} else {"false"}.to_string(),
      NormalizedPartitionValue::Minute(x) => x.minutes.to_string(), // TODO
    };
    write!(
      f,
      "{}={}",
      self.name,
      value_str,
    )
  }
}

impl NormalizedPartitionField {
  pub fn to_path_buf(&self) -> PathBuf {
    PathBuf::from(self.to_string())
  }

  pub fn try_from_raw(name: &str, raw_field: &PartitionFieldValue) -> ServerResult<NormalizedPartitionField> {
    let value_result: ServerResult<NormalizedPartitionValue> = match raw_field.value.as_ref() {
      Some(Value::StringVal(x)) => {
        common::validate_partition_string(x)?;
        Ok(NormalizedPartitionValue::String(x.clone()))
      },
      Some(Value::Int64Val(x)) => Ok(NormalizedPartitionValue::Int64(*x)),
      Some(Value::BoolVal(x)) => Ok(NormalizedPartitionValue::Bool(*x)),
      Some(Value::TimestampVal(x)) => {
        let minute = PartitionMinute::try_from(x)?;
        Ok(NormalizedPartitionValue::Minute(minute))
      },
      None => Err(ServerError::invalid(format!("partition field value for {} is empty", name))),
    };
    let value = value_result?;
    Ok(NormalizedPartitionField {
      name: name.to_string(),
      value,
    })
  }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NormalizedPartition {
  fields: Vec<NormalizedPartitionField>
}

impl Display for NormalizedPartition {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    for field in &self.fields {
      write!(
        f,
        "{}",
        field,
      )?;
    }
    Ok(())
  }
}

impl NormalizedPartition {
  pub fn to_path_buf(&self) -> PathBuf {
    self.fields.iter().map(|f| f.to_path_buf()).collect()
  }

  pub fn check_against_schema(&self, schema: &Schema) -> ServerResult<()> {
    if schema.partitioning.len() != self.fields.len() {
      return Err(ServerError::invalid("number of partition fields does not match schema"));
    }
    let mut field_map = HashMap::new();
    for field in &self.fields {
      field_map.insert(&field.name, field);
    }
    for (partition_name, partition_meta) in &schema.partitioning {
      let maybe_field = field_map.get(partition_name);
      if maybe_field.is_none() {
        return Err(ServerError::invalid(format!("partition field {} is missing", partition_name)));
      }
      let field = *maybe_field.unwrap();
      if !common::partition_dtype_matches_field(
        &common::unwrap_partition_dtype(partition_meta.dtype)?,
        field
      ) {
        return Err(ServerError::invalid("partition field dtype does not match schema"));
      }
    }
    Ok(())
  }

  pub fn from_normalized_fields(fields: &[NormalizedPartitionField]) -> NormalizedPartition {
    let mut fields = fields.to_vec();
    fields.sort_by_key(|f| f.name.clone());
    NormalizedPartition {
      fields
    }
  }

  pub fn from_raw_fields(
    raw_fields: &HashMap<String, PartitionFieldValue>
  ) -> ServerResult<NormalizedPartition> {
    let mut normalized_fields = Vec::new();
    for (partition_name, pfv) in raw_fields {
      normalized_fields.push(NormalizedPartitionField::try_from_raw(partition_name, pfv)?);
    }
    Ok(NormalizedPartition::from_normalized_fields(&normalized_fields))
  }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct PartitionKey {
  pub table_name: String,
  pub partition: NormalizedPartition,
}

impl Display for PartitionKey {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{}/{}",
      self.table_name,
      self.partition,
    )
  }
}

impl PartitionKey {
  pub fn shard_key(&self, shard_id: ShardId) -> ShardKey {
    ShardKey {
      table_name: self.table_name.clone(),
      partition: self.partition.clone(),
      shard_id,
    }
  }

  pub fn segment_key(&self, segment_id: Uuid) -> SegmentKey {
    SegmentKey {
      table_name: self.table_name.clone(),
      partition: self.partition.clone(),
      segment_id,
    }
  }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct ShardId {
  pub n_shards_log: u32,
  pub replication_factor: u32,
  pub shard: u64,
}

impl ShardId {
  pub fn randomly_select(
    n_shards_log: u32,
    replication_factor: u32,
    partition_key: &PartitionKey,
    sharding_denominator_log: u32,
  ) -> Self {
    let n_shards = 1_u64 << n_shards_log;
    let mut hasher = DefaultHasher::new();
    partition_key.hash(&mut hasher);
    let base_shard = hasher.finish() % n_shards;

    let offset_range = 1_u64 << (n_shards_log - sharding_denominator_log);
    let mut rng = rand::thread_rng();
    let offset: u64 = rng.gen_range(0..offset_range);
    let shard = (base_shard + offset) % n_shards;
    ShardId {
      replication_factor,
      n_shards_log,
      shard,
    }
  }

  pub fn children(&self) -> (ShardId, ShardId) {
    let parent_n_shards_log = self.n_shards_log + 1;
    let replication_factor = self.replication_factor;
    (
      ShardId {
        replication_factor,
        n_shards_log: parent_n_shards_log,
        shard: self.shard * 2,
      },
      ShardId {
        replication_factor,
        n_shards_log: parent_n_shards_log,
        shard: self.shard * 2 + 1,
      }
    )
  }

  pub fn parent(&self) -> Option<ShardId> {
    if self.n_shards_log > 0 {
      Some(ShardId {
        replication_factor: self.replication_factor,
        n_shards_log: self.n_shards_log - 1,
        shard: self.shard / 2,
      })
    } else {
      None
    }
  }

  pub fn contains_segment_id(&self, segment_id: Uuid) -> bool {
    let segment_shard = sharding::segment_id_to_shard(self.n_shards_log, segment_id);
    segment_shard == self.shard
  }

  pub fn generate_segment_id(&self) -> Uuid {
    // create a UUID and overwrite the first bits with the shard ID bits
    let uuid = Uuid::new_v4();
    if self.n_shards_log == 0 {
      return uuid;
    }

    let mut uuid_bytes = *uuid.as_bytes();
    let shard_bytes = (self.shard << (64 - self.n_shards_log)).to_be_bytes();

    for bit_idx in 0..self.n_shards_log {
      let byte_idx = (bit_idx / 8) as usize;
      let byte_diff = uuid_bytes[byte_idx] ^ shard_bytes[byte_idx];
      let shift = 7 - bit_idx % 8;
      if (byte_diff >> shift) & 1 == 1 {
        uuid_bytes[byte_idx] ^= 1 << shift
      }
    }

    Uuid::from_bytes(uuid_bytes)
  }
}

impl Display for ShardId {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    if self.n_shards_log == 0 {
      return Ok(());
    }

    let shifted = self.shard << (64 - self.n_shards_log);
    let bytes = shifted.to_be_bytes();
    let mut res = String::with_capacity(16);
    for byte in bytes.iter().take(SHARD_ID_BYTE_LENGTH) {
      res.push_str(&format!("{:#02}", byte))
    }
    write!(
      f,
      "{}_{}_{}",
      self.n_shards_log,
      self.replication_factor,
      res,
    )
  }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct ShardKey {
  pub table_name: String,
  pub partition: NormalizedPartition,
  pub shard_id: ShardId,
}

impl Display for ShardKey {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{}/{} shard {}",
      self.table_name,
      self.partition,
      self.shard_id,
    )
  }
}

impl ShardKey {
  pub fn partition_key(&self) -> PartitionKey {
    PartitionKey {
      table_name: self.table_name.clone(),
      partition: self.partition.clone(),
    }
  }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct SegmentKey {
  pub table_name: String,
  pub partition: NormalizedPartition,
  pub segment_id: Uuid,
}

impl Display for SegmentKey {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{}/{} segment {}",
      self.table_name,
      self.partition,
      self.segment_id
    )
  }
}

impl SegmentKey {
  pub fn partition_key(&self) -> PartitionKey {
    PartitionKey {
      table_name: self.table_name.clone(),
      partition: self.partition.clone(),
    }
  }

  pub fn compaction_key(&self, version: u64) -> CompactionKey {
    CompactionKey {
      table_name: self.table_name.clone(),
      partition: self.partition.clone(),
      segment_id: self.segment_id,
      version,
    }
  }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct CompactionKey {
  pub table_name: String,
  pub partition: NormalizedPartition,
  pub segment_id: Uuid,
  pub version: u64,
}

impl Display for CompactionKey {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{}/{} segment {} version {}",
      self.table_name,
      self.partition,
      self.segment_id,
      self.version,
    )
  }
}

impl CompactionKey {
  pub fn segment_key(&self) -> SegmentKey {
    SegmentKey {
      table_name: self.table_name.clone(),
      partition: self.partition.clone(),
      segment_id: self.segment_id,
    }
  }
}

#[derive(Clone, Debug)]
pub struct InternalTableInfo {
  pub name: String,
  pub meta: TableMetadata,
}
