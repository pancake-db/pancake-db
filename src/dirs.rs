use crate::types::{PartitionKey, SegmentKey, CompactionKey, NormalizedPartition};
use std::path::PathBuf;

pub fn table_subdir(table_name: &str) -> String {
  table_name.to_string()
}

pub fn table_dir(dir: &PathBuf, table_name: &str) -> PathBuf {
  dir.join(table_subdir(table_name))
}

pub fn flush_col_file(dir: &PathBuf, compaction_key: &CompactionKey, col_name: &str) -> PathBuf {
  version_dir(dir, compaction_key).join(format!("f_{}", col_name))
}

pub fn compact_col_file(dir: &PathBuf, compaction_key: &CompactionKey, col_name: &str) -> PathBuf {
  version_dir(dir, compaction_key).join(format!("c_{}", col_name))
}

pub fn col_files(dir: &PathBuf, compaction_key: &CompactionKey, col_name: &str) -> Vec<PathBuf> {
  vec![
    flush_col_file(dir, compaction_key, col_name),
    compact_col_file(dir, compaction_key, col_name),
  ]
}

pub fn partition_subdir(partition: &NormalizedPartition) -> String {
  partition.fields
    .iter()
    .map(|f| f.to_string())
    .collect::<Vec<String>>()
    .join("/")
}

pub fn partition_dir(dir: &PathBuf, table_partition: &PartitionKey) -> PathBuf {
  table_dir(dir, &table_partition.table_name).join(
    partition_subdir(&table_partition.partition)
  )
}

pub fn relative_partition_dir(table_partition: &PartitionKey) -> String {
  format!(
    "{}/{}",
    table_subdir(&table_partition.table_name),
    partition_subdir(&table_partition.partition),
  )
}

pub fn relative_segment_dir(segment_key: &SegmentKey) -> String {
  format!(
    "{}/s_{}",
    relative_partition_dir(&segment_key.partition_key()),
    segment_key.segment_id,
  )
}

pub fn segment_dir(dir: &PathBuf, segment_key: &SegmentKey) -> PathBuf {
  dir.join(relative_segment_dir(segment_key))
}

pub fn version_dir(dir: &PathBuf, compaction_key: &CompactionKey) -> PathBuf {
  segment_dir(dir, &compaction_key.segment_key()).join(format!("v{}", compaction_key.version))
}
