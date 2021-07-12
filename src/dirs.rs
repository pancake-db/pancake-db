use crate::types::{PartitionKey, SegmentKey, CompactionKey, NormalizedPartition};

pub fn table_subdir(table_name: &str) -> String {
  table_name.to_string()
}

pub fn table_dir(dir: &str, table_name: &str) -> String {
  format!("{}/{}", dir, table_subdir(table_name))
}

pub fn flush_col_file(dir: &str, compaction_key: &CompactionKey, col_name: &str) -> String {
  format!("{}/f_{}", version_dir(dir, compaction_key), col_name)
}

pub fn compact_col_file(dir: &str, compaction_key: &CompactionKey, col_name: &str) -> String {
  format!("{}/c_{}", version_dir(dir, compaction_key), col_name)
}

pub fn col_files(dir: &str, compaction_key: &CompactionKey, col_name: &str) -> Vec<String> {
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

pub fn partition_dir(dir: &str, table_partition: &PartitionKey) -> String {
  format!(
    "{}/{}",
    table_dir(dir, &table_partition.table_name),
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

pub fn segment_dir(dir: &str, segment_key: &SegmentKey) -> String {
  format!(
    "{}/{}",
    dir,
    relative_segment_dir(segment_key),
  )
}

pub fn version_dir(dir: &str, compaction_key: &CompactionKey) -> String {
  format!("{}/v{}", segment_dir(dir, &compaction_key.segment_key()), compaction_key.version)
}
