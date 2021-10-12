use std::path::{Path, PathBuf};

use crate::types::{CompactionKey, NormalizedPartition, PartitionKey, SegmentKey};

pub fn table_subdir(table_name: &str) -> PathBuf {
  PathBuf::from(table_name)
}

pub fn table_dir(dir: &Path, table_name: &str) -> PathBuf {
  dir.join(table_subdir(table_name))
}

pub fn flush_col_file(dir: &Path, compaction_key: &CompactionKey, col_name: &str) -> PathBuf {
  version_dir(dir, compaction_key).join(format!("f_{}", col_name))
}

pub fn compact_col_file(dir: &Path, compaction_key: &CompactionKey, col_name: &str) -> PathBuf {
  version_dir(dir, compaction_key).join(format!("c_{}", col_name))
}

pub fn partition_subdir(partition: &NormalizedPartition) -> PathBuf {
  partition.fields
    .iter()
    .map(|f| f.to_path_buf())
    .collect()
}

pub fn partition_dir(dir: &Path, table_partition: &PartitionKey) -> PathBuf {
  table_dir(dir, &table_partition.table_name).join(
    partition_subdir(&table_partition.partition)
  )
}

pub fn relative_partition_dir(table_partition: &PartitionKey) -> PathBuf {
  PathBuf::from(&table_partition.table_name)
    .join(&table_partition.partition.to_path_buf())
}

pub fn relative_segment_dir(segment_key: &SegmentKey) -> PathBuf {
  relative_partition_dir(&segment_key.partition_key())
    .join(format!("s_{}", &segment_key.segment_id))
}

pub fn segment_dir(dir: &Path, segment_key: &SegmentKey) -> PathBuf {
  dir.join(relative_segment_dir(segment_key))
}

pub fn relative_version_dir(compaction_key: &CompactionKey) -> PathBuf {
  relative_segment_dir(&compaction_key.segment_key())
    .join(format!("v{}", compaction_key.version))
}

pub fn version_dir(dir: &Path, compaction_key: &CompactionKey) -> PathBuf {
  dir.join(relative_version_dir(compaction_key))
}

pub fn staged_rows_path(dir: &Path, segment_key: &SegmentKey) -> PathBuf {
  segment_dir(dir, segment_key).join("staged_rows")
}
