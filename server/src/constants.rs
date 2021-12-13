pub const MAX_FIELD_BYTE_SIZE: usize = 4096;
pub const LIST_LENGTH_BYTES: usize = 2;
pub const MAX_NESTED_LIST_DEPTH: u32 = 3;
pub const MAX_PARTITIONING_DEPTH: usize = 4;
pub const MAX_NAME_LENGTH: usize = 255;
pub const MAX_N_COLUMNS: usize = 255;

pub const TABLE_METADATA_FILENAME: &str = "table_metadata.json";
pub const PRE_COMPACTION_DELETIONS_FILENAME: &str = "pre_compaction_deletions.qco";
pub const POST_COMPACTION_DELETIONS_FILENAME: &str = "post_compaction_deletions.qco";
pub const DATA_SUBDIR: &str = "data";

pub const ROW_ID_COLUMN_NAME: &str = "_row_id";
pub const WRITTEN_AT_COLUMN_NAME: &str = "_written_at";

pub const SHARD_ID_BYTE_LENGTH: usize = 2; // so 4 hex chars
