pub mod traits;
pub mod alter_table;
pub mod create_table;
pub mod drop_table;
pub mod get_schema;
pub mod list_segments;
pub mod write_to_partition;
pub mod flush;
pub mod read_segment_column;
pub mod compact;
pub mod list_tables;
pub mod delete_from_segment;
pub mod read_segment_deletions;

pub mod create_table_rest;
pub mod drop_table_rest;
pub mod list_tables_rest;
pub mod write_to_partition_rest;
