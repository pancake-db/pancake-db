pub fn table_dir(dir: &str, table_name: &str) -> String {
  return format!("{}/{}", dir, table_name);
}

pub fn version_dir(dir: &str, table_name: &str, version: u64) -> String {
  return format!("{}/v{}", table_dir(dir, table_name), version);
}

pub fn flush_col_file(dir: &str, table_name: &str, version: u64, col_name: &str) -> String {
  return format!("{}/f_{}", version_dir(dir, table_name, version), col_name);
}

pub fn compact_col_file(dir: &str, table_name: &str, version: u64, col_name: &str) -> String {
  return format!("{}/c_{}", version_dir(dir, table_name, version), col_name);
}

pub fn col_files(dir: &str, table_name: &str, version: u64, col_name: &str) -> Vec<String> {
  return vec![
    flush_col_file(dir, table_name, version, col_name),
    compact_col_file(dir, table_name, version, col_name),
  ];
}
