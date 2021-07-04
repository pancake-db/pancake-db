// use std::collections::HashMap;
// use std::sync::Arc;
//
// use serde::{Deserialize, Serialize};
// use tokio::fs;
// use tokio::sync::RwLock;
//
// use crate::dirs;
// use crate::utils;
//
// #[derive(Serialize, Deserialize, Clone)]
// pub struct FlushMetadata {
//   pub n: usize,
//   pub write_versions: Vec<u64>,
//   pub read_version: u64,
// }
//
// impl FlushMetadata {
//   fn empty() -> FlushMetadata {
//     return FlushMetadata {
//       n: 0,
//       write_versions: vec![0],
//       read_version: 0,
//     };
//   }
//
//   pub async fn read_from_disk(dir: &str, table_name: &str) -> FlushMetadata {
//     let file = dirs::flush_metadata_file(dir, table_name);
//     let json_str;
//     match fs::read_to_string(file).await {
//       Ok(x) => {
//         json_str = x;
//       }
//       Err(_) => {
//         return FlushMetadata::empty();
//       }
//     }
//     return serde_json::from_str(&json_str).unwrap();
//   }
//
//   async fn overwrite(&self, dir: &str, table_name: &str) {
//     let path = dirs::flush_metadata_file(dir, table_name);
//     let metadata_str = serde_json::to_string(&self).expect("unable to serialize flush");
//     utils::overwrite_file(&path, metadata_str.as_bytes()).await;
//   }
// }
//
// type FlushMetadataMap = Arc<RwLock<HashMap<String, FlushMetadata>>>;
//
// #[derive(Default, Clone)]
// pub struct FlushMetadataCache {
//   pub dir: String,
//   pub data: FlushMetadataMap
// }
//
// impl FlushMetadataCache {
//   pub async fn get(&self, table_name: &str) -> FlushMetadata {
//     let mux_guard = self.data.read().await;
//     let map = &*mux_guard;
//
//     let maybe_metadata = map.get(table_name);
//     return maybe_metadata
//         .map(|x| x.clone())
//         .unwrap_or(FlushMetadata::read_from_disk(&self.dir, table_name).await);
//   }
//
//   pub async fn increment_n(&self, table_name: &str, incr: usize) {
//     let mut mux_guard = self.data.write().await;
//     let map = &mut *mux_guard;
//     if !map.contains_key(table_name) {
//       map.insert(String::from(table_name), FlushMetadata::read_from_disk(&self.dir, table_name).await);
//     }
//     let metadata = map.get_mut(table_name).unwrap();
//
//     metadata.n = metadata.n + incr;
//     metadata.overwrite(&self.dir, table_name).await;
//   }
//
//   pub async fn set_read_version(&self, table_name: &str, read_version: u64) {
//     let mut new_versions = Vec::new();
//
//     let mut mux_guard = self.data.write().await;
//     let map = &mut *mux_guard;
//     if !map.contains_key(table_name) {
//       map.insert(String::from(table_name), FlushMetadata::read_from_disk(&self.dir, table_name).await);
//     }
//     let metadata = map.get_mut(table_name).unwrap();
//
//     for version in &metadata.write_versions {
//       if *version >= read_version {
//         new_versions.push(version.clone());
//       }
//     }
//
//     metadata.read_version = read_version;
//     metadata.write_versions = new_versions;
//     metadata.overwrite(&self.dir, table_name).await;
//   }
//
//   pub async fn add_write_version(&self, table_name: &str, write_version: u64) {
//     let mut mux_guard = self.data.write().await;
//     let map = &mut *mux_guard;
//     if !map.contains_key(table_name) {
//       map.insert(String::from(table_name), FlushMetadata::read_from_disk(&self.dir, table_name).await);
//     }
//     let metadata = map.get_mut(table_name).unwrap();
//
//     metadata.write_versions.push(write_version);
//     metadata.overwrite(&self.dir, table_name).await;
//   }
// }
//
// #[derive(Serialize, Deserialize)]
// pub struct Compaction {
//   pub copied_n: usize,
// }
