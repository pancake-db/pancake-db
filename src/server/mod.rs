use std::collections::HashMap;
use std::sync::Arc;

use futures::Future;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, Instant};

use crate::storage::flush::FlushMetadataCache;
use crate::storage::schema::SchemaCache;
use crate::storage::compaction::CompactionCache;
use pancake_db_idl::dml::Row;
use warp::{Filter, Rejection, Reply};

mod create_table;
mod write;
mod read;
mod compact;

const FLUSH_SECONDS: u64 = 1;
const FLUSH_NANOS: u32 = 0;
const COMPACT_SECONDS: u64 = 2;

#[derive(Default, Clone)]
pub struct Activity {
  lock: Arc<RwLock<ActivityState>>
}

#[derive(Default, Clone)]
pub struct ActivityState {
  inactive: bool,
}

impl Activity {
  pub async fn stop(&self) {
    let mut mux_guard = self.lock.write().await;
    let state = &mut *mux_guard;
    state.inactive = true;
  }

  pub async fn is_active(&self) -> bool {
    let mux_guard = self.lock.read().await;
    let state = &*mux_guard;
    return !state.inactive;
  }
}

#[derive(Default, Clone)]
pub struct Staged {
  mutex: Arc<Mutex<StagedState>>
}

#[derive(Default)]
pub struct StagedState {
  rows: HashMap<String, Vec<Row>>,
  // tables: Vec<String>
}

impl Staged {
  pub async fn add_rows(&self, table_name: &str, rows: &[Row]) {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    state.rows.entry(table_name.to_owned())
      .or_insert(Vec::new())
      .extend_from_slice(rows);
  }

  pub async fn pop_rows(&self, table_name: &str) -> Option<Vec<Row>> {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    return state.rows.remove(table_name);
  }

  pub async fn get_tables(&self) -> Vec<String> {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    return state.rows.keys().map(|x| x.clone()).collect();
  }
}

#[derive(Clone)]
pub struct Server {
  pub dir: String,
  staged: Staged,
  activity: Activity,
  schema_cache: SchemaCache,
  flush_metadata_cache: FlushMetadataCache,
  compaction_cache: CompactionCache,
}

impl Server {
  pub async fn init(&self) -> (impl Future<Output=()> + '_, impl Future<Output=()> + '_) {
    let flush_clone = self.clone();
    let flush_forever_future = async move {
      let mut last_t = Instant::now();
      let flush_interval = Duration::new(FLUSH_SECONDS, FLUSH_NANOS);
      loop {
        let cur_t = Instant::now();
        let planned_t = last_t + flush_interval;
        if cur_t < planned_t {
          tokio::time::sleep_until(planned_t).await;
        }
        last_t = cur_t;
        let tables = flush_clone.staged.get_tables().await;
        for table in &tables {
          match self.flush(table).await {
            Ok(()) => (),
            Err(_) => println!("oh no why did flush fail"),
          }
        }

        let is_active = self.activity.is_active().await;
        if !is_active {
          return;
        }
      }
    };

    let compact_clone = self.clone();
    let compact_forever_future = async move {
      let mut last_t = Instant::now();
      let compact_interval = Duration::new(COMPACT_SECONDS, 0);
      loop {
        let cur_t = Instant::now();
        let planned_t = last_t + compact_interval;
        if cur_t < planned_t {
          tokio::time::sleep_until(planned_t).await;
        }
        last_t = cur_t;
        let tables = compact_clone.schema_cache.get_all_table_names().await;
        for table in &tables {
          match self.compact_if_needed(table).await {
            Ok(()) => (),
            Err(_) => println!("oh no why did compact fail"),
          }
        }

        let is_active = self.activity.is_active().await;
        if !is_active {
          return;
        }
      }
    };

    return (flush_forever_future, compact_forever_future);
  }

  pub async fn stop(&self) {
    self.activity.stop().await;
  }

  pub fn new(dir: String) -> Server {
    let flush_metadata_cache = FlushMetadataCache::new(&dir);
    let schema_cache = SchemaCache::new(&dir);
    let compaction_cache = CompactionCache::new(&dir);
    let res = Server {
      dir,
      schema_cache,
      flush_metadata_cache,
      compaction_cache,
      staged: Staged::default(),
      activity: Activity::default(),
    };
    return res;
  }

  pub fn warp_filter(&self) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    Self::create_table_filter()
      .or(Self::write_to_partition_filter())
      .or(Self::read_segment_column_filter())
  }
}
