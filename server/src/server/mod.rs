use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::Future;
use pancake_db_idl::dml::Row;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, Instant};
use warp::{Filter, Rejection, Reply};

use crate::storage::compaction::CompactionCache;
use crate::storage::flush::FlushMetadataCache;
use crate::storage::schema::SchemaCache;
use crate::storage::segments::SegmentsMetadataCache;
use crate::types::{PartitionKey, SegmentKey};

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
    !state.inactive
  }
}

#[derive(Default, Clone)]
pub struct Staged {
  mutex: Arc<Mutex<StagedState>>
}

#[derive(Default)]
pub struct StagedState {
  rows: HashMap<PartitionKey, Vec<Row>>,
  compaction_candidates: HashSet<SegmentKey>,
}

impl Staged {
  pub async fn add_rows(&self, table_partition: PartitionKey, rows: &[Row]) {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    state.rows.entry(table_partition)
      .or_insert_with(Vec::new)
      .extend_from_slice(rows);
  }

  pub async fn pop_rows_for_flush(
    &self,
    table_partition: &PartitionKey,
    segment_id: &str
  ) -> Option<Vec<Row>> {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    state.compaction_candidates.insert(table_partition.segment_key(segment_id.to_string()));
    state.rows.remove(table_partition)
  }

  pub async fn get_table_partitions(&self) -> Vec<PartitionKey> {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    state.rows.keys().cloned().collect()
  }

  pub async fn get_compaction_candidates(&self) -> HashSet<SegmentKey> {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    let res = state.compaction_candidates.clone();
    state.compaction_candidates = HashSet::new();
    res
  }
}

#[derive(Clone)]
pub struct Server {
  pub dir: PathBuf,
  staged: Staged,
  activity: Activity,
  schema_cache: SchemaCache,
  segments_metadata_cache: SegmentsMetadataCache,
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
        let table_partitions = flush_clone.staged.get_table_partitions().await;
        for table_partition in &table_partitions {
          match self.flush(table_partition).await {
            Ok(()) => (),
            Err(e) => println!("oh no why did flush fail {}", e),
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
        let compaction_candidates = compact_clone.staged.get_compaction_candidates().await;
        for segment_key in &compaction_candidates {
          match self.compact_if_needed(segment_key).await {
            Ok(()) => (),
            Err(e) => println!("oh no why did compact fail {}", e),
          }
        }

        let is_active = self.activity.is_active().await;
        if !is_active {
          return;
        }
      }
    };

    (flush_forever_future, compact_forever_future)
  }

  pub async fn stop(&self) {
    self.activity.stop().await;
  }

  pub fn new(dir: &Path) -> Server {
    let schema_cache = SchemaCache::new(&dir);
    let segments_metadata_cache = SegmentsMetadataCache::new(&dir);
    let flush_metadata_cache = FlushMetadataCache::new(&dir);
    let compaction_cache = CompactionCache::new(&dir);
    Server {
      dir: dir.to_path_buf(),
      schema_cache,
      segments_metadata_cache,
      flush_metadata_cache,
      compaction_cache,
      staged: Staged::default(),
      activity: Activity::default(),
    }
  }

  pub fn warp_filter(&self) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("rest")
      .and(
        Self::create_table_filter()
          .or(Self::write_to_partition_filter())
          .or(Self::read_segment_column_filter())
          .or(Self::list_segments_filter())
      )
      // .recover(errors::warp_recover)
  }
}
