use std::collections::HashSet;
use std::sync::Arc;

use futures::Future;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, Instant};
use warp::{Filter, Rejection, Reply};

use crate::storage::compaction::CompactionCache;
use crate::storage::segment::SegmentMetadataCache;
use crate::storage::schema::SchemaCache;
use crate::storage::partition::PartitionMetadataCache;
use crate::types::SegmentKey;
use crate::opt::Opt;
use crate::errors::ServerError;
use crate::storage::staged::StagedMetadataCache;
use crate::locks::segment::SegmentLockKey;
use crate::ops::compact::CompactionOp;
use crate::ops::traits::ServerOp;

mod compact;
mod create_table;
mod delete;
mod get_schema;
mod read;
mod write;

const FLUSH_SECONDS: u64 = 1;
const FLUSH_NANOS: u32 = 0;

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
  // rows: HashMap<PartitionKey, Vec<Row>>,
  flush_candidates: HashSet<SegmentLockKey>,
  compaction_candidates: HashSet<SegmentKey>,
}

impl Staged {
  pub async fn add_flush_candidate(&self, key: SegmentLockKey) {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    state.flush_candidates.insert(key);
  }

  // pub async fn pop_rows_for_flush(
  //   &self,
  //   table_partition: &PartitionKey,
  //   segment_id: &str
  // ) -> Option<Vec<Row>> {
  //   let mut mux_guard = self.mutex.lock().await;
  //   let state = &mut *mux_guard;
  //   state.compaction_candidates.insert(table_partition.segment_key(segment_id.to_string()));
  //   state.rows.remove(table_partition)
  // }

  pub async fn pop_flush_candidates(&self) -> HashSet<SegmentLockKey> {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    let res = state.flush_candidates.clone();
    state.flush_candidates = HashSet::new();
    res
  }

  pub async fn get_compaction_candidates(&self) -> HashSet<SegmentKey> {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    state.compaction_candidates.clone()
  }

  pub async fn remove_compaction_candidates(&self, keys: &[SegmentKey]) {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    for key in keys {
      state.compaction_candidates.remove(key);
    }
  }
}

#[derive(Clone)]
pub struct Server {
  pub opts: Opt,
  staged: Staged,
  activity: Activity,
  pub schema_cache: SchemaCache,
  pub partition_metadata_cache: PartitionMetadataCache,
  pub segment_metadata_cache: SegmentMetadataCache,
  pub compaction_cache: CompactionCache,
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
        let candidates = flush_clone.staged.pop_flush_candidates().await;
        for candidate in &candidates {
          match self.flush(candidate.to_segment_lock_key()).await {
            Ok(()) => (),
            Err(e) => {
              log::error!("flushing {} failed: {}", candidate, e)
            },
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
      let compact_interval = Duration::new(self.opts.compaction_loop_seconds, 0);
      loop {
        let cur_t = Instant::now();
        let planned_t = last_t + compact_interval;
        if cur_t < planned_t {
          tokio::time::sleep_until(planned_t).await;
        }
        last_t = cur_t;
        let compaction_candidates = compact_clone.staged
          .get_compaction_candidates()
          .await;
        let mut segment_keys_to_remove = Vec::new();
        for segment_key in &compaction_candidates {
          let remove = CompactionOp { key: segment_key.clone() }.execute(&self).await
            .unwrap_or_else(|e| {
              log::error!("compacting {} failed: {}", segment_key, e);
              false
            });
          if remove {
            segment_keys_to_remove.push(segment_key.clone());
          }
        }
        compact_clone.staged
          .remove_compaction_candidates(&segment_keys_to_remove)
          .await;

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

  pub fn new(opts: Opt) -> Server {
    let dir = &opts.dir;
    let schema_cache = SchemaCache::new(dir);
    let partition_metadata_cache = PartitionMetadataCache::new(dir);
    let segment_metadata_cache = SegmentMetadataCache::new(dir);
    let compaction_cache = CompactionCache::new(dir);
    Server {
      opts,
      schema_cache,
      partition_metadata_cache,
      segment_metadata_cache,
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
          .or(Self::get_schema_filter())
          .or(Self::drop_table_filter())
      )
  }

  pub async fn add_flush_candidate(&self, key: SegmentLockKey) {
    self.staged.add_flush_candidate(key).await;
  }
}
