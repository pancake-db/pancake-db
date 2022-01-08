use std::collections::HashSet;
use std::sync::Arc;

use futures::Future;
use hyper::body::Bytes;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, Instant};
use warp::{Filter, Rejection, Reply};

use crate::errors::{ServerErrorKind, ServerResult};
use crate::metadata::compaction::CompactionCache;
use crate::metadata::correlation::CorrelationMetadataCache;
use crate::metadata::deletion::DeletionMetadataCache;
use crate::metadata::global::GlobalMetadata;
use crate::metadata::partition::PartitionMetadataCache;
use crate::metadata::PersistentMetadata;
use crate::metadata::segment::SegmentMetadataCache;
use crate::metadata::table::TableMetadataCache;
use crate::ops::compact::CompactionOp;
use crate::ops::flush::FlushOp;
use crate::ops::traits::ServerOp;
use crate::opt::Opt;
use crate::types::{SegmentKey, EmptyKey};
use crate::utils::common;

mod create_table;
mod delete;
mod get_schema;
mod read;
mod write;
mod write_simple;
mod recovery;
mod alter_table;
mod list_tables;

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
pub struct Background {
  mutex: Arc<Mutex<BackgroundState>>
}

#[derive(Default)]
pub struct BackgroundState {
  flush_candidates: HashSet<SegmentKey>,
  compaction_candidates: HashSet<SegmentKey>,
}

impl Background {
  pub async fn add_flush_candidate(&self, key: SegmentKey) {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    state.flush_candidates.insert(key);
  }

  pub async fn pop_flush_candidates(&self) -> HashSet<SegmentKey> {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    let res = state.flush_candidates.clone();
    state.flush_candidates = HashSet::new();
    res
  }

  pub async fn add_compaction_candidate(&self, segment_key: SegmentKey) {
    let mut mux_guard = self.mutex.lock().await;
    let state = &mut *mux_guard;
    state.compaction_candidates.insert(segment_key);
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
  background: Background,
  activity: Activity,
  pub global_metadata_lock: Arc<RwLock<GlobalMetadata>>,
  pub table_metadata_cache: TableMetadataCache,
  pub partition_metadata_cache: PartitionMetadataCache,
  pub deletion_metadata_cache: DeletionMetadataCache,
  pub correlation_metadata_cache: CorrelationMetadataCache,
  pub segment_metadata_cache: SegmentMetadataCache,
  pub compaction_cache: CompactionCache,
}

impl Server {
  async fn bootstrap(&self) -> ServerResult<()> {
    let maybe_existing_global = GlobalMetadata::load(&self.opts.dir, &EmptyKey).await?;
    if let Some(existing_global) = maybe_existing_global {
      let mut global_meta_guard = self.global_metadata_lock.write().await;
      existing_global.overwrite(&self.opts.dir, &EmptyKey).await?;
      *global_meta_guard = existing_global;
    }
    Ok(())
  }

  pub async fn init(&self) -> ServerResult<(impl Future<Output=()> + '_, impl Future<Output=()> + '_)> {
    self.bootstrap().await?;

    common::create_if_new(self.opts.dir.join("tmp")).await?;

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
        let candidates = flush_clone.background.pop_flush_candidates().await;
        for candidate in &candidates {
          let flush_result = FlushOp { segment_key: candidate.clone() }
            .execute(self)
            .await;
          if let Err(err) = flush_result {
            let remove = if matches!(err.kind, ServerErrorKind::Internal) {
              self.background.add_compaction_candidate(candidate.clone()).await;
              false
            } else {
              true
            };
            log::error!("flushing {} failed (will give up? {}): {}", candidate, remove, err);
          } else {
            self.background.add_compaction_candidate(candidate.clone()).await;
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
        let compaction_candidates = compact_clone.background
          .get_compaction_candidates()
          .await;
        let mut segment_keys_to_remove = Vec::new();
        for segment_key in &compaction_candidates {
          let remove = CompactionOp { key: segment_key.clone() }.execute(self).await
            .unwrap_or_else(|e| {
              let remove = !matches!(e.kind, ServerErrorKind::Internal);
              log::error!("compacting {} failed (will give up? {}): {}", segment_key, remove, e);
              remove
            });
          if remove {
            segment_keys_to_remove.push(segment_key.clone());
          }
        }
        compact_clone.background
          .remove_compaction_candidates(&segment_keys_to_remove)
          .await;

        let is_active = self.activity.is_active().await;
        if !is_active {
          return;
        }
      }
    };

    Ok((flush_forever_future, compact_forever_future))
  }

  pub async fn stop(&self) {
    self.activity.stop().await;
  }

  pub fn new(opts: Opt) -> Server {
    let dir = &opts.dir;
    let global_metadata_lock = Arc::new(RwLock::new(GlobalMetadata::default()));
    let table_metadata_cache = TableMetadataCache::new(dir);
    let partition_metadata_cache = PartitionMetadataCache::new(dir);
    let deletion_metadata_cache = DeletionMetadataCache::new();
    let correlation_metadata_cache = CorrelationMetadataCache::new();
    let segment_metadata_cache = SegmentMetadataCache::new(dir);
    let compaction_cache = CompactionCache::new(dir);
    Server {
      opts,
      global_metadata_lock,
      table_metadata_cache,
      partition_metadata_cache,
      deletion_metadata_cache,
      correlation_metadata_cache,
      segment_metadata_cache,
      compaction_cache,
      background: Background::default(),
      activity: Activity::default(),
    }
  }

  fn log_request(route_name: &str, body: &Bytes) {
    log::info!(
      "received REST request for {} containing {} bytes",
      route_name,
      body.len(),
    );
  }

  pub fn warp_filter(&self) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("rest")
      .and(
        Self::create_table_filter()
          .or(Self::alter_table_filter())
          .or(Self::write_to_partition_filter())
          .or(Self::write_to_partition_simple_filter())
          .or(Self::read_segment_column_filter())
          .or(Self::list_segments_filter())
          .or(Self::get_schema_filter())
          .or(Self::list_tables_filter())
          .or(Self::drop_table_filter())
          .or(Self::delete_from_segment_filter())
          .or(Self::read_segment_deletions_filter())
      )
  }

  pub async fn add_flush_candidate(&self, key: SegmentKey) {
    self.background.add_flush_candidate(key).await;
  }
}
