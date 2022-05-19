use std::collections::HashSet;
use std::sync::Arc;
use aws_sdk_s3::Client;

use futures::{Future, pin_mut};
use futures::StreamExt;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, Instant};

use crate::errors::ServerResult;
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
use crate::types::{EmptyKey, SegmentKey};
use crate::utils::common;

mod read;
mod recovery;
mod misc;
mod grpc;
mod s3;

const FLUSH_SECONDS: u64 = 10;

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
}

#[derive(Clone)]
pub struct Server {
  pub opts: Opt,
  background: Background,
  activity: Activity,
  s3_client: Arc<RwLock<Option<Client>>>,
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

    let flush_forever_future = async move {
      let mut last_t = Instant::now();
      let flush_interval = Duration::from_secs(FLUSH_SECONDS);
      loop {
        let cur_t = Instant::now();
        let planned_t = last_t + flush_interval;
        if cur_t < planned_t {
          tokio::time::sleep_until(planned_t).await;
        }
        last_t = cur_t;
        let candidates = self.background.pop_flush_candidates().await;
        for candidate in &candidates {
          let flush_result = FlushOp { segment_key: candidate.clone() }
            .execute(self)
            .await;
          if let Err(err) = flush_result {
            log::error!("flushing {} failed: {}", candidate, err);
          }
        }

        let is_active = self.activity.is_active().await;
        if !is_active {
          return;
        }
      }
    };

    let compact_forever_future = async move {
      let mut last_t = Instant::now();
      let compact_interval = Duration::from_secs(self.opts.compaction_loop_seconds);
      loop {
        let cur_t = Instant::now();
        let planned_t = last_t + compact_interval;
        if cur_t < planned_t {
          tokio::time::sleep_until(planned_t).await;
        }
        last_t = cur_t;
        let segment_key_stream = self.stream_all_segment_keys();
        pin_mut!(segment_key_stream);
        while let Some(segment_key_result) = segment_key_stream.next().await {
          // The CompactionOp uses heuristics to determine whether a compaction
          // is needed, so we don't do any of those heuristics here.
          match segment_key_result {
            Ok(segment_key) => {
              let compact_result = CompactionOp { key: segment_key.clone() }.execute(self).await;
              if let Err(e) = compact_result {
                log::error!("compaction failed: {}", e);
              }
            },
            Err(e) => {
              log::error!("compaction loop failed: {}", e);
            },
          }
        }

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
      s3_client: Arc::new(RwLock::new(None)),
    }
  }

  pub async fn add_flush_candidate(&self, key: SegmentKey) {
    self.background.add_flush_candidate(key).await;
  }
}
