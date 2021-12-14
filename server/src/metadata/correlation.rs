use crate::metadata::traits::{EphemeralCacheData, EphemeralMetadata};
use crate::types::{CompactionKey, SegmentKey};
use crate::errors::{ServerResult, ServerError};

#[derive(Clone)]
pub struct CorrelationMetadata {
  pub compaction_key: CompactionKey,
}

impl EphemeralMetadata for CorrelationMetadata {}

pub type CorrelationMetadataCache = EphemeralCacheData<String, CorrelationMetadata>;

impl CorrelationMetadataCache {
  pub async fn get_correlated_read_version(
    &self,
    correlation_id: &String,
    segment_key: &SegmentKey,
    version: u64,
  ) -> ServerResult<u64> {
    let lock = self.get_lock(correlation_id).await?;
    let mut guard = lock.write().await;
    let compaction_key = segment_key.compaction_key(version);
    if guard.is_some() {
      let meta = guard.clone().unwrap();
      let prev_segment_key = meta.compaction_key.segment_key();
      let this_segment_key = compaction_key.segment_key();
      if prev_segment_key == this_segment_key {
        Ok(meta.compaction_key.version)
      } else {
        Err(ServerError::invalid(&format!(
          "correlation id {} originally used for segment {} now used for segment {}",
          correlation_id,
          prev_segment_key,
          this_segment_key,
        )))
      }
    } else {
      *guard = Some(CorrelationMetadata {
        compaction_key,
      });
      Ok(version)
    }
  }
}
