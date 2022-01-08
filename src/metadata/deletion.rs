use serde::{Deserialize, Serialize};

use crate::metadata::traits::{EphemeralCacheData, EphemeralMetadata};
use crate::types::SegmentKey;

#[derive(Serialize, Deserialize, Clone)]
pub struct DeletionMetadata {}

impl EphemeralMetadata for DeletionMetadata {
  const CACHE_SIZE_LIMIT: usize = 16384;
}

pub type DeletionMetadataCache = EphemeralCacheData<SegmentKey, DeletionMetadata>;
