use aws_sdk_s3::types::ByteStream;

use crate::{Opt, Server, ServerResult};
use crate::errors::ServerError;
use crate::metadata::segment::VersionCoords;
use crate::opt::ObjectStore;
use crate::types::CompactionKey;
use crate::utils::{common, dirs};
use crate::utils::dirs::compact_col_filename;

pub fn version_coords(
  opts: &Opt,
  compaction_key: &CompactionKey,
  warrants_object_storage: bool,
) -> VersionCoords {
  let cloud_opts = &opts.cloud_opts;
  match (cloud_opts.object_store, warrants_object_storage) {
    (ObjectStore::Local, _) => VersionCoords::Local {
      path: dirs::version_dir(&opts.dir, compaction_key)
    },
    (_, false) => VersionCoords::Local {
      path: dirs::version_dir(&opts.dir, compaction_key)
    },
    (ObjectStore::S3, true) => {
      VersionCoords::S3 {
        endpoint: cloud_opts.s3_endpoint.clone(),
        bucket: cloud_opts.s3_bucket.clone().unwrap(),
        key: version_s3_key(compaction_key),
      }
    }
  }
}

pub fn version_s3_key(compaction_key: &CompactionKey) -> String {
  format!(
    "cold/{}/{}/s_{}/v_{}",
    &compaction_key.table_name,
    &compaction_key.partition,
    &compaction_key.segment_id,
    &compaction_key.version,
  )
}

pub async fn write_col(
  server: &Server,
  bytes: &[u8],
  coords: &VersionCoords,
  col_name: &str,
) -> ServerResult<()> {
  match coords {
    VersionCoords::Local { path } => {
      common::append_to_file(
        &path.join(compact_col_filename(col_name)),
        bytes,
      ).await?;
      Ok(())
    }
    VersionCoords::S3 { endpoint, bucket, key } => {
      let col_key = format!(
        "{}/c_{}",
        &key,
        &col_name
      );

      let client = server.s3_client(endpoint).await?;

      client.put_object()
        .bucket(bucket.clone())
        .key(col_key)
        .body(ByteStream::from(bytes.to_vec()))
        .send()
        .await
        .map_err(|e| ServerError::internal(format!(
          "put object failed: {:?}",
          e,
        )))?;

      Ok(())
    }
  }
}