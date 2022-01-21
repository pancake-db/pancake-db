use crate::constants::{MAJOR_VERSION, MINOR_VERSION};
use crate::errors::{ServerError, ServerResult};
use crate::metadata::global::GlobalMetadata;
use crate::metadata::PersistentMetadata;
use crate::server::Server;
use crate::types::EmptyKey;

impl Server {
  pub async fn upgrade_to_minor_version(&self) -> ServerResult<()> {
    let global_guard = self.global_metadata_lock.write().await;
    if global_guard.upgrade_in_progress_minor_version > MINOR_VERSION {
      return Err(ServerError::invalid(format!(
        "server was on {}.{}, but server binary is only {}.{}",
        MAJOR_VERSION,
        global_guard.upgrade_in_progress_minor_version,
        MAJOR_VERSION,
        MINOR_VERSION,
      )))
    }

    for minor_version in global_guard.minor_version..MINOR_VERSION {
      let upgrade_version = minor_version + 1;
      global_guard.upgrade_in_progress_minor_version = upgrade_version;
      global_guard.overwrite(&self.opts.dir, &EmptyKey).await?;
      log::info!("upgrading minor version to {}", upgrade_version);
      self.increment_minor_version_to(upgrade_version, &mut *global_guard).await?;
      global_guard.minor_version = upgrade_version;
      global_guard.overwrite(&self.opts.dir, &EmptyKey).await?;
    }
    Ok(())
  }

  async fn increment_minor_version_to(
    &self,
    version: u32,
    _global_meta: &mut GlobalMetadata
  ) -> ServerResult<()> {
    log::info!("no changes needed for minor version upgrade {}", version);
    Ok(())
  }
}