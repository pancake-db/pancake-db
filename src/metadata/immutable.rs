use uuid::Uuid;
use rand::Rng;

use crate::impl_metadata_serde_json;
use crate::opt::Opt;
use crate::errors::{ServerResult, ServerError};
use crate::constants::{MAJOR_VERSION, MINOR_VERSION};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ImmutableMetadata {
  pub cluster_name: String,
  pub major_version: u32,
  pub node_id: u64,
  pub prod: bool,
}

impl_metadata_serde_json!(ImmutableMetadata);

impl ImmutableMetadata {
  pub fn try_generate_prod(opts: &Opt) -> ServerResult<Self> {
    let cluster_name = opts.cluster_name
      .ok_or_else(|| Err(ServerError::invalid(
        "cluster name argument is required to initialize server"
      )))?;
    let mut rng = rand::thread_rng();
    let node_id = rng.gen();

    Ok(ImmutableMetadata {
      cluster_name,
      major_version: MAJOR_VERSION,
      node_id,
      prod: true,
    })
  }

  // checks agreement between server code, opts, and existing immutable
  // metadata
  pub fn validate(&self, opts: &Opt) -> ServerResult<()> {
    if !self.prod {
      return Err(ServerError::invalid("non-prod immutable metadata found"));
    }

    if let Some(opt_cluster_name) = &opts.cluster_name {
      if *opt_cluster_name != self.cluster_name {
        return Err(ServerError::invalid(format!(
          "cluster name argument {} does not match immutable cluster name {}",
          opt_cluster_name,
          self.cluster_name,
        )));
      }
    }

    if MAJOR_VERSION != self.major_version {
      return Err(ServerError::invalid(format!(
        "server code major version {} does not match immutable major version {}",
        MAJOR_VERSION,
        self.major_version,
      )));
    }

    Ok(())
  }
}
