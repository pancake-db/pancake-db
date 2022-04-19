use std::path::PathBuf;
use std::str::FromStr;

use log::LevelFilter;
use structopt::StructOpt;

use crate::errors::ServerError;
use crate::ServerResult;

const MIN_DIR_LEN: usize = 5;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "PancakeDB Server")]
pub struct Opt {
  // where state will be persisted on disk
  #[structopt(long)]
  pub dir: PathBuf,

  #[structopt(long, default_value = "3841")]
  pub http_port: u16,

  #[structopt(long, default_value = "3842")]
  pub grpc_port: u16,

  #[structopt(long, default_value = "INFO")]
  pub log_level: LevelFilter,

  #[structopt(flatten)]
  pub cloud_opts: CloudOpt,

  // Segments should complete shortly after reaching either the target
  // number of rows or the target uncompressed size (whichever comes first).
  #[structopt(long, default_value = "5000000")]
  pub target_rows_per_segment: u32,

  #[structopt(long, default_value = "130000000")] // just under 128MB
  pub target_uncompressed_bytes_per_segment: u64,

  // the fewest number of rows in a segment before compaction
  // will be considered
  #[structopt(long, default_value = "30000")]
  pub min_rows_for_compaction: u32,

  // how often the background loop will check each partition and
  // see if it needs compaction
  #[structopt(long, default_value = "10")]
  pub compaction_loop_seconds: u64,

  // how obsolete a (previous read version) compaction must
  // be before we delete it
  // If this is too short, we might delete data from ongoing reads.
  #[structopt(long, default_value = "7200")]
  pub delete_stale_compaction_seconds: i64,

  // the minimum time to wait since the last compaction of a segment
  // before compacting again
  #[structopt(long, default_value = "60")]
  pub min_compaction_intermission_seconds: i64,

  // after this duration of no writes to a segment, compact again
  // so that there are no uncompressed files
  #[structopt(long, default_value = "1800")]
  pub compact_as_constant_seconds: i64,

  #[structopt(long, default_value = "2097152")]
  pub read_page_byte_size: usize,
}

#[derive(Clone, Copy, Debug, StructOpt)]
pub enum CloudProvider {
  None,
  Aws,
}

#[derive(Clone, Debug, StructOpt)]
pub struct CloudOpt {
  #[structopt(long, default_value = "NONE")]
  pub cloud_provider: CloudProvider,

  #[structopt(long)]
  pub aws_s3_bucket: Option<String>,
}

impl FromStr for CloudProvider {
  type Err = ServerError;

  fn from_str(s: &str) -> ServerResult<Self> {
    match s.to_lowercase().as_str() {
      "none" => Ok(CloudProvider::None),
      "aws" => Ok(CloudProvider::Aws),
      invalid => Err(ServerError::invalid(format!(
        "invalid cloud provider {}",
        invalid,
      ))),
    }
  }
}

impl Opt {
  pub fn validate(&self) {
    let dir_maybe_str = self.dir
      .canonicalize()
      .expect("unable to canonicalize dir - make sure it exists");
    let dir_str = dir_maybe_str
      .to_str()
      .expect("dir was not a valid string");
    if dir_str.len() < MIN_DIR_LEN {
      panic!("suspiciously short length for dir; please choose a more specific path")
    }
  }
}
