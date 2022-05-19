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
pub enum ObjectStore {
  Local,
  S3,
}

#[derive(Clone, Debug, StructOpt)]
pub struct CloudOpt {
  #[structopt(long)]
  pub object_store: ObjectStore,

  #[structopt(long)]
  pub s3_bucket: Option<String>,
  #[structopt(long)]
  pub s3_access_key: Option<String>,
  #[structopt(long)]
  pub s3_secret_key: Option<String>,
  #[structopt(long)]
  pub s3_endpoint: Option<String>,
  #[structopt(long)]
  pub s3_region: Option<String>,
}

impl FromStr for ObjectStore {
  type Err = ServerError;

  fn from_str(s: &str) -> ServerResult<Self> {
    match s.to_lowercase().replace(r"[\-_]", "").as_str() {
      "local" => Ok(ObjectStore::Local),
      "s3" => Ok(ObjectStore::S3),
      _ => Err(ServerError::invalid(format!(
        "unsupported cloud provider {}",
        s,
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

    let cloud_opts = &self.cloud_opts;
    match cloud_opts.object_store {
      ObjectStore::Local => (),
      ObjectStore::S3 => {
        if cloud_opts.s3_bucket.is_none() {
          panic!("must specify --s3-bucket when using S3");
        }
        if cloud_opts.s3_access_key.is_none() {
          panic!("must specify --s3-access-key when using S3");
        }
        if cloud_opts.s3_secret_key.is_none() {
          panic!("must specify --s3-secret-key when using S3");
        }
      }
    }
  }

  pub fn from_waterfall() -> Self {
    let maybe_config_path = std::env::var("PANCAKE_CONFIG");
    let conf_arg_strs = match maybe_config_path {
      Ok(config_path) => {
        log::info!("loading some args from {}", config_path);
        let toml_bytes = std::fs::read(config_path)
          .expect("could not read config file");
        let toml_str = String::from_utf8(toml_bytes)
          .expect("non-utf8 config file");
        let config = toml_str.parse::<toml::Value>()
          .expect("config file contains invalid TOML");
        match config {
          toml::Value::Table(table) => {
            table.iter()
              .map(|(key, val)| {
                let val_str = match val {
                  toml::Value::String(s) => s.to_string(),
                  toml::Value::Boolean(b) => b.to_string(),
                  toml::Value::Integer(i) => i.to_string(),
                  toml::Value::Float(f) => f.to_string(),
                  _ => panic!("no nested TOML config values allowed")
                };
                format!(
                  "--{}={}",
                  key,
                  val_str,
                )
              })
              .collect::<Vec<_>>()
          },
          _ => panic!("config file TOML must be a table")
        }
      },
      Err(_) => vec![]
    };
    let cmd_arg_strs: Vec<_> = std::env::args().collect();
    let arg_strs = cmd_arg_strs.iter().chain(&conf_arg_strs).collect::<Vec<_>>();

    Self::from_iter(arg_strs)
  }
}
