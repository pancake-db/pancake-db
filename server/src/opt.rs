use structopt::StructOpt;
use std::path::PathBuf;
use log::LevelFilter;

const MIN_DIR_LEN: usize = 10;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "PancakeDB Server")]
pub struct Opt {
  // where state will be persisted on disk
  #[structopt(long)]
  pub dir: PathBuf,

  #[structopt(long, default_value = "1337")]
  pub port: u16,

  #[structopt(long, default_value = "INFO")]
  pub log_level: LevelFilter,

  // a target number of rows for each segment of data
  // Segments should complete slightly after this row count
  #[structopt(long, default_value = "1000000")]
  pub default_rows_per_segment: usize,

  // the fewest number of rows in a segment before compaction
  // will be considered
  #[structopt(long, default_value = "10000")]
  pub min_rows_for_compaction: usize,

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

  #[structopt(long, default_value = "1048576")]
  pub read_page_byte_size: usize,
}

impl Opt {
  pub fn validate(&self) {
    let dir_maybe_str = self.dir
      .canonicalize()
      .expect("unable to canonicalize dir");
    let dir_str = dir_maybe_str
      .to_str()
      .expect("dir was not a valid string");
    if dir_str.len() < MIN_DIR_LEN {
      panic!("suspiciously short length for dir; please choose a more specific path")
    }
  }
}