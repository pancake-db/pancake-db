use structopt::StructOpt;
use std::path::PathBuf;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "PancakeDB Server")]
pub struct Opt {
  #[structopt(long)]
  pub dir: PathBuf,
  #[structopt(long, default_value = "1337")]
  pub port: u16,
  #[structopt(long, default_value = "1000000")]
  pub default_rows_per_segment: usize,
  #[structopt(long, default_value = "10000")]
  pub min_rows_for_compaction: usize,
  #[structopt(long, default_value = "10")]
  pub compaction_loop_seconds: u64,
  #[structopt(long, default_value = "7200")]
  pub delete_stale_compaction_seconds: i64,
}