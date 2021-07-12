use structopt::StructOpt;
use std::path::PathBuf;

#[derive(Debug, StructOpt)]
#[structopt(name = "PancakeDB Server")]
pub struct Opt {
  #[structopt(long)]
  pub dir: PathBuf,
  #[structopt(long, default_value = "1337")]
  pub port: u16
}