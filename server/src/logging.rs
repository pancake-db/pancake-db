use log::{Log, Metadata, Record};
use chrono::{Utc, SecondsFormat};

pub struct Logger;

impl Log for Logger {
  fn enabled(&self, _: &Metadata) -> bool {
    true
  }

  fn log(&self, record: &Record) {
    println!(
      "{} {}| {}",
      Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true),
      record.level(),
      record.args()
    );
  }

  fn flush(&self) {}
}