use log::{Level, Log, Metadata, Record};
use chrono::{Utc, SecondsFormat};

pub struct Logger;

impl Log for Logger {
  fn enabled(&self, meta: &Metadata) -> bool {
    meta.level() <= Level::Info
  }

  fn log(&self, record: &Record) {
    if self.enabled(record.metadata()) {
      let time_string = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
      println!(
        "{} {}| {}",
        &time_string[2..],
        record.level(),
        record.args()
      );
    }
  }

  fn flush(&self) {}
}