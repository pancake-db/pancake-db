use pancake_db_client::errors::ClientResult;
use structopt::StructOpt;
use std::net::IpAddr;
use pancake_db_client::Client;
use rand::rngs::ThreadRng;
use rand::Rng;
use pancake_db_idl::dml::{partition_field_value, field_value, FieldValue, RepeatedFieldValue};
use pancake_db_idl::dml::{WriteToPartitionRequest, Row, PartitionFieldValue};
use tokio::time::{Instant, Duration};
use std::collections::HashMap;
use protobuf::well_known_types::Timestamp;
use pancake_db_idl::ddl::CreateTableRequest;
use pancake_db_idl::ddl::create_table_request::SchemaMode;
use protobuf::{ProtobufEnumOrUnknown, MessageField};
use pancake_db_idl::schema::{Schema, PartitionMeta, ColumnMeta};
use pancake_db_idl::partition_dtype::PartitionDataType;
use pancake_db_idl::dtype::DataType;

const TABLE_NAME: &str = "publisher_test";
const PERFORMANCE_TABLE_NAME: &str = "publisher_performance";

#[derive(Clone, Debug, StructOpt)]
struct Opt {
  #[structopt(long, default_value="127.0.0.1")]
  host: IpAddr,
  #[structopt(long, default_value="3841")]
  port: u16,
  #[structopt(long, default_value="5")]
  max_concurrency: usize,
  #[structopt(long, default_value="30.0")]
  target_rows_per_second: f32,
}

fn generate_row(rng: &mut ThreadRng, words: &[String], timestamp: Timestamp) -> Row {
  let mut row = Row::new();
  fn maybe_insert(rng: &mut ThreadRng, row: &mut Row, name: &str, value: field_value::Value) {
    if rng.gen_bool(0.5) {
      row.fields.insert(name.to_string(), FieldValue {
        value: Some(value),
        ..Default::default()
      });
    }
  }
  let b = rng.gen_bool(0.001);
  maybe_insert(rng, &mut row, "bool_col", field_value::Value::bool_val(b));
  let byte = rng.gen::<u8>();
  let mut bytes = Vec::new();
  for _ in 0..rng.gen_range(0..20) {
    bytes.push(byte);
  }
  maybe_insert(rng, &mut row, "bytes_col", field_value::Value::bytes_val(bytes));
  let i = rng.gen_range(0..101);
  maybe_insert(rng, &mut row, "int_col", field_value::Value::int64_val(i));
  let f = rng.gen_range(1.0..2.0);
  maybe_insert(rng, &mut row, "float_col", field_value::Value::float64_val(f));
  let mut list = RepeatedFieldValue::new();
  for _ in 0..rng.gen_range(0..3) {
    let word_idx = rng.gen_range(0..words.len());
    list.vals.push(FieldValue {
      value: Some(field_value::Value::string_val(words[word_idx].to_string())),
      ..Default::default()
    })
  }
  maybe_insert(rng, &mut row, "list_col", field_value::Value::list_val(list));
  maybe_insert(rng, &mut row, "timestamp_col", field_value::Value::timestamp_val(timestamp));
  row
}

fn make_performance_row(duration: Duration, concurrency: usize, errors: usize) -> Row {
  let mut row = Row::new();
  row.fields.insert("response_time".to_string(), FieldValue {
    value: Some(field_value::Value::float32_val(duration.as_secs_f32())),
    ..Default::default()
  });
  row.fields.insert("write_start_at".to_string(), FieldValue {
    value: Some(field_value::Value::timestamp_val(Timestamp::now())),
    ..Default::default()
  });
  row.fields.insert("concurrency".to_string(), FieldValue {
    value: Some(field_value::Value::int64_val(concurrency as i64)),
    ..Default::default()
  });
  row.fields.insert("errors".to_string(), FieldValue {
    value: Some(field_value::Value::int64_val(errors as i64)),
    ..Default::default()
  });
  row
}

fn make_schema() -> Schema {
  let mut schema = Schema::new();
  schema.partitioning.insert("time_bucket".to_string(), PartitionMeta {
    dtype: ProtobufEnumOrUnknown::new(PartitionDataType::TIMESTAMP_MINUTE),
    ..Default::default()
  });
  schema.columns.insert("bool_col".to_string(), ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::BOOL),
    ..Default::default()
  });
  schema.columns.insert("bytes_col".to_string(), ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::BYTES),
    ..Default::default()
  });
  schema.columns.insert("float_col".to_string(), ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::FLOAT64),
    ..Default::default()
  });
  schema.columns.insert("int_col".to_string(), ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
    ..Default::default()
  });
  schema.columns.insert("list_col".to_string(), ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::STRING),
    nested_list_depth: 1,
    ..Default::default()
  });
  schema.columns.insert("timestamp_col".to_string(), ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::TIMESTAMP_MICROS),
    ..Default::default()
  });
  schema
}

fn make_performance_schema() -> Schema {
  let mut schema = Schema::new();
  schema.partitioning.insert("action".to_string(), PartitionMeta {
    dtype: ProtobufEnumOrUnknown::new(PartitionDataType::STRING),
    ..Default::default()
  });
  schema.columns.insert("response_time".to_string(), ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::FLOAT32),
    ..Default::default()
  });
  schema.columns.insert("concurrency".to_string(), ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
    ..Default::default()
  });
  schema.columns.insert("errors".to_string(), ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
    ..Default::default()
  });
  schema.columns.insert("write_start_at".to_string(), ColumnMeta {
    dtype: ProtobufEnumOrUnknown::new(DataType::TIMESTAMP_MICROS),
    ..Default::default()
  });
  schema
}

fn truncate_to_time_bucket(t: Timestamp) -> Timestamp {
  let time_bucketing = 5 * 86400; // 5 days
  let truncated_seconds = (t.seconds % time_bucketing) * time_bucketing;
  Timestamp {
    seconds: truncated_seconds,
    ..Default::default()
  }
}

#[tokio::main]
async fn main() -> ClientResult<()> {
  let opt: Opt = Opt::from_args();
  let client = Client::from_ip_port(opt.host, opt.port);
  let mut rng = rand::thread_rng();

  let delay_seconds = (opt.max_concurrency as f32 + 1.0) /
    (2.0 * opt.target_rows_per_second);
  let delay = tokio::time::Duration::from_secs_f32(delay_seconds);

  let mut performance_partition = HashMap::new();
  performance_partition.insert("action".to_string(), PartitionFieldValue {
    value: Some(partition_field_value::Value::string_val("write".to_string())),
    ..Default::default()
  });

  let create_req = CreateTableRequest {
    table_name: TABLE_NAME.to_string(),
    schema: MessageField::some(make_schema()),
    mode: ProtobufEnumOrUnknown::new(SchemaMode::ADD_NEW_COLUMNS),
    ..Default::default()
  };
  client.api_create_table(&create_req).await?;
  let performance_create_req = CreateTableRequest {
    table_name: PERFORMANCE_TABLE_NAME.to_string(),
    schema: MessageField::some(make_performance_schema()),
    mode: ProtobufEnumOrUnknown::new(SchemaMode::ADD_NEW_COLUMNS),
    ..Default::default()
  };
  client.api_create_table(&performance_create_req).await?;
  let words: Vec<_> = String::from_utf8(std::fs::read("/usr/share/dict/words").unwrap()).unwrap()
    .split("\n")
    .map(|s| s.to_string())
    .collect();

  let mut iter = 0;
  let mut write_start_at = Instant::now();
  loop {
    let mut write_reqs = Vec::new();
    let concurrency = 1 + rng.gen_range(0..opt.max_concurrency);
    let sleep_until = write_start_at + delay;
    let current_instant = Instant::now();
    if sleep_until < current_instant {
      println!(
        "publisher is lagging; iter={}, current time={:?}, planned write time={:?}",
        iter,
        current_instant,
        sleep_until,
      );
    }
    tokio::time::sleep_until(sleep_until).await;
    write_start_at = Instant::now();
    let timestamp = Timestamp::now();
    let mut partition = HashMap::new();
    partition.insert("time_bucket".to_string(), PartitionFieldValue {
      value: Some(partition_field_value::Value::timestamp_val(truncate_to_time_bucket(timestamp.clone()))),
      ..Default::default()
    });
    for _ in 0..concurrency {
      let row = generate_row(&mut rng, &words, timestamp.clone());
      let write_req = WriteToPartitionRequest {
        table_name: TABLE_NAME.to_string(),
        partition: partition.clone(),
        rows: vec![row],
        ..Default::default()
      };
      write_reqs.push(write_req);
    }
    let mut write_futures = Vec::new();
    for i in 0..concurrency {
      write_futures.push(client.api_write_to_partition(&write_reqs[i]));
    }
    let write_results = futures::future::join_all(write_futures).await;
    let errors = write_results.iter()
      .filter(|r| r.is_err())
      .map(|r| {
        println!("error while writing: {}", r.as_ref().unwrap_err());
        r
      })
      .count();

    let duration = Instant::now() - write_start_at;
    let performance_row = make_performance_row(duration, concurrency, errors);
    let performance_write_req = WriteToPartitionRequest {
      table_name: PERFORMANCE_TABLE_NAME.to_string(),
      partition: performance_partition.clone(),
      rows: vec![performance_row],
      ..Default::default()
    };
    match client.api_write_to_partition(&performance_write_req).await {
      Ok(_) => (),
      Err(e) => {
        println!("error while reporting performance: {}", e);
      }
    }
    iter += 1;
  }
}