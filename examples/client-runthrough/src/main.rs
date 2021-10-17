use std::net::{IpAddr, Ipv4Addr};

use pancake_db_idl::ddl::{CreateTableRequest, DropTableRequest};
use pancake_db_idl::dml::{Field, FieldValue, ListSegmentsRequest, PartitionField, PartitionFilter, RepeatedFieldValue, Row, WriteToPartitionRequest};
use pancake_db_idl::dml::field_value::Value;
use pancake_db_idl::dml::partition_field::Value as PartitionValue;
use pancake_db_idl::dml::partition_filter::Value as PartitionFilterValue;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use pancake_db_idl::schema::{ColumnMeta, PartitionMeta, Schema};
use protobuf::{MessageField, ProtobufEnumOrUnknown};
use tokio;

use pancake_db_client::Client;
use pancake_db_client::errors::ClientResult;

const TABLE_NAME: &str = "t";

#[tokio::main]
async fn main() -> ClientResult<()> {
  let client = Client::from_ip_port(
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
    1337,
  );

  let i_meta = ColumnMeta {
    name: "i".to_string(),
    dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
    ..Default::default()
  };
  let l_meta = ColumnMeta {
    name: "l".to_string(),
    dtype: ProtobufEnumOrUnknown::new(DataType::STRING),
    nested_list_depth: 1,
    ..Default::default()
  };
  let create_table_req = CreateTableRequest {
    table_name: TABLE_NAME.to_string(),
    schema: MessageField::some(Schema {
      partitioning: vec![
        PartitionMeta {
          name: "part".to_string(),
          dtype: ProtobufEnumOrUnknown::new(PartitionDataType::STRING),
          ..Default::default()
        }
      ],
      columns: vec![
        i_meta.clone(),
        ColumnMeta {
          name: "s".to_string(),
          dtype: ProtobufEnumOrUnknown::new(DataType::STRING),
          ..Default::default()
        },
        l_meta.clone()
      ],
      ..Default::default()
    }),
    ..Default::default()
  };
  let create_resp = client.api_create_table(&create_table_req).await?;
  println!("Created table: {:?}", create_resp);

  let mut rows = Vec::new();
  for _ in 0..50 {
    rows.push(
      Row {
        fields: vec![
          Field {
            name: "s".to_string(),
            value: MessageField::some(FieldValue {
              value: Some(Value::string_val("a row".to_string())),
              ..Default::default()
            }),
            ..Default::default()
          },
          Field {
            name: "i".to_string(),
            value: MessageField::some(FieldValue {
              value: Some(Value::int64_val(33)),
              ..Default::default()
            }),
            ..Default::default()
          },
          Field {
            name: "l".to_string(),
            value: MessageField::some(FieldValue {
              value: Some(Value::list_val(RepeatedFieldValue {
                vals: vec![
                  FieldValue {
                    value: Some(Value::string_val("l0 item".to_string())),
                    ..Default::default()
                  },
                  FieldValue {
                    value: Some(Value::string_val("l1 item".to_string())),
                    ..Default::default()
                  },
                ],
                ..Default::default()
              })),
              ..Default::default()
            }),
            ..Default::default()
          },
        ],
        ..Default::default()
      },
    );
    rows.push(Row::new());
  }
  let write_to_partition_req = WriteToPartitionRequest {
    table_name: TABLE_NAME.to_string(),
    partition: vec![
      PartitionField {
        name: "part".to_string(),
        value: Some(PartitionValue::string_val("x0".to_string())),
        ..Default::default()
      },
    ],
    rows,
    ..Default::default()
  };
  for _ in 0..1000 {
    client.api_write_to_partition(&write_to_partition_req).await?;
  }
  // let write_resp = client.write_to_partition(&write_to_partition_req).await?;
  // println!("Wrote rows: {:?}", write_resp);

  let list_segments_eq = ListSegmentsRequest {
    table_name: TABLE_NAME.to_string(),
    partition_filter: vec![
      PartitionFilter {
        value: Some(PartitionFilterValue::equal_to(PartitionField {
          name: "part".to_string(),
          value: Some(PartitionValue::string_val("x0".to_string())),
          ..Default::default()
        })),
        ..Default::default()
      }
    ],
    ..Default::default()
  };
  let list_resp = client.api_list_segments(&list_segments_eq).await?;
  println!("Listed segments: {:?}", list_resp);

  let mut total = 0;
  let partition = vec![
    PartitionField {
      name: "part".to_string(),
      value: Some(PartitionValue::string_val("x0".to_string())),
      ..Default::default()
    }
  ];
  let columns_to_decode = vec![
    i_meta.clone(),
  ];
  for segment in &list_resp.segments {
    let rows = client.decode_segment(
      TABLE_NAME,
      &partition,
      &segment.segment_id,
      &columns_to_decode,
    ).await?;
    let count = rows.len();
    total += count;
    println!("read segment {} with {} rows (total {})", segment.segment_id, count, total);
  }

  let drop_resp = client.api_drop_table(&DropTableRequest {
    table_name: TABLE_NAME.to_string(),
    ..Default::default()
  }).await?;
  println!("Dropped table: {:?}", drop_resp);

  Ok(())
}
