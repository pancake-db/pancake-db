use std::net::{IpAddr, Ipv4Addr};

use pancake_db_idl::ddl::CreateTableRequest;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use pancake_db_idl::schema::{ColumnMeta, PartitionMeta, Schema};
use protobuf::{MessageField, ProtobufEnumOrUnknown};
use tokio;

use pancake_db_client::Client;
use pancake_db_client::errors::Result as ClientResult;
use pancake_db_idl::dml::{WriteToPartitionRequest, PartitionField, Row, Field, FieldValue, RepeatedFieldValue};
use pancake_db_idl::dml::partition_field::Value as PartitionValue;
use pancake_db_idl::dml::field_value::Value;

const TABLE_NAME: &str = "t";

#[tokio::main]
async fn main() -> ClientResult<()> {
  let client = Client::from_ip_port(
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
    1337,
  );

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
        ColumnMeta {
          name: "i".to_string(),
          dtype: ProtobufEnumOrUnknown::new(DataType::INT64),
          ..Default::default()
        },
        ColumnMeta {
          name: "s".to_string(),
          dtype: ProtobufEnumOrUnknown::new(DataType::STRING),
          ..Default::default()
        },
        ColumnMeta {
          name: "l".to_string(),
          dtype: ProtobufEnumOrUnknown::new(DataType::STRING),
          nested_list_depth: 1,
          ..Default::default()
        }
      ],
      ..Default::default()
    }),
    ..Default::default()
  };
  let create_resp = client.create_table(&create_table_req).await?;
  println!("Created table: {:?}", create_resp);

  let rows = vec![
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
                  value: Some(Value::string_val("l0 item".to_string())),
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
    Row::new(),
  ];
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
  let write_resp = client.write_to_partition(&write_to_partition_req).await?;
  println!("Wrote rows: {:?}", write_resp);

  Ok(())
}
