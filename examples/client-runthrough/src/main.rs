use std::net::{IpAddr, Ipv4Addr};

use pancake_db_idl::ddl::CreateTableRequest;
use pancake_db_idl::dtype::DataType;
use pancake_db_idl::partition_dtype::PartitionDataType;
use pancake_db_idl::schema::{ColumnMeta, PartitionMeta, Schema};
use protobuf::{MessageField, ProtobufEnumOrUnknown};
use tokio;

use pancake_db_client::Client;
use pancake_db_client::errors::Result as ClientResult;

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
      ],
      ..Default::default()
    }),
    ..Default::default()
  };
  let create_resp = client.create_table(&create_table_req).await?;
  println!("Created table: {:?}", create_resp);

  Ok(())
}
