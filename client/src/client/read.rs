use pancake_db_idl::dml::{PartitionField, Row, ReadSegmentColumnRequest, FieldValue, Field};
use pancake_db_core::compression;
use pancake_db_core::encoding;

use crate::errors::{ClientResult, ClientError};

use super::Client;
use pancake_db_idl::schema::ColumnMeta;
use protobuf::MessageField;

impl Client {
  async fn decode_segment_column(
    &self,
    table_name: &str,
    partition: &[PartitionField],
    segment_id: &str,
    column: &ColumnMeta,
  ) -> ClientResult<Vec<FieldValue>> {
    let mut initial_request = true;
    let mut continuation_token = "".to_string();
    let mut compressed_bytes = Vec::new();
    let mut uncompressed_bytes = Vec::new();
    let mut codec = "".to_string();
    while initial_request || !continuation_token.is_empty() {
      let req = ReadSegmentColumnRequest {
        table_name: table_name.to_string(),
        partition: partition.to_vec(),
        segment_id: segment_id.to_string(),
        column_name: column.name.clone(),
        continuation_token,
        ..Default::default()
      };
      let resp = self.api_read_segment_column(&req).await?;
      if !resp.codec.is_empty() {
        codec = resp.codec.clone();
      }
      compressed_bytes.extend(&resp.compressed_data);
      uncompressed_bytes.extend(&resp.uncompressed_data);
      continuation_token = resp.continuation_token;
      initial_request = false;
    }
    println!("READ IN {} compressed {} uncompressed for {}", compressed_bytes.len(), uncompressed_bytes.len(), column.name);

    let mut res = Vec::new();

    let dtype = column.dtype.enum_value_or_default();
    if !compressed_bytes.is_empty() {
      let decompressor = compression::new_codec(
        dtype,
        &codec,
      )?;
      res.extend(decompressor.decompress(
        compressed_bytes,
        column.nested_list_depth as u8,
      )?);
    }

    if !uncompressed_bytes.is_empty() {
      let decoder = encoding::new_field_value_decoder(
        dtype,
        column.nested_list_depth as u8,
      );
      res.extend(decoder.decode(&uncompressed_bytes)?);
    }

    Ok(res)
  }

  pub async fn decode_segment(
    &self,
    table_name: &str,
    partition: &[PartitionField],
    segment_id: &str,
    columns: &[ColumnMeta],
  ) -> ClientResult<Vec<Row>> {
    if columns.is_empty() {
      return Err(ClientError::other(
        "unable to decode segment with no columns specified".to_string()
      ))
    }

    let mut n = usize::MAX;
    let mut column_fvalues = Vec::new();
    for column in columns {
      let fvalues = self.decode_segment_column(
        table_name,
        partition,
        segment_id,
        column
      ).await?;
      n = n.min(fvalues.len());
      column_fvalues.push(fvalues);
    }

    let mut rows = Vec::with_capacity(n);
    for row_idx in 0..n {
      let mut row = Row::new();
      for col_idx in 0..column_fvalues.len() {
        row.fields.push(Field {
          name: columns[col_idx].name.clone(),
          value: MessageField::some(column_fvalues[col_idx][row_idx].clone()),
          ..Default::default()
        });
      }
      rows.push(row);
    }
    Ok(rows)
  }
}
