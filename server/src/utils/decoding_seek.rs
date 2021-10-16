use pancake_db_idl::dtype::DataType;

use pancake_db_core::encoding;

use crate::errors::{ServerError, ServerResult};

pub fn byte_idx_for_row_idx(
  dtype: DataType,
  nested_list_depth: u8,
  bytes: &[u8],
  idx: usize,
) -> ServerResult<usize> {
  let decoder = encoding::new_byte_idx_decoder(dtype, nested_list_depth);
  let byte_idxs = decoder.decode_limited(bytes, idx)?;
  if byte_idxs.len() != idx {
    return Err(ServerError::internal(&format!(
      "expected at least {} rows in flush file but found {}",
      idx,
      byte_idxs.len(),
    )));
  }

  Ok(byte_idxs.last().copied().unwrap_or(0))
}
