use pancake_db_idl::dtype::DataType;
use q_compress::TimestampNs;

use crate::errors::{CoreError, CoreResult};
use crate::primitives::Primitive;

use super::{Q_COMPRESS, ZSTD};
use super::ValueCodec;

pub fn new_codec(
  dtype: DataType,
  codec: &str,
) -> CoreResult<Box<dyn ValueCodec>> {
  let maybe_res: Option<Box<dyn ValueCodec>> = match dtype {
    DataType::STRING => String::new_value_codec(codec),
    DataType::INT64 => i64::new_value_codec(codec),
    DataType::BYTES => Vec::<u8>::new_value_codec(codec),
    DataType::BOOL => bool::new_value_codec(codec),
    DataType::FLOAT64 => f64::new_value_codec(codec),
    DataType::TIMESTAMP_NS => TimestampNs::new_value_codec(codec),
  };

  match maybe_res {
    Some(res) => Ok(res),
    None => Err(CoreError::invalid(&format!(
      "compression codec {} unavailable for data type {:?}",
      codec,
      dtype,
    )))
  }
}

pub fn choose_codec(dtype: DataType) -> String {
  match dtype {
    DataType::INT64 => Q_COMPRESS.to_string(),
    DataType::STRING => ZSTD.to_string(),
    DataType::BYTES => ZSTD.to_string(),
    DataType::FLOAT64 => Q_COMPRESS.to_string(),
    DataType::BOOL => Q_COMPRESS.to_string(),
    DataType::TIMESTAMP_NS => Q_COMPRESS.to_string(),
  }
}

