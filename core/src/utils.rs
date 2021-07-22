use std::array::TryFromSliceError;
use std::convert::TryInto;

pub fn try_byte_array<const N: usize>(v: &[u8]) -> Result<[u8; N], TryFromSliceError> {
  v.try_into()
}
