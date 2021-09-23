use std::fmt;
use crate::errors::{CoreError, CoreResult};
use super::{NULL_BYTE, ESCAPE_BYTE};
use std::fmt::{Formatter, Debug};

pub struct ByteReader<'a> {
  bytes: &'a [u8],
  i: usize,
}

impl<'a> Debug for ByteReader<'a> {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "ByteReader at {}; prev: {:?} next: {:?}",
      self.i,
      &self.bytes[0.max(self.i - 10)..self.i],
      &self.bytes[self.i..self.bytes.len().min(self.i + 10)],
    )
  }
}

impl<'a> ByteReader<'a> {
  pub fn new(bytes: &'a [u8]) -> Self {
    ByteReader {
      bytes,
      i: 0,
    }
  }

  pub fn complete(&self) -> bool {
    self.i >= self.bytes.len()
  }

  pub fn back_one(&mut self) {
    self.i -= 1;
  }

  pub fn read_one(&mut self) -> CoreResult<u8> {
    if self.i >= self.bytes.len() {
      return Err(CoreError::corrupt("read_one out of bytes"));
    }
    let res = self.bytes[self.i];
    self.i += 1;
    Ok(res)
  }

  pub fn unescaped_read_one(&mut self) -> CoreResult<u8> {
    let b = self.read_one()?;
    if b == ESCAPE_BYTE {
      Ok(!self.read_one()?)
    } else if b >= NULL_BYTE {
      Err(CoreError::corrupt(&format!("unexpected unescaped byte at {}", self.i)))
    } else {
      Ok(b)
    }
  }

  pub fn unescaped_read_n(&mut self, n: usize) -> CoreResult<Vec<u8>> {
    let mut res = Vec::with_capacity(n);
    for _ in 0..n {
      res.push(self.unescaped_read_one()?);
    }
    Ok(res)
  }

  pub fn unescaped_read_u16(&mut self) -> CoreResult<u16> {
    let byte0 = self.unescaped_read_one()?;
    let byte1 = self.unescaped_read_one()?;
    Ok(byte0 as u16 * 256 + byte1 as u16)
  }

  pub fn read_n(&mut self, n: usize) -> CoreResult<&'a [u8]> {
    if self.i + n >= self.bytes.len() {
      return Err(CoreError::corrupt("read_n out of bytes"));
    }
    let res = &self.bytes[self.i..self.i+n];
    self.i += n;
    Ok(res)
  }
}

