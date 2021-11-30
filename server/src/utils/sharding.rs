use uuid::Uuid;

pub fn segment_id_to_shard(n_shards_log: u32, segment_id: Uuid) -> u64 {
  let mut res = 0;
  let bytes = segment_id.as_bytes();
  for bit_idx in 0..n_shards_log {
    let byte_idx = (bit_idx / 8) as usize;
    let shift = 7 - bit_idx % 8;
    res *= 2;
    res += ((bytes[byte_idx] >> shift) & 1) as u64;
  }
  res
}
