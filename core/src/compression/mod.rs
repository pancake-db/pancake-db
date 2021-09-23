pub use traits::{Codec, ValueCodec};
pub use utils::{choose_codec, new_codec};

mod traits;
mod utils;
pub mod q_codec;
pub mod zstd_codec;

pub const Q_COMPRESS: &str = "q_compress";
pub const ZSTD: &str = "zstd";
