pub use traits::{PersistentMetadata, MetadataKey};

mod traits;
pub mod segment;
pub mod table;
pub mod compaction;
pub mod partition;
pub mod global;
pub mod deletion;
pub mod correlation;
