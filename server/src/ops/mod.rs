pub mod traits;
pub mod create_table;
pub mod drop_table;
pub mod get_schema;
pub mod list_segments;
pub mod write_to_partition;
pub mod flush;
pub mod read_segment_column;
pub mod compact;

use async_trait::async_trait;

struct MyLifetimeType<'a> {
  s: &'a mut String,
}

#[async_trait]
trait MyTrait<T> {
  async fn handle(t: T) where T: 'async_trait;
}

struct MyImpl;

#[async_trait]
impl<'a> MyTrait<MyLifetimeType<'a>> for MyImpl {
  async fn handle(t: MyLifetimeType<'a>) {
    t.s.push_str("hi");
    println!("{}", t.s);
  }
}
