use server::Server;
use hyper::Server as HyperServer;

use protobuf::ProtobufEnumOrUnknown;
use tokio::time;
use warp::Filter;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use std::net::{TcpListener, SocketAddr};
use tower::make::Shared;

mod utils;
mod types;
mod server;
mod dirs;
mod storage;
mod compression;

const DIR: &str = "/Users/martin/Downloads/pancake_db_data";

#[tokio::main]
async fn main() {
  let server = Server::new(DIR.to_string());
  let backgrounds = server.init().await;

  let filter = server.warp_filter();
  let warp_service = warp::service(filter);
  let tower_service = ServiceBuilder::new()
    .layer(AddExtensionLayer::new(server.clone()))
    .service(warp_service);
  let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 1337)))
    .expect("port busy");
  HyperServer::from_tcp(listener)
    .unwrap()
    .serve(Shared::new(tower_service))
    .await
    .expect("idk what the issue is");

  server.stop().await;
  backgrounds.0.await;
  backgrounds.1.await;
}
