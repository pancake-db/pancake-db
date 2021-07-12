use std::net::{SocketAddr, TcpListener};

use hyper::Server as HyperServer;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;

use server::Server;

mod utils;
mod server;
mod dirs;
mod storage;
mod compression;
mod types;

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
  let outcomes = futures::future::join3(
    HyperServer::from_tcp(listener)
      .unwrap()
      .serve(Shared::new(tower_service)),
    // server.stop(),
    backgrounds.0,
    backgrounds.1,
  )
    .await;

  outcomes.0.expect("server crashed");
}
