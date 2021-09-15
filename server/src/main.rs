use std::net::{SocketAddr, TcpListener};

use hyper::Server as HyperServer;
use structopt::StructOpt;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;

use crate::opt::Opt;
use crate::server::Server;

mod dirs;
mod opt;
mod server;
mod types;
mod utils;
pub mod errors;
pub mod storage;

#[tokio::main]
async fn main() {
  let opts: Opt = Opt::from_args();
  let server = Server::new(opts.clone());
  let backgrounds = server.init().await;

  let filter = server.warp_filter();
  let warp_service = warp::service(filter);
  let tower_service = ServiceBuilder::new()
    .layer(AddExtensionLayer::new(server.clone()))
    .service(warp_service);
  let listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], opts.port)))
    .expect("port busy");
  let outcomes = futures::future::join3(
    HyperServer::from_tcp(listener)
      .unwrap()
      .serve(Shared::new(tower_service)),
    backgrounds.0,
    backgrounds.1,
  )
    .await;

  outcomes.0.expect("server crashed");
}
