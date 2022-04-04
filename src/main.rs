#![allow(clippy::new_without_default)]
#![allow(clippy::needless_range_loop)]

use std::net::{SocketAddr, TcpListener};

use hyper::Server as HyperServer;
use pancake_db_idl::service::pancake_db_server::PancakeDbServer;
use structopt::StructOpt;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;

use crate::errors::{Contextable, ServerResult};
use crate::logging::Logger;
use crate::opt::Opt;
use crate::server::Server;

mod logging;
mod opt;
mod server;
mod types;
mod utils;
mod constants;
mod errors;
mod ops;
mod metadata;
mod locks;
mod serde_models;

static LOGGER: Logger = Logger;

#[tokio::main]
async fn main() -> ServerResult<()> {
  let opts: Opt = Opt::from_args();
  opts.validate();
  log::set_max_level(opts.log_level);
  log::set_logger(&LOGGER)
    .expect("unable to initialize logging");

  let server = Server::new(opts.clone());
  server.recover()
    .await
    .with_context(|| "while recovering server state")?;

  let backgrounds = server.init()
    .await
    .with_context(|| "while initializing background processes")?;
  log::info!("initialized server background processes in dir {:?}", opts.dir);

  let filter = utils::rest::warp_filter();
  let warp_service = warp::service(filter);
  let tower_service = ServiceBuilder::new()
    .layer(AddExtensionLayer::new(server.clone()))
    .service(warp_service);
  let listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], opts.http_port)))
    .expect("port busy");
  let hyper_future = HyperServer::from_tcp(listener)
    .unwrap()
    .serve(Shared::new(tower_service));
  log::info!("bound HTTP listener to port {}", opts.http_port);

  let tonic_future = tonic::transport::Server::builder()
    .add_service(PancakeDbServer::new(server.clone()))
    .serve(SocketAddr::from(([0, 0, 0, 0], opts.grpc_port)));
  log::info!("bound GRPC listener to port {}", opts.grpc_port);

  log::info!("ready to serve requests");

  let (hyper_res, tonic_res, _, _) = futures::future::join4(
    hyper_future,
    tonic_future,
    backgrounds.0,
    backgrounds.1,
  )
    .await;

  hyper_res.expect("HTTP server crashed");
  tonic_res.expect("GRPC server crashed");
  Ok(())
}
