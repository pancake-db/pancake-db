mod create_table;
mod write_to_partition;

use std::net::IpAddr;
use hyper::Client as HClient;
use hyper::client::HttpConnector;

pub struct Client {
  pub ip: IpAddr,
  pub port: u16,
  pub h_client: HClient<HttpConnector>,
}

impl Client {
  pub fn from_ip_port(ip: IpAddr, port: u16) -> Self {
    let h_client = HClient::new();
    Client {
      ip,
      port,
      h_client,
    }
  }

  pub fn rest_endpoint(&self, name: &'static str) -> String {
    format!("http://{}:{}/rest/{}", self.ip, self.port, name)
  }
}
