mod create_table;

use std::net::IpAddr;

pub struct Client {
  pub ip: IpAddr,
  pub port: u16,
}

impl Client {
  pub fn from_ip_port(ip: IpAddr, port: u16) -> Self {
    Client {
      ip,
      port
    }
  }
}
