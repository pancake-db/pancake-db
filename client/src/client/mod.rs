use std::net::IpAddr;

use hyper::{Body, Client as HClient, Method, StatusCode, Request};
use hyper::body::HttpBody;
use hyper::client::HttpConnector;
use protobuf::Message;

use crate::errors::{ClientError, ClientResult};

mod create_table;
mod read;
mod write_to_partition;
mod delete;

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

  pub fn rest_endpoint(&self, name: &str) -> String {
    format!("http://{}:{}/rest/{}", self.ip, self.port, name)
  }

  async fn simple_json_request<Req: Message, Resp: Message>(
    &self,
    endpoint_name: &str,
    method: Method,
    req: &Req
  ) -> ClientResult<Resp> {
    let uri = self.rest_endpoint(endpoint_name);
    let pb_str = protobuf::json::print_to_string(req)?;

    let http_req = Request::builder()
      .method(method)
      .uri(&uri)
      .header("Content-Type", "application/json")
      .body(Body::from(pb_str))?;
    let mut resp = self.h_client.request(http_req).await?;
    let status = resp.status();
    let mut content = String::new();
    while let Some(chunk) = resp.body_mut().data().await {
      content.push_str(&String::from_utf8(chunk?.to_vec())?);
    }

    if status != StatusCode::OK {
      return Err(ClientError::http(status, &content));
    }
    let mut res = Resp::new();
    protobuf::json::merge_from_str(&mut res, &content)?;
    Ok(res)
  }
}
