use hyper::{Body, Client as HClient, Method, Request, StatusCode};
use hyper::body::HttpBody;
use pancake_db_idl::ddl::{CreateTableRequest, CreateTableResponse};
use serde_json;

use crate::errors::{Error, Result};

use super::Client;

impl Client {
  pub async fn create_table(&self, req: &CreateTableRequest) -> Result<CreateTableResponse> {
    let h_client = HClient::new();
    let uri = format!("http://{}:{}/rest/create_table", self.ip, self.port);
    let pb_str = serde_json::to_string(req)?;

    let http_req = Request::builder()
      .method(Method::POST)
      .uri(&uri)
      .header("Content-Type", "application/json")
      .body(Body::from(pb_str))?;
    let mut resp = h_client.request(http_req).await?;
    let status = resp.status();
    let mut content = String::new();
    while let Some(chunk) = resp.body_mut().data().await {
      content.push_str(&String::from_utf8(chunk?.to_vec())?);
    }

    if status != StatusCode::OK {
      return Err(Error::http(status, &content));
    }
    let mut res = CreateTableResponse::new();
    protobuf::json::merge_from_str(&mut res, &content)?;
    Ok(res)
  }
}
