use std::str::FromStr;

use aws_config::ConfigLoader;
use aws_sdk_s3::{Client, Credentials, Endpoint, Region};
use http::Uri;

use crate::errors::ServerError;
use crate::{Server, ServerResult};

impl Server {
  pub async fn s3_client(&self, endpoint: &Option<String>) -> ServerResult<Client> {
    let cloud_opts = &self.opts.cloud_opts;
    if endpoint != &cloud_opts.s3_endpoint {
      return Err(ServerError::internal(format!(
        "multiple S3 endpoints ({:?} and {:?}) not yet supported",
        endpoint,
        cloud_opts.s3_endpoint,
      )));
    }

    {
      let maybe_client = &*self.s3_client.read().await;
      if let Some(client) = maybe_client {
        return Ok(client.clone());
      }
    }

    let mut maybe_client = self.s3_client.write().await;
    if maybe_client.is_none() {
      let mut config_loader = ConfigLoader::default()
        .credentials_provider(Credentials::new(
          cloud_opts.s3_access_key.clone().unwrap(),
          cloud_opts.s3_secret_key.clone().unwrap(),
          None,
          None,
          "PancakeDB S3",
        ));
      if let Some(endpoint) = endpoint {
        let uri = Uri::from_str(endpoint)
          .map_err(|e| ServerError::from(e)
            .with_context("while creating S3 client")
          )?;
        config_loader = config_loader.endpoint_resolver(Endpoint::immutable(uri));
      }
      if let Some(region) = &cloud_opts.s3_region {
        config_loader = config_loader.region(Region::new(region.to_string()));
      }
      let config = config_loader.load().await;
      *maybe_client = Some(Client::new(&config));
    }

    return Ok(maybe_client.clone().unwrap());
  }
}
