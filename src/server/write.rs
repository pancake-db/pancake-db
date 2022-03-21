use pancake_db_idl::dml::{WriteToPartitionRequest, WriteToPartitionResponse};

use crate::errors::ServerResult;
use crate::ops::traits::ServerOp;
use crate::ops::write_to_partition::WriteToPartitionOp;

use super::Server;

impl Server {
  pub async fn write_to_partition(&self, req: WriteToPartitionRequest) -> ServerResult<WriteToPartitionResponse> {
    WriteToPartitionOp { req }.execute(self).await
  }
}
