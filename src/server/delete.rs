use pancake_db_idl::ddl::{DropTableRequest, DropTableResponse};
use pancake_db_idl::dml::{DeleteFromSegmentRequest, DeleteFromSegmentResponse};

use crate::errors::ServerResult;
use crate::ops::delete_from_segment::DeleteFromSegmentOp;
use crate::ops::drop_table::DropTableOp;
use crate::ops::traits::ServerOp;
use crate::server::Server;

impl Server {
  pub async fn delete_from_segment(&self, req: DeleteFromSegmentRequest) -> ServerResult<DeleteFromSegmentResponse> {
    DeleteFromSegmentOp { req }.execute(self).await
  }

  pub async fn drop_table(&self, req: DropTableRequest) -> ServerResult<DropTableResponse> {
    DropTableOp { req }.execute(self).await
  }
}