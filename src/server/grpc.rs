use pancake_db_idl::ddl::{AlterTableRequest, AlterTableResponse, CreateTableRequest, CreateTableResponse, DropTableRequest, DropTableResponse, GetSchemaRequest, GetSchemaResponse, ListTablesRequest, ListTablesResponse};
use pancake_db_idl::dml::{DeleteFromSegmentRequest, DeleteFromSegmentResponse, ListSegmentsRequest, ListSegmentsResponse, ReadSegmentColumnRequest, ReadSegmentColumnResponse, ReadSegmentDeletionsRequest, ReadSegmentDeletionsResponse, WriteToPartitionRequest, WriteToPartitionResponse};
use pancake_db_idl::service::pancake_db_server::PancakeDb;
use tonic::{Request, Response, Status};

use crate::{Server, ServerResult};
use crate::ops::alter_table::AlterTableOp;
use crate::ops::create_table::CreateTableOp;
use crate::ops::delete_from_segment::DeleteFromSegmentOp;
use crate::ops::drop_table::DropTableOp;
use crate::ops::get_schema::GetSchemaOp;
use crate::ops::list_segments::ListSegmentsOp;
use crate::ops::list_tables::ListTablesOp;
use crate::ops::read_segment_column::ReadSegmentColumnOp;
use crate::ops::read_segment_deletions::ReadSegmentDeletionsOp;
use crate::ops::traits::ServerOp;
use crate::ops::write_to_partition::WriteToPartitionOp;

fn grpc_result<T>(pancake_result: ServerResult<T>) -> Result<Response<T>, Status> {
  match pancake_result {
    Ok(resp) => Ok(Response::new(resp)),
    Err(err) => Err(err.into()),
  }
}

#[async_trait::async_trait]
impl PancakeDb for Server {
  async fn alter_table(&self, request: Request<AlterTableRequest>) -> Result<Response<AlterTableResponse>, Status> {
    grpc_result(AlterTableOp { req: request.into_inner() }.execute(&self).await)
  }

  async fn create_table(&self, request: Request<CreateTableRequest>) -> Result<Response<CreateTableResponse>, Status> {
    grpc_result(CreateTableOp { req: request.into_inner() }.execute(&self).await)
  }

  async fn drop_table(&self, request: Request<DropTableRequest>) -> Result<Response<DropTableResponse>, Status> {
    grpc_result(DropTableOp { req: request.into_inner() }.execute(&self).await)
  }

  async fn get_schema(&self, request: Request<GetSchemaRequest>) -> Result<Response<GetSchemaResponse>, Status> {
    grpc_result(GetSchemaOp { req: request.into_inner() }.execute(&self).await)
  }

  async fn list_tables(&self, request: Request<ListTablesRequest>) -> Result<Response<ListTablesResponse>, Status> {
    grpc_result(ListTablesOp { req: request.into_inner() }.execute(&self).await)
  }

  async fn delete_from_segment(&self, request: Request<DeleteFromSegmentRequest>) -> Result<Response<DeleteFromSegmentResponse>, Status> {
    grpc_result(DeleteFromSegmentOp { req: request.into_inner() }.execute(&self).await)
  }

  async fn list_segments(&self, request: Request<ListSegmentsRequest>) -> Result<Response<ListSegmentsResponse>, Status> {
    grpc_result(ListSegmentsOp { req: request.into_inner() }.execute(&self).await)
  }

  async fn read_segment_column(&self, request: Request<ReadSegmentColumnRequest>) -> Result<Response<ReadSegmentColumnResponse>, Status> {
    grpc_result(ReadSegmentColumnOp { req: request.into_inner() }.execute(&self).await)
  }

  async fn read_segment_deletions(&self, request: Request<ReadSegmentDeletionsRequest>) -> Result<Response<ReadSegmentDeletionsResponse>, Status> {
    grpc_result(ReadSegmentDeletionsOp { req: request.into_inner() }.execute(&self).await)
  }

  async fn write_to_partition(&self, request: Request<WriteToPartitionRequest>) -> Result<Response<WriteToPartitionResponse>, Status> {
    grpc_result(WriteToPartitionOp { req: request.into_inner() }.execute(&self).await)
  }
}