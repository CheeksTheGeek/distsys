use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use tonic::{Request, Response, Status};
use datanode::data_node_server::DataNode;
use datanode::{PulseRequest, PulseResponse, GetDataRequest, GetDataResponse, PutDataRequest, PutDataResponse, ReplicationPassthroughRequest, ReplicationPassthroughResponse};
pub struct DataNodeService {
    data_dir: String,
}

impl DataNodeService {
    pub fn new(data_dir: String) -> Self {
        DataNodeService { data_dir }
    }
}

#[tonic::async_trait]
impl DataNode for DataNodeService {
    async fn pulse(&self, request: Request<PulseRequest>) -> Result<Response<PulseResponse>, Status> {
        let req = request.into_inner();
        let success = if req.pulse {
            // Handle heartbeat
            true
        } else {
            // Handle initial ping
            true
        };
        Ok(Response::new(PulseResponse { success }))
    }

    async fn get_data(&self, request: Request<GetDataRequest>) -> Result<Response<GetDataResponse>, Status> {
        let req = request.into_inner();
        let file_path = Path::new(&self.data_dir).join(req.filename);
        let mut file = File::open(file_path).map_err(|e| Status::internal(format!("Failed to open file: {}", e)))?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(|e| Status::internal(format!("Failed to read file: {}", e)))?;
        Ok(Response::new(GetDataResponse { data }))
    }

    async fn put_data(&self, request: Request<PutDataRequest>) -> Result<Response<PutDataResponse>, Status> {
        let req = request.into_inner();
        let file_path = Path::new(&self.data_dir).join(req.block_id);
        let mut file = OpenOptions::new().create(true).write(true).open(file_path).map_err(|e| Status::internal(format!("Failed to create file: {}", e)))?;
        file.write_all(&req.data).map_err(|e| Status::internal(format!("Failed to write file: {}", e)))?;
        file.flush().map_err(|e| Status::internal(format!("Failed to flush file: {}", e)))?;
        // Forward data to the next data node for replication
        for node in req.nodes_left {
            // Implement forwarding logic here
        }
        Ok(Response::new(PutDataResponse { success: true }))
    }

    async fn replication_passthrough(&self, request: Request<ReplicationPassthroughRequest>) -> Result<Response<ReplicationPassthroughResponse>, Status> {
        let req = request.into_inner();
        if req.data.is_empty() {
            return Ok(Response::new(ReplicationPassthroughResponse { success: false }));
        }
        // Implement replication passthrough logic here
        Ok(Response::new(ReplicationPassthroughResponse { success: true }))
    }
}
