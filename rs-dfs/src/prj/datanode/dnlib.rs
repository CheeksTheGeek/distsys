use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use tonic::{Request, Response, Status};
use crate::datanode::data_node_client::DataNodeClient;
use crate::datanode::data_node_server::DataNode;
use crate::datanode::{PulseRequest, PulseResponse, GetDataRequest, GetDataResponse, PutDataRequest, PutDataResponse};
use std::sync::Arc;
use tokio::sync::RwLock;
pub struct DataNodeState {
    data_dir: String,
}

pub struct DataNodeService {
    state: Arc<RwLock<DataNodeState>>,
}

impl DataNodeService {
    pub fn new(data_dir: String) -> Self {
        DataNodeService { state: Arc::new(RwLock::new(DataNodeState { data_dir })) }
    }
}

#[tonic::async_trait]
impl DataNode for DataNodeService {
    // Pulse Exhaustive Explanation:
    // Pulse is used by namenode to check if the datanode is still alive or not, it has two modes, pulse = true means that the namenode is checking if the datanode is still alive, pulse = false is for an initial ping
    //     - the datanode should respond with a PulseResponse with success = true if it is still alive or if the namenode is sending an initial ping
    //     - the datanode should respond with a PulseResponse with success = false if there's something wrong with the datanode (disk error, network error, etc.) 
    //         or if the datanode is not able to serve requests, or if the namenode is sending an initial ping and that has failed (the datanode is already registered with another namenode)
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
    // get_data Exhaustive Explanation:
    //      1. Get the block ID from the request
    //      2. Read the file from the data directory with the block ID as the name
    //      3. Return the data to the NameNode
    async fn get_data(&self, request: Request<GetDataRequest>) -> Result<Response<GetDataResponse>, Status> {
        let req = request.into_inner();
        let state = self.state.read().await;
        let file_path = Path::new(&state.data_dir).join(req.filename);  
        let mut file = File::open(file_path).map_err(|e| Status::internal(format!("Failed to open file: {}", e)))?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(|e| Status::internal(format!("Failed to read file: {}", e)))?;
        Ok(Response::new(GetDataResponse { data }))
    }

    // put_data Exhaustive Explanation:
    //    1. Create a new file in the data directory with the block ID as the name
    //    2. Write the data to the file
    //    3. Flush the file writer
    //    4. Forward the data to the next data node for replication
    async fn put_data(&self, request: Request<PutDataRequest>) -> Result<Response<PutDataResponse>, Status> {
        let req = request.into_inner();
        let state = self.state.read().await;
        let file_path = Path::new(&state.data_dir).join(req.block_id.clone());
        let mut file = OpenOptions::new().create(true).write(true).open(file_path).map_err(|e| Status::internal(format!("Failed to create file: {}", e)))?;
        file.write_all(&req.data).map_err(|e| Status::internal(format!("Failed to write file: {}", e)))?;
        file.flush().map_err(|e| Status::internal(format!("Failed to flush file: {}", e)))?;
        if req.nodes_left.len() > 0 {
            pass_data_onto_next_dn(req.block_id.clone(), req.data, req.nodes_left).await?;
        }
        Ok(Response::new(PutDataResponse { success: true }))
    }

}


// pass_data_onto_next_dn Exhaustive Explanation:
//      1. Get the block ID and the replication nodes from the request
//      2. If there are no replication nodes, return nil
//      3. Get the starting data node from the block addresses
//      4. Get the remaining data nodes from the block addresses
//      5. Dial the starting data node and call the PutData method
//      6. Forward the data to the next data node for replication by calling the PutData method on the next data node
async fn pass_data_onto_next_dn(block_id: String, data: Vec<u8>, nodes_left: Vec<String>) -> Result<Response<PutDataResponse>, Status> {
    let put_data_request = PutDataRequest {
        block_id,
        data,
        nodes_left: nodes_left[1..].to_vec(),
    };
    // make grpc call to the next node, the nodes_left is formatted like this: "host:port,host:port,host:port"
    let first_node = nodes_left.first().ok_or_else(|| Status::internal("No nodes left"))?;
    let mut client = DataNodeClient::connect(format!("http://{}", first_node)).await.map_err(|e| Status::internal(format!("Failed to connect: {}", e)))?;
    let put_data_response = client.put_data(put_data_request).await.map_err(|e| Status::internal(format!("Failed to put data: {}", e)))?;
    Ok(put_data_response) 
}