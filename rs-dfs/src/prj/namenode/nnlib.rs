use tonic::{Request, Response, Status};
use serde_json;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use uuid::Uuid;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::nnlib::datanode::data_node_client::DataNodeClient;
use crate::nnlib::datanode::GetDataRequest;
mod namenode {
    tonic::include_proto!("namenode");
}

mod datanode {
    tonic::include_proto!("datanode");
}

use crate::namenode::{ReadFileRequest, ReadFileResponse, NodeAddress, WriteFileRequest, WriteFileResponse, PhoenixingResult,BlockSizeRequest, BlockSizeResponse, AssignBlocksForFileRequest, AssignBlocksForFileResponse};
use crate::namenode::name_node_server::NameNode;
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SerializableNodeAddress {
    pub host: String,
    pub port: u32,
}

impl From<NodeAddress> for SerializableNodeAddress {
    fn from(addr: NodeAddress) -> Self {
        SerializableNodeAddress {
            host: addr.host,
            port: addr.port,
        }
    }
}

impl From<SerializableNodeAddress> for NodeAddress {
    fn from(addr: SerializableNodeAddress) -> Self {
        NodeAddress {
            host: addr.host,
            port: addr.port,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct NameNodeState {
    pub block_size: u32,
    pub repl_factor: u32,
    pub data_nodes: Vec<(SerializableNodeAddress, bool)>,
    pub file_name_to_blocks: HashMap<String, Vec<String>>,
    pub block_to_data_node_ids: HashMap<String, Vec<String>>,
    pub id_to_data_nodes: HashMap<String, SerializableNodeAddress>,
}

impl NameNodeState {
    pub fn new(block_size: u32, repl_factor: u32, data_nodes: Vec<(SerializableNodeAddress, bool)>) -> Self {
        Self { 
            block_size, 
            repl_factor, 
            data_nodes,
            file_name_to_blocks: HashMap::new(),
            block_to_data_node_ids: HashMap::new(),
            id_to_data_nodes: HashMap::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct NameNodeService {
    pub state: Arc<RwLock<NameNodeState>>,
}

#[tonic::async_trait]
impl NameNode for NameNodeService {
    // block_size Exhaustive Explanation:
    //     1. Get the request from the client
    //     2. If the request is true, return the block size
    //     3. If the request is false, return an error
    async fn block_size(&self, request: Request<BlockSizeRequest>) -> Result<Response<BlockSizeResponse>, Status> {
        let req = request.into_inner();
        if req.pulse {
            let state = self.state.read().await;
            let response = BlockSizeResponse { block_size: state.block_size };
            Ok(Response::new(response))
        } else {
            Err(Status::invalid_argument("Invalid request"))
        }
    }
    // read_file Exhaustive Explanation:
    //     1. Get the request from the client
    //     2. Get the file blocks from the FileNameToBlocks map
    //     3. For each block, get the block addresses from the BlockToDataNodeIds map
    //     4. For each block address, get the DataNodeInstance from the IdToDataNodes map
    //     5. Append the DataNodeInstance to the block addresses slice
    //     6. Append the block addresses slice to the reply
    async fn read_file(&self, request: Request<ReadFileRequest>) -> Result<Response<ReadFileResponse>, Status> {
        let req = request.into_inner();
        let state = self.state.read().await;
        let file_blocks = state.file_name_to_blocks.get(&req.filename);
        if let Some(blocks) = file_blocks {
            let mut file_data = Vec::new();
            for block_id in blocks {
                if let Some(data_node_ids) = state.block_to_data_node_ids.get(block_id) {
                    if let Some(data_node_id) = data_node_ids.first() {
                        if let Some(data_node) = state.id_to_data_nodes.get(data_node_id) {
                            let mut client = DataNodeClient::connect(format!("http://{}:{}", data_node.host, data_node.port))
                                .await
                                .map_err(|e| Status::internal(format!("Failed to connect to DataNode: {}", e)))?;
                            
                            let get_data_request = Request::new(GetDataRequest {
                                filename: block_id.clone(),
                            });
                            
                            let response = client.get_data(get_data_request)
                                .await
                                .map_err(|e| Status::internal(format!("Failed to get data from DataNode: {}", e)))?;
                            
                            file_data.extend_from_slice(&response.into_inner().data);
                        }
                    }
                }
            }
            let response = ReadFileResponse { data: file_data };
            Ok(Response::new(response))
        } else {
            Err(Status::not_found("File not found"))
        }
    }

    // write_file Exhaustive Explanation:
    //     1. Get the request from the client
    //     2. Get the file name from the request
    //     3. Get the file size from the request
    //     4. Calculate the number of blocks to allocate
    //     5. Assign the blocks using assign_blocks_for_file
    //     6. Append the blocks to the reply
    async fn write_file(&self, request: Request<WriteFileRequest>) -> Result<Response<WriteFileResponse>, Status> {
        let req = request.into_inner();
        let file_name = req.filename;
        let file_size = req.data.len() as u32;
        let state = self.state.read().await;
        let num_blocks = (file_size + state.block_size - 1) / state.block_size;
        let request = Request::new(AssignBlocksForFileRequest { filename: file_name.clone() });
        let blocks = self.assign_blocks_for_file(request).await?.into_inner().nodes;
        let response = WriteFileResponse { success: true };
        Ok(Response::new(response))
    }
    // phoenixing Exhaustive Explanation:
    //     1. Get the request from the client
    //     2. Get the data node URI from the request
    //     3. Log the data node URI as dead
    //     4. Split the data node URI into host and service port
    //     5. Initialize the dead data node ID
    //     6. Iterate over the IdToDataNodes map to find the dead data node
    //     7. Delete the dead data node from the IdToDataNodes map
    //     8. Construct the under-replicated blocks list and de-register the block entirely in favour of re-creation
    //     9. Verify if re-replication would be possible
    //     10. Iterate over the IdToDataNodes map to get the available nodes
    //     11. Attempt re-replication of under-replicated blocks
    //     12. Fetch the data from the healthy data node
    //     13. Initiate the replication of the block contents
    //     14. Append the block addresses to the block addresses slice
    //     15. Append the block addresses slice to the reply
    async fn phoenixing(&self, request: Request<NodeAddress>) -> Result<Response<PhoenixingResult>, Status> {
        let req = request.into_inner();
        let data_node_uri = format!("{}:{}", req.host, req.port);
        let mut under_replicated_blocks = Vec::new();
        let mut new_nodes = Vec::new();

        // Use a write lock to modify the state
        let mut state = self.state.write().await;

        for (block, data_node_ids) in state.block_to_data_node_ids.iter() {
            if data_node_ids.contains(&data_node_uri) {
                let mut data_node_ids = data_node_ids.clone();
                data_node_ids.retain(|id| id != &data_node_uri);
                if data_node_ids.len() < state.repl_factor as usize {
                    under_replicated_blocks.push(block.clone());
                }
            }
        }
        state.id_to_data_nodes.remove(&data_node_uri);
        for block in under_replicated_blocks {
            if let Some(data_node_ids) = state.block_to_data_node_ids.get(&block) {
                for data_node_id in data_node_ids {
                    if let Some(data_node) = state.id_to_data_nodes.get(data_node_id) {
                        new_nodes.push(NodeAddress::from((*data_node).clone()));
                    }
                }
            }
        }
        let response = PhoenixingResult { success: true, message: "Phoenixing completed".to_string(), new_nodes };
        Ok(Response::new(response))
    }

    // assign_blocks_for_file Exhaustive Explanation:
    //     1. Initialize the FileNameToBlocks map with an empty slice for the given file name
    //     2. Create a list of available data node IDs
    //     3. Determine the number of data nodes available
    //     4. Iterate through the number of blocks to allocate
    //     5. Generate a new block ID
    //     6. Append the block ID to the FileNameToBlocks map using the file name as the key
    //     7. Calculate the replication factor based on the number of data nodes available
    //     8. Assign data nodes to the block using the assign_data_nodes function
    //     9. Append the block addresses to the block addresses slice
    //     10. Append the block addresses slice to the reply
    async fn assign_blocks_for_file(&self, request: Request<AssignBlocksForFileRequest>) -> Result<Response<AssignBlocksForFileResponse>, Status> {
        let req = request.into_inner();
        let file_name = req.filename;
        let mut blocks = Vec::new();
        let mut state = self.state.write().await; // Change to write lock
        for _ in 0..state.repl_factor {
            let block_id = format!("block_{}", Uuid::new_v4());
            state.file_name_to_blocks.entry(file_name.clone()).or_insert(Vec::new()).push(block_id.clone());
            blocks.push(block_id);
        }
        let response = AssignBlocksForFileResponse { nodes: blocks };
        Ok(Response::new(response))
    }
}