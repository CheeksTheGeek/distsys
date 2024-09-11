use clap::{Arg, Command};
mod nnlib;
mod namenode {
    tonic::include_proto!("namenode");
}
use namenode::name_node_server::{NameNode, NameNodeServer};
use namenode::{ReadFileRequest, ReadFileResponse, NodeAddress, WriteFileRequest, WriteFileResponse, PhoenixingResult,BlockSizeRequest, BlockSizeResponse, AssignBlocksForFileRequest, AssignBlocksForFileResponse};
use tonic::{transport::Server, Request, Response, Status};
use std::net::SocketAddr;
use std::str::FromStr;
use std::fs;
use serde_json;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use uuid::Uuid;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SerializableNodeAddress {
    host: String,
    port: u32,
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
    state: Arc<RwLock<NameNodeState>>,
}

#[tonic::async_trait]
impl NameNode for NameNodeService {
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

    async fn read_file(&self, request: Request<ReadFileRequest>) -> Result<Response<ReadFileResponse>, Status> {
        let req = request.into_inner();
        let state = self.state.read().await;
        let file_blocks = state.file_name_to_blocks.get(&req.filename);
        if let Some(blocks) = file_blocks {
            let mut data = Vec::new();
            for block in blocks {
                if let Some(data_node_ids) = state.block_to_data_node_ids.get(block) {
                    for data_node_id in data_node_ids {
                        if let Some(data_node) = state.id_to_data_nodes.get(data_node_id) {
                            data.push(data_node.clone());
                        }
                    }
                }
            }
            let response = ReadFileResponse { data: serde_json::to_vec(&data).unwrap() };
            Ok(Response::new(response))
        } else {
            Err(Status::not_found("File not found"))
        }
    }

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = Command::new("DFS Namenode")
        .version("0.1.0")
        .about("DFS Namenode")
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .help("Sets the port to listen on")
        )
        .arg(
            Arg::new("blockSize")
                .short('b')
                .long("block-size")
                .value_name("BLOCK_SIZE")
                .help("Sets the block size")
        )
        .arg(
            Arg::new("replFactor")
                .short('r')
                .long("repl-factor")
                .value_name("REPL_FACTOR")
                .help("Sets the replication factor")
        )
        .arg(
            Arg::new("dataNodes")
                .short('d')
                .long("data-nodes")
                .value_name("DATA_NODES")
                .help("List of data nodes in cluster")
        )
        .get_matches();

    let port = matches.get_one::<String>("port").map(String::as_str).unwrap_or("8080");
    let block_size = matches.get_one::<String>("blockSize").map(String::as_str).unwrap_or("100");
    let repl_factor = matches.get_one::<String>("replFactor").map(String::as_str).unwrap_or("3");
    let data_nodes = matches.get_one::<String>("dataNodes").map(String::as_str).unwrap_or("localhost:8080,localhost:8081,localhost:8082");

    println!("Port: {}", port);
    println!("Block Size: {}", block_size);
    println!("Replication Factor: {}", repl_factor);

    let data_nodes = parse_data_nodes(data_nodes);
    println!("Data Nodes: {:?}", data_nodes);
    let nn_addr = format!("0.0.0.0:{}", port);
    let mut state = NameNodeState::new(
        block_size.parse().unwrap(),
        repl_factor.parse().unwrap(),
        data_nodes.into_iter().map(|(addr, _)| (addr.into(), false)).collect()
    );

    let state_file = format!("{}.state", port);
    if let Ok(state_str) = fs::read_to_string(&state_file) {
        if let Ok(loaded_state) = serde_json::from_str::<NameNodeState>(&state_str) {
            state = loaded_state;
        }
    }

    let server = Server::builder()
        .add_service(NameNodeServer::new(NameNodeService { state: Arc::new(RwLock::new(state)) }))
        .serve(SocketAddr::from_str(&nn_addr).unwrap());

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            println!("Still waiting for requests...");
        }
    });

    match server.await {
        Ok(_) => println!("Server shut down gracefully"),
        Err(e) => println!("Server error: {}", e),
    }

    Ok(())
}

fn parse_data_nodes(data_nodes: &str) -> Vec<(NodeAddress, bool)> {
    data_nodes.split(",").map(|node| {
        let parts: Vec<&str> = node.split(":").collect();
        (NodeAddress {
            host: parts[0].to_string(),
            port: parts[1].parse().unwrap(),
        }, false)
    }).collect()
}
