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

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct NameNodeState {
    pub block_size: u32,
    pub repl_factor: u32,
    pub data_nodes: Vec<(SerializableNodeAddress, bool)>,
    pub file_name_to_blocks: HashMap<String, Vec<String>>,
    pub block_to_data_node_ids: HashMap<String, Vec<String>>,
    pub id_to_data_nodes: HashMap<String, NodeAddress>,
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
    state: NameNodeState,
}

#[tonic::async_trait]
impl NameNode for NameNodeService {
    async fn block_size(&self, request: Request<BlockSizeRequest>) -> Result<Response<BlockSizeResponse>, Status> {
        let req = request.into_inner();
        if req.pulse {
            let response = BlockSizeResponse { block_size: self.state.block_size };
            Ok(Response::new(response))
        } else {
            Err(Status::invalid_argument("Invalid request"))
        }
    }

    async fn read_file(&self, request: Request<ReadFileRequest>) -> Result<Response<ReadFileResponse>, Status> {
        let req = request.into_inner();
        let file_blocks = self.state.file_name_to_blocks.get(&req.filename);
        if let Some(blocks) = file_blocks {
            let mut data = Vec::new();
            for block in blocks {
                if let Some(data_node_ids) = self.state.block_to_data_node_ids.get(block) {
                    for data_node_id in data_node_ids {
                        if let Some(data_node) = self.state.id_to_data_nodes.get(data_node_id) {
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
        let num_blocks = (file_size + self.state.block_size - 1) / self.state.block_size;
        let blocks = self.assign_blocks_for_file(file_name.clone(), num_blocks).await?;
        let response = WriteFileResponse { success: true };
        Ok(Response::new(response))
    }

    async fn phoenixing(&self, request: Request<NodeAddress>) -> Result<Response<PhoenixingResult>, Status> {
        let req = request.into_inner();
        let data_node_uri = format!("{}:{}", req.host, req.port);
        let mut under_replicated_blocks = Vec::new();
        for (block, data_node_ids) in self.state.block_to_data_node_ids.iter_mut() {
            if data_node_ids.contains(&data_node_uri) {
                data_node_ids.retain(|id| id != &data_node_uri);
                if data_node_ids.len() < self.state.repl_factor as usize {
                    under_replicated_blocks.push(block.clone());
                }
            }
        }
        self.state.id_to_data_nodes.remove(&data_node_uri);
        let mut new_nodes = Vec::new();
        for block in under_replicated_blocks {
            if let Some(data_node_ids) = self.state.block_to_data_node_ids.get(&block) {
                for data_node_id in data_node_ids {
                    if let Some(data_node) = self.state.id_to_data_nodes.get(data_node_id) {
                        new_nodes.push(data_node.clone());
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
        for _ in 0..self.state.repl_factor {
            let block_id = format!("block_{}", uuid::Uuid::new_v4());
            self.state.file_name_to_blocks.entry(file_name.clone()).or_insert(Vec::new()).push(block_id.clone());
            blocks.push(block_id);
        }
        let response = AssignBlocksForFileResponse { nodes: blocks };
        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = Command::new("HDFS Namenode")
        .version("0.1.0")
        .about("HDFS Namenode")
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
        .add_service(NameNodeServer::new(NameNodeService { state }))
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
