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
}

impl NameNodeState {
    pub fn new(block_size: u32, repl_factor: u32, data_nodes: Vec<(SerializableNodeAddress, bool)>) -> Self {
        Self { block_size, repl_factor, data_nodes }
    }
}

#[derive(Debug, Default)]
pub struct NameNodeService {}

#[tonic::async_trait]
impl NameNode for NameNodeService {
    // block_size Exhaustive Explanation:
    //     1. Get the request from the client
    //     2. If the request is true, return the block size
    //     3. If the request is false, return an error
    async fn block_size(&self, request: Request<BlockSizeRequest>) -> Result<Response<BlockSizeResponse>, Status> {
        let response = BlockSizeResponse { block_size: 100 };
        Ok(Response::new(response))
    }

    // read_file Exhaustive Explanation:
    //     1. Get the request from the client
    //     2. Get the file blocks from the FileNameToBlocks map
    //     3. For each block, get the block addresses from the BlockToDataNodeIds map
    //     4. For each block address, get the DataNodeInstance from the IdToDataNodes map
    //     5. Append the DataNodeInstance to the block addresses slice
    //     6. Append the block addresses slice to the reply
    async fn read_file(&self, request: Request<ReadFileRequest>) -> Result<Response<ReadFileResponse>, Status> {
        let response = ReadFileResponse { data: vec![] };
        Ok(Response::new(response))
    }
    // write_file Exhaustive Explanation:
    //     1. Get the request from the client
    //     2. Get the file name from the request
    //     3. Get the file size from the request
    //     4. Calculate the number of blocks to allocate
    //     5. Assign the blocks using assign_blocks_for_file
    //     6. Append the blocks to the reply
    async fn write_file(&self, request: Request<WriteFileRequest>) -> Result<Response<WriteFileResponse>, Status> {
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
        let response = PhoenixingResult { success: true, message: "Phoenixing in business".to_string(), new_nodes: vec![] };
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
        let response = AssignBlocksForFileResponse { nodes: vec![] };
        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // hdfsn -p 8080 -bs 100 -r 3 -dn localhost:8080,localhost:8081,somenonlocal:8082
    let matches = Command::new("HDFS Namenode")
        .version("0.1.0")
        .about("HDFS Namenode")
        // .arg_required_else_help(true)
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
        .add_service(NameNodeServer::new(NameNodeService::default()))
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