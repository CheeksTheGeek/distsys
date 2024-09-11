use clap::{Arg, Command};
mod nnlib;
pub mod namenode {
    tonic::include_proto!("namenode");
}
use namenode::name_node_server::{NameNode, NameNodeServer};
use crate::nnlib::{NameNodeState, NameNodeService, SerializableNodeAddress};
use tonic::transport::Server;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::net::SocketAddr;
use std::str::FromStr;
use std::fs;
use serde_json;
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

fn parse_data_nodes(data_nodes: &str) -> Vec<(SerializableNodeAddress, bool)> {
    data_nodes.split(",").map(|node| {
        let parts: Vec<&str> = node.split(":").collect();
        (SerializableNodeAddress {
            host: parts[0].to_string(),
            port: parts[1].parse().unwrap(),
        }, false)
    }).collect()
}
