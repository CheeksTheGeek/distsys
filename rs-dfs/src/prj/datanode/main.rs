use clap::{Arg, Command};
use tonic::transport::Server;
use crate::datanode::data_node_server::DataNodeServer;
use crate::datanode::data_node_client::DataNodeClient;
mod dnlib;
use dnlib::DataNodeService;
use std::net::SocketAddr;
use std::str::FromStr;
mod datanode {
    tonic::include_proto!("datanode");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let matches = Command::new("DFS Datanode")
        .version("0.1.0")
        .about("DFS Datanode")
        // .arg_required_else_help(true)
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .help("Sets the port to listen on")
        )
        .arg(
            Arg::new("datadir")
                .short('d')
                .long("datadir")
                .value_name("DATADIR")
                .help("Sets the datadir")
        )
        .get_matches();

    let port = matches.get_one::<String>("port").map(String::as_str).unwrap_or("4210");
    let datadir = matches.get_one::<String>("datadir").map(String::as_str).unwrap_or("data");

    let addr = format!("0.0.0.0:{}", port);
    let datanode: DataNodeService = DataNodeService::new(datadir.to_string());

    let addr = SocketAddr::from_str(&addr).unwrap();
    println!("DataNode server starting on {}", addr);
    println!("Waiting for incoming requests...");

    let server = Server::builder()
        .add_service(DataNodeServer::new(datanode))
        .serve(addr);

    // Removed the periodic printing as it's not needed for a daemon

    match server.await {
        Ok(_) => println!("Server shut down gracefully"),
        Err(e) => println!("Server error: {}", e),
    }

    Ok(())
}