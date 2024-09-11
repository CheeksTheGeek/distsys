use clap::{Arg, Command};
mod dnlib;
use datanode::data_node_server::{DataNode, DataNodeServer};
use datanode::{PulseRequest, PulseResponse, GetDataRequest, GetDataResponse, PutDataRequest, PutDataResponse, ReplicationPassthroughRequest, ReplicationPassthroughResponse};
use tonic::{transport::Server, Request, Response, Status};
use std::net::SocketAddr;
use std::str::FromStr;
mod datanode {
    tonic::include_proto!("datanode");
}

#[derive(Debug, Default)]
pub struct DataNodeService {}

#[tonic::async_trait]
impl DataNode for DataNodeService {
    // Pulse is used by namenode to check if the datanode is still alive or not, it has two modes, pulse = true means that the namenode is checking if the datanode is still alive, pulse = false is for an initial ping
    //     - the datanode should respond with a PulseResponse with success = true if it is still alive or if the namenode is sending an initial ping
    //     - the datanode should respond with a PulseResponse with success = false if there's something wrong with the datanode (disk error, network error, etc.) 
    //         or if the datanode is not able to serve requests, or if the namenode is sending an initial ping and that has failed (the datanode is already registered with another namenode)
    async fn pulse(
        &self,
        request: Request<PulseRequest>,
    ) -> Result<Response<PulseResponse>, Status> {
        let response = PulseResponse { success: true };
        Ok(Response::new(response))
    }

    // GetData is used by namenode to get a block of data from the datanode
    async fn get_data(
        &self,
        request: Request<GetDataRequest>,
    ) -> Result<Response<GetDataResponse>, Status> {
        Ok(Response::new(GetDataResponse::default()))
    }

    // PutData is used by namenode to put a block of data to the datanode
    async fn put_data(
        &self,
        request: Request<PutDataRequest>,
    ) -> Result<Response<PutDataResponse>, Status> {
        Ok(Response::new(PutDataResponse::default()))
    }

    // ReplicationPassthrough is used by namenode to replicate a block of data to another datanode
    async fn replication_passthrough(
        &self,
        request: Request<ReplicationPassthroughRequest>,
    ) -> Result<Response<ReplicationPassthroughResponse>, Status> {
        Ok(Response::new(ReplicationPassthroughResponse::default()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let matches = Command::new("HDFS Datanode")
        .version("0.1.0")
        .about("HDFS Datanode")
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

    let port = matches.get_one::<String>("port").map(String::as_str).unwrap_or("8080");
    let datadir = matches.get_one::<String>("datadir").map(String::as_str).unwrap_or("data");

    let addr = format!("0.0.0.0:{}", port);
    let datanode: DataNodeService = DataNodeService::default();

    let addr = SocketAddr::from_str(&addr).unwrap();
    println!("DataNode server starting on {}", addr);
    println!("Waiting for incoming requests...");

    let server = Server::builder()
        .add_service(DataNodeServer::new(datanode))
        .serve(addr);

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