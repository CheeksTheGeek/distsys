use tonic::transport::Server;
use datanode::data_node_server::DataNodeServer;
use dnlib::DataNodeService;
use std::env;

mod dnlib;
mod datanode {
    tonic::include_proto!("datanode");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let port = args.get(1).expect("Port not specified").parse::<u16>()?;
    let data_dir = args.get(2).expect("Data directory not specified").to_string();

    let addr = format!("[::1]:{}", port).parse()?;
    let data_node_service = DataNodeService::new(data_dir);

    println!("DataNode server listening on {}", addr);

    Server::builder()
        .add_service(DataNodeServer::new(data_node_service))
        .serve(addr)
        .await?;

    Ok(())
}
