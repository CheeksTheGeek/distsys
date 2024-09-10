use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::sync::Arc;
use tarpc::context;
use tarpc::server::{self, Channel};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Datanode {
    data: Arc<Mutex<Vec<u8>>>,
}

#[tarpc::service]
pub trait DatanodeService {
    async fn pulse(ctx: context::Context, message: String) -> String;
    async fn get_data(ctx: context::Context, filename: String) -> Result<Vec<u8>, String>;
    async fn put_data(ctx: context::Context, block_id: String, data: Vec<u8>, nodes_left: Vec<String>) -> Result<(), String>;
    async fn replication_passthrough(ctx: context::Context, block_id: String, data: Vec<u8>, nodes_left: Vec<String>) -> Result<(), String>;
}

impl DatanodeService for Datanode {
    async fn pulse(self, _: context::Context, message: String) -> String {
        println!("Received pulse from: {}", message);
        "Acknowledged".to_string()
    }

    async fn get_data(self, _: context::Context, filename: String) -> Result<Vec<u8>, String> {
        let mut file = match File::open(&filename) {
            Ok(file) => file,
            Err(err) => return Err(err.to_string()),
        };
        let mut data = Vec::new();
        if let Err(err) = file.read_to_end(&mut data) {
            return Err(err.to_string());
        }
        Ok(data)
    }

    async fn put_data(self, _: context::Context, block_id: String, data: Vec<u8>, nodes_left: Vec<String>) -> Result<(), String> {
        let filename = format!("{}.block", block_id);
        let mut file = match OpenOptions::new().write(true).create(true).open(&filename) {
            Ok(file) => file,
            Err(err) => return Err(err.to_string()),
        };
        if let Err(err) = file.write_all(&data) {
            return Err(err.to_string());
        }
        if !nodes_left.is_empty() {
            let next_node = nodes_left[0].clone();
            let remaining_nodes = nodes_left[1..].to_vec();
            let client = DatanodeServiceClient::new(tarpc::serde_transport::tcp::connect(next_node, tarpc::serde_transport::formats::Json::default).await.unwrap()).spawn();
            client.put_data(context::current(), block_id, data, remaining_nodes).await.unwrap();
        }
        Ok(())
    }

    async fn replication_passthrough(self, _: context::Context, block_id: String, data: Vec<u8>, nodes_left: Vec<String>) -> Result<(), String> {
        if !nodes_left.is_empty() {
            let next_node = nodes_left[0].clone();
            let remaining_nodes = nodes_left[1..].to_vec();
            let client = DatanodeServiceClient::new(tarpc::serde_transport::tcp::connect(next_node, tarpc::serde_transport::formats::Json::default).await.unwrap()).spawn();
            client.put_data(context::current(), block_id, data, remaining_nodes).await.unwrap();
        }
        Ok(())
    }
}

pub async fn start_server(addr: &str) -> anyhow::Result<()> {
    let data = Arc::new(Mutex::new(Vec::new()));
    let server = Datanode { data };
    let transport = tarpc::serde_transport::tcp::listen(addr, tarpc::serde_transport::formats::Json::default).await?;
    transport
        .filter_map(|r| async { r.ok() })
        .map(server.serve)
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;
    Ok(())
}
