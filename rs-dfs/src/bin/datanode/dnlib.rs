use tarpc::{server, context};
use rs_dfs::NodeAddress;
use serde::{Serialize, Deserialize};
use std::error::Error;
use rs_dfs::SerializableError;

#[derive(Clone, Serialize, Deserialize)]
pub struct DataNodeInstance {
    port: u16,
    datadir: String,
}


// DataNodeInstance constructor
// pub fn new_datanode(port: u16, datadir: String, namenode: NodeAddress) -> DataNodeInstance {
pub fn new_datanode(port: u16, datadir: &str) -> DataNodeInstance {
    // DataNodeInstance { port, datadir, namenode }
    DataNodeInstance { port, datadir: datadir.to_string() }
}

impl DataNodeInstance {
    pub fn new(port: u16, datadir: String) -> Self {
        Self { port, datadir }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct HeartbeatRequest {
    namenode: NodeAddress,
}

#[derive(Debug, Serialize, Deserialize)]
struct HeartbeatResponse {
    success: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct PassItOnRequest {
    data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PassItOnResponse {
    success: bool,
}



#[tarpc::service]
trait DataNode {
    async fn heartbeat(request: HeartbeatRequest) -> Result<HeartbeatResponse, SerializableError>;
    async fn pass_it_on(request: PassItOnRequest) -> Result<PassItOnResponse, SerializableError>;
}

impl DataNode for DataNodeInstance {
    async fn heartbeat(self, _: context::Context, request: HeartbeatRequest) -> Result<HeartbeatResponse, SerializableError> {
        println!("Heartbeat request received from namenode: {:?}", request);
        Ok(HeartbeatResponse { success: true })
    }

    async fn pass_it_on(self, _: context::Context, request: PassItOnRequest) -> Result<PassItOnResponse, SerializableError> {
        println!("Pass it on request received from namenode: {:?}", request);
        Ok(PassItOnResponse { success: true })
    }
}