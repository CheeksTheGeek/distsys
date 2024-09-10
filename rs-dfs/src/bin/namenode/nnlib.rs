use tarpc::context;

use rs_dfs::NodeAddress;
use std::collections::HashMap;
use rs_dfs::SerializableError;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct NameNodeInstance {
    port: u16,
    block_size: u64,
    repl_factor: u64,
    block_file_names: Vec<String>,
    data_nodes: Vec<(NodeAddress, bool)>,
    block_datanode_map: HashMap<String, NodeAddress>,
}
// NameNodeInstance constructor
pub fn new_namenode(port: u16, block_size: u64, repl_factor: u64) -> NameNodeInstance {
    NameNodeInstance {
        port,
        block_size,
        repl_factor,
        block_file_names: vec![],
        data_nodes: vec![],
        block_datanode_map: HashMap::new(),
    }
}

// add_datanode adds a datanode to the namenode
pub fn add_datanode(namenode: &mut NameNodeInstance, datanode: NodeAddress) {
    namenode.data_nodes.push((datanode, false));
}

pub fn add_datanodes(namenode: &mut NameNodeInstance, datanodes: Vec<(NodeAddress, bool)>) {
    namenode.data_nodes.extend(datanodes);
}

// knockknockDataNodes takes in the NameNodeInstance and does an rpc call to all the DataNodes 
// pub fn knockknock_data_nodes(namenode: &NameNodeInstance) 


#[tarpc::service]
trait NameNode {
    async fn register_datanodes(datanodes: Vec<(NodeAddress, bool)>) -> Result<(), SerializableError>;
}

impl NameNode for NameNodeInstance {
    async fn register_datanodes(self, _: context::Context, datanodes: Vec<(NodeAddress, bool)>) -> Result<(), SerializableError> {
        Ok(())
    }
}