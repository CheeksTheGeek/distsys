use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use crate::proto::namenode::{
    namenode_server::NameNode, BlockSizeRequest, BlockSizeResponse, ReadFileRequest, ReadFileResponse,
    WriteFileRequest, WriteFileResponse, PhoenixingResult, NodeAddress, AssignBlocksForFileRequest,
    AssignBlocksForFileResponse,
};
use crate::nn::NameNodeService;

#[tokio::test]
async fn test_block_size() {
    let namenode = NameNodeService::new(1024);
    let request = Request::new(BlockSizeRequest { pulse: true });
    let response = namenode.block_size(request).await.unwrap();
    assert_eq!(response.into_inner().block_size, 1024);

    let request = Request::new(BlockSizeRequest { pulse: false });
    let response = namenode.block_size(request).await;
    assert!(response.is_err());
}

#[tokio::test]
async fn test_read_file() {
    let namenode = NameNodeService::new(1024);
    let mut file_name_to_blocks = namenode.file_name_to_blocks.write().await;
    file_name_to_blocks.insert("testfile".to_string(), vec!["block1".to_string()]);
    drop(file_name_to_blocks);

    let mut block_to_datanode_ids = namenode.block_to_datanode_ids.write().await;
    block_to_datanode_ids.insert("block1".to_string(), vec!["datanode1".to_string()]);
    drop(block_to_datanode_ids);

    let mut id_to_datanodes = namenode.id_to_datanodes.write().await;
    id_to_datanodes.insert("datanode1".to_string(), NodeAddress { host: "localhost".to_string(), port: 8080 });
    drop(id_to_datanodes);

    let request = Request::new(ReadFileRequest { filename: "testfile".to_string() });
    let response = namenode.read_file(request).await.unwrap();
    let block_addresses: Vec<Vec<NodeAddress>> = serde_json::from_slice(&response.into_inner().data).unwrap();
    assert_eq!(block_addresses.len(), 1);
    assert_eq!(block_addresses[0].len(), 1);
    assert_eq!(block_addresses[0][0].host, "localhost");
    assert_eq!(block_addresses[0][0].port, 8080);
}

#[tokio::test]
async fn test_write_file() {
    let namenode = NameNodeService::new(1024);
    let request = Request::new(WriteFileRequest {
        filename: "testfile".to_string(),
        data: vec![0; 2048],
        nodes_left: vec![],
    });
    let response = namenode.write_file(request).await.unwrap();
    assert!(response.into_inner().success);
}

#[tokio::test]
async fn test_phoenixing() {
    let namenode = NameNodeService::new(1024);
    let mut id_to_datanodes = namenode.id_to_datanodes.write().await;
    id_to_datanodes.insert("datanode1".to_string(), NodeAddress { host: "localhost".to_string(), port: 8080 });
    drop(id_to_datanodes);

    let mut block_to_datanode_ids = namenode.block_to_datanode_ids.write().await;
    block_to_datanode_ids.insert("block1".to_string(), vec!["datanode1".to_string()]);
    drop(block_to_datanode_ids);

    let request = Request::new(NodeAddress { host: "localhost".to_string(), port: 8080 });
    let response = namenode.phoenixing(request).await.unwrap();
    assert!(response.into_inner().success);
}

#[tokio::test]
async fn test_assign_blocks_for_file() {
    let namenode = NameNodeService::new(1024);
    let mut id_to_datanodes = namenode.id_to_datanodes.write().await;
    id_to_datanodes.insert("datanode1".to_string(), NodeAddress { host: "localhost".to_string(), port: 8080 });
    drop(id_to_datanodes);

    let request = Request::new(AssignBlocksForFileRequest { filename: "testfile".to_string() });
    let response = namenode.assign_blocks_for_file(request).await.unwrap();
    let nodes = response.into_inner().nodes;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0], "datanode1");
}
