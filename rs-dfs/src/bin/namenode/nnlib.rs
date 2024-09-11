use std::collections::HashMap;
use anyhow::Result;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use crate::proto::namenode::{
    namenode_server::NameNode, BlockSizeRequest, BlockSizeResponse, ReadFileRequest, ReadFileResponse,
    WriteFileRequest, WriteFileResponse, PhoenixingResult, NodeAddress, AssignBlocksForFileRequest,
    AssignBlocksForFileResponse,
};

pub struct NameNodeService {
    block_size: u32,
    file_name_to_blocks: RwLock<HashMap<String, Vec<String>>>,
    block_to_datanode_ids: RwLock<HashMap<String, Vec<String>>>,
    id_to_datanodes: RwLock<HashMap<String, NodeAddress>>,
}

impl NameNodeService {
    pub fn new(block_size: u32) -> Self {
        Self {
            block_size,
            file_name_to_blocks: RwLock::new(HashMap::new()),
            block_to_datanode_ids: RwLock::new(HashMap::new()),
            id_to_datanodes: RwLock::new(HashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl NameNode for NameNodeService {
    async fn block_size(
        &self,
        request: Request<BlockSizeRequest>,
    ) -> Result<Response<BlockSizeResponse>, Status> {
        let req = request.into_inner();
        if req.pulse {
            Ok(Response::new(BlockSizeResponse {
                block_size: self.block_size,
            }))
        } else {
            Err(Status::invalid_argument("Invalid request"))
        }
    }

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<ReadFileResponse>, Status> {
        let req = request.into_inner();
        let file_blocks = self.file_name_to_blocks.read().await;
        let block_ids = file_blocks.get(&req.filename).ok_or_else(|| Status::not_found("File not found"))?;
        let mut block_addresses = Vec::new();

        for block_id in block_ids {
            let datanode_ids = self.block_to_datanode_ids.read().await;
            let datanode_ids = datanode_ids.get(block_id).ok_or_else(|| Status::not_found("Block not found"))?;
            let mut addresses = Vec::new();

            for datanode_id in datanode_ids {
                let datanodes = self.id_to_datanodes.read().await;
                let datanode = datanodes.get(datanode_id).ok_or_else(|| Status::not_found("DataNode not found"))?;
                addresses.push(datanode.clone());
            }

            block_addresses.push(addresses);
        }

        Ok(Response::new(ReadFileResponse {
            data: serde_json::to_vec(&block_addresses).map_err(|e| Status::internal(e.to_string()))?,
        }))
    }

    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        let req = request.into_inner();
        let num_blocks = (req.data.len() as u32 + self.block_size - 1) / self.block_size;
        let blocks = self.assign_blocks_for_file(&req.filename, num_blocks).await?;
        Ok(Response::new(WriteFileResponse { success: true }))
    }

    async fn phoenixing(
        &self,
        request: Request<NodeAddress>,
    ) -> Result<Response<PhoenixingResult>, Status> {
        let req = request.into_inner();
        let mut id_to_datanodes = self.id_to_datanodes.write().await;
        let dead_datanode_id = id_to_datanodes.iter().find_map(|(id, addr)| {
            if addr.host == req.host && addr.port == req.port {
                Some(id.clone())
            } else {
                None
            }
        }).ok_or_else(|| Status::not_found("DataNode not found"))?;

        id_to_datanodes.remove(&dead_datanode_id);

        let mut under_replicated_blocks = Vec::new();
        let mut block_to_datanode_ids = self.block_to_datanode_ids.write().await;
        for (block_id, datanode_ids) in block_to_datanode_ids.iter_mut() {
            if datanode_ids.contains(&dead_datanode_id) {
                datanode_ids.retain(|id| id != &dead_datanode_id);
                if datanode_ids.len() < 3 {
                    under_replicated_blocks.push(block_id.clone());
                }
            }
        }

        let mut new_nodes = Vec::new();
        for block_id in under_replicated_blocks {
            let datanode_ids = block_to_datanode_ids.get_mut(&block_id).unwrap();
            let available_nodes: Vec<_> = id_to_datanodes.keys().cloned().collect();
            if available_nodes.is_empty() {
                return Err(Status::internal("No available DataNodes for re-replication"));
            }
            let new_datanode_id = available_nodes[0].clone();
            datanode_ids.push(new_datanode_id.clone());
            new_nodes.push(id_to_datanodes.get(&new_datanode_id).unwrap().clone());
        }

        Ok(Response::new(PhoenixingResult {
            success: true,
            message: "Re-replication completed".to_string(),
            new_nodes,
        }))
    }

    async fn assign_blocks_for_file(
        &self,
        request: Request<AssignBlocksForFileRequest>,
    ) -> Result<Response<AssignBlocksForFileResponse>, Status> {
        let req = request.into_inner();
        let mut file_blocks = self.file_name_to_blocks.write().await;
        file_blocks.insert(req.filename.clone(), Vec::new());

        let available_datanode_ids: Vec<_> = self.id_to_datanodes.read().await.keys().cloned().collect();
        let num_datanodes = available_datanode_ids.len();
        let num_blocks = (req.filename.len() as u32 + self.block_size - 1) / self.block_size;

        let mut block_addresses = Vec::new();
        for _ in 0..num_blocks {
            let block_id = uuid::Uuid::new_v4().to_string();
            file_blocks.get_mut(&req.filename).unwrap().push(block_id.clone());

            let replication_factor = std::cmp::min(3, num_datanodes);
            let mut assigned_datanodes = Vec::new();
            for i in 0..replication_factor {
                assigned_datanodes.push(available_datanode_ids[i % num_datanodes].clone());
            }

            let mut block_to_datanode_ids = self.block_to_datanode_ids.write().await;
            block_to_datanode_ids.insert(block_id.clone(), assigned_datanodes.clone());

            block_addresses.push(assigned_datanodes);
        }

        Ok(Response::new(AssignBlocksForFileResponse {
            nodes: block_addresses.into_iter().flatten().collect(),
        }))
    }
}
