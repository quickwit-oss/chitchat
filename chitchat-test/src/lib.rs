use chitchat::{ClusterStateSnapshot, NodeId};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct ApiResponse {
    pub cluster_id: String,
    pub cluster_state: ClusterStateSnapshot,
    pub live_nodes: Vec<NodeId>,
    pub dead_nodes: Vec<NodeId>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SetKeyValueResponse {
    pub status: bool,
}
