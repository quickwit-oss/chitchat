use chitchat::{NodeId, SerializableClusterState};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct ApiResponse {
    pub cluster_id: String,
    pub cluster_state: SerializableClusterState,
    pub live_nodes: Vec<NodeId>,
    pub dead_nodes: Vec<NodeId>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SetKeyValueResponse {
    pub status: bool,
}
