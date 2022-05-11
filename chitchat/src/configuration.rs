use std::net::SocketAddr;
use std::time::Duration;

use crate::{FailureDetectorConfig, NodeId};

pub struct ChitchatConfig {
    pub node_id: NodeId,
    pub cluster_id: String,
    pub gossip_interval: Duration,
    pub listen_addr: SocketAddr,
    pub seed_nodes: Vec<String>,
    pub mtu: usize,
    pub failure_detector_config: FailureDetectorConfig,
}

impl ChitchatConfig {
    #[cfg(test)]
    pub fn for_test(port: u16) -> Self {
        let node_id = NodeId::for_test_localhost(port);
        let listen_addr = node_id.gossip_public_address;
        Self {
            node_id,
            cluster_id: "default-cluster".to_string(),
            gossip_interval: Duration::from_millis(50),
            listen_addr,
            seed_nodes: Vec::new(),
            mtu: 60_000,
            failure_detector_config: Default::default(),
        }
    }
}

impl Default for ChitchatConfig {
    fn default() -> Self {
        let node_id = NodeId::for_test_localhost(10_000);
        let listen_addr = node_id.gossip_public_address;
        Self {
            node_id,
            cluster_id: "default-cluster".to_string(),
            gossip_interval: Duration::from_millis(1_000),
            listen_addr,
            seed_nodes: Vec::new(),
            mtu: 60_000,
            failure_detector_config: Default::default(),
        }
    }
}
