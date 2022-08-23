#![allow(clippy::derive_partial_eq_without_eq)]

use std::net::SocketAddr;
use std::time::Duration;

use crate::state::NodeState;
use crate::{FailureDetectorConfig, NodeId};

/// A struct for configuring a Chitact instance.
pub struct ChitchatConfig {
    pub node_id: NodeId,
    pub cluster_id: String,
    pub gossip_interval: Duration,
    pub listen_addr: SocketAddr,
    pub seed_nodes: Vec<String>,
    pub failure_detector_config: FailureDetectorConfig,
    // `is_ready_predicate` makes it possible for a node to advertise itself as not "ready".
    // For instance, if it is `starting` or if it lost connection to a third-party service.
    //
    // If `None`, a node is ready as long as it is alive.
    pub is_ready_predicate: Option<Box<dyn Fn(&NodeState) -> bool + Send>>,
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
            failure_detector_config: Default::default(),
            is_ready_predicate: None,
        }
    }

    pub fn set_is_ready_predicate(&mut self, pred: impl Fn(&NodeState) -> bool + Send + 'static) {
        self.is_ready_predicate = Some(Box::new(pred));
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
            failure_detector_config: Default::default(),
            is_ready_predicate: None,
        }
    }
}
