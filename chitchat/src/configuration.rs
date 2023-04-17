#![allow(clippy::derive_partial_eq_without_eq)]

use std::net::SocketAddr;
use std::time::Duration;

use crate::state::NodeState;
use crate::{ChitchatId, FailureDetectorConfig};

/// A struct for configuring a Chitchat instance.
pub struct ChitchatConfig {
    pub chitchat_id: ChitchatId,
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
    // Marked for deletion grace period expressed as a number of version threshold.
    // Chitchat ensures a marked for deletion key is eventually deleted by three mecanisms:
    // - Garbage collection: each heartbeat, marked for deletion keys with `key_version +
    //   marked_for_deletion_grace_period < node.max_version` are deleted.
    // - Compute delta: for a given node, if `digest_node_max_version +
    //   marked_for_deletion_grace_period < node_max_version`, the node is flagged "to be reset"
    //   and the delta is populated with all keys and values.
    // - Apply delta: for a node flagged "to be reset", Chitchat will remove the node state and
    //   populate a fresh new node state with the keys and values present in the delta.
    pub marked_for_deletion_grace_period: usize,
}

impl ChitchatConfig {
    pub fn set_is_ready_predicate(&mut self, pred: impl Fn(&NodeState) -> bool + Send + 'static) {
        self.is_ready_predicate = Some(Box::new(pred));
    }

    #[cfg(test)]
    pub fn for_test(port: u16) -> Self {
        let chitchat_id = ChitchatId::for_local_test(port);
        let listen_addr = chitchat_id.gossip_advertise_address;
        Self {
            chitchat_id,
            cluster_id: "default-cluster".to_string(),
            gossip_interval: Duration::from_millis(50),
            listen_addr,
            seed_nodes: Vec::new(),
            failure_detector_config: Default::default(),
            is_ready_predicate: None,
            marked_for_deletion_grace_period: 10_000,
        }
    }
}

#[cfg(test)]
impl Default for ChitchatConfig {
    fn default() -> Self {
        let chitchat_id = ChitchatId::for_local_test(10_000);
        let listen_addr = chitchat_id.gossip_advertise_address;
        Self {
            chitchat_id,
            cluster_id: "default-cluster".to_string(),
            gossip_interval: Duration::from_millis(1_000),
            listen_addr,
            seed_nodes: Vec::new(),
            failure_detector_config: Default::default(),
            is_ready_predicate: None,
            // Each heartbeat increments the version, with one heartbeat each second
            // 43200 ~ 12h.
            marked_for_deletion_grace_period: 43200,
        }
    }
}
