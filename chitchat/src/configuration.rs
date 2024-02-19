#![allow(clippy::derive_partial_eq_without_eq)]

use std::net::SocketAddr;
use std::time::Duration;

use crate::{ChitchatId, FailureDetectorConfig};

/// A struct for configuring a Chitchat instance.
pub struct ChitchatConfig {
    pub chitchat_id: ChitchatId,
    pub cluster_id: String,
    pub gossip_interval: Duration,
    pub listen_addr: SocketAddr,
    pub seed_nodes: Vec<String>,
    pub failure_detector_config: FailureDetectorConfig,
    // Marked for deletion grace period expressed as a number of hearbeats.
    // Chitchat ensures a key marked for deletion is eventually deleted by three mechanisms:
    // - Garbage collection: each heartbeat, marked for deletion keys with `deletion now > instant
    //   + marked_for_deletion_grace_period` are deleted.
    // - Compute delta: for a given node digest, if `node_digest.heartbeat +
    //   marked_for_deletion_grace_period < node_state.heartbeat` the node is flagged "to be reset"
    //   and the delta is populated with all keys and values.
    // - Apply delta: for a node flagged "to be reset", Chitchat will remove the node state and
    //   populate a fresh new node state with the keys and values present in the delta.
    pub marked_for_deletion_grace_period: Duration,
}

impl ChitchatConfig {
    #[cfg(test)]
    pub fn for_test(port: u16) -> Self {
        let chitchat_id = ChitchatId::for_local_test(port);
        let listen_addr = chitchat_id.gossip_advertise_addr;
        Self {
            chitchat_id,
            cluster_id: "default-cluster".to_string(),
            gossip_interval: Duration::from_millis(50),
            listen_addr,
            seed_nodes: Vec::new(),
            failure_detector_config: Default::default(),
            marked_for_deletion_grace_period: Duration::from_secs(10_000),
        }
    }
}

#[cfg(test)]
impl Default for ChitchatConfig {
    fn default() -> Self {
        let chitchat_id = ChitchatId::for_local_test(10_000);
        let listen_addr = chitchat_id.gossip_advertise_addr;
        Self {
            chitchat_id,
            cluster_id: "default-cluster".to_string(),
            gossip_interval: Duration::from_millis(1_000),
            listen_addr,
            seed_nodes: Vec::new(),
            failure_detector_config: Default::default(),
            // Each heartbeat increments the version, with one heartbeat each second
            // 86400 ~ 24h.
            // TODO set that to something much lower.
            marked_for_deletion_grace_period: Duration::from_secs(86_400),
        }
    }
}
