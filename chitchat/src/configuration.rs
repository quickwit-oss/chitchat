#![allow(clippy::derive_partial_eq_without_eq)]

use std::net::SocketAddr;
use std::time::Duration;

use crate::{ChitchatId, FailureDetectorConfig, NodeState};

/// An optional user-defined callback executed when the self node is lagging behind.
pub type CatchupCallback = Box<dyn Fn() + Send>;

/// An optional user-defined predicate liveness predication applied on top of the output of the
/// failure detector.
pub type ExtraLivenessPredicate = Box<dyn Fn(&NodeState) -> bool + Send>;

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
    /// An optional callback executed when the self node is lagging behind. It
    /// is meant to wire up an external mechanism capable of catching up the
    /// Chitchat state faster than the Chitchat protocol. That external
    /// mechanism will call [`crate::Chitchat::reset_node_state`] to communicate
    /// the results back to Chitchat. For instance, in Quickwit, we use a GRPC
    /// API to fetch the states from other nodes in the cluster.
    pub catchup_callback: Option<CatchupCallback>,
    // Extra lifeness predicate that can be used to define what a node being "live" means.
    // It can be used for instance, to only surface the nodes that are both alive according
    // to the failure detector, but also have a given set of required keys.
    pub extra_liveness_predicate: Option<ExtraLivenessPredicate>,
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
            catchup_callback: None,
            extra_liveness_predicate: None,
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
            marked_for_deletion_grace_period: Duration::from_secs(3_600 * 2), // 2h
            catchup_callback: None,
            extra_liveness_predicate: None,
        }
    }
}
