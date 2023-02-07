#![allow(clippy::type_complexity)]
#![allow(clippy::derive_partial_eq_without_eq)]

mod server;

mod configuration;
mod delta;
mod digest;
mod failure_detector;
mod message;
pub(crate) mod serialize;
mod state;
pub mod transport;

use std::collections::HashSet;
use std::net::SocketAddr;

use delta::Delta;
use failure_detector::FailureDetector;
pub use failure_detector::FailureDetectorConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tracing::{debug, error, warn};

pub use self::configuration::ChitchatConfig;
pub use self::state::{ClusterStateSnapshot, NodeState};
use crate::digest::Digest;
use crate::message::syn_ack_serialized_len;
pub use crate::message::ChitchatMessage;
pub use crate::server::{spawn_chitchat, ChitchatHandle};
use crate::state::ClusterState;

/// Map key for the heartbeat node value.
pub(crate) const HEARTBEAT_KEY: &str = "heartbeat";

/// Maximum UDP datagram payload size (in bytes).
///
/// Note that 65KB typically won't fit in a single IP packet,
/// so long messages will be sent over several IP fragments of MTU size.
///
/// We pick a large payload size because at the moment because
/// we send the self digest "in full".
/// An Ethernet frame size of 1400B would limit us to 20 nodes
/// or so.
const MAX_UDP_DATAGRAM_PAYLOAD_SIZE: usize = 65_507;

pub type Version = u64;

/// [`NodeId`] represents a Chitchat Node identifier.
///
/// For the lifetime of a cluster, nodes can go down and back up, they may
/// permanently die. These are couple of issues we want to solve with [`NodeId`] struct:
/// - We want a fresh local chitchat state for every run of a node.
/// - We don’t want other nodes to override a newly started node state with an obsolete state.
/// - We want other running nodes to detect that a newly started node’s state prevails all its
///   previous state.
/// - We want a node to advertise its own gossip address.
/// - We want a node to have an id that is the same across subsequent runs for keeping cache data
///   around as long as possible.
///
/// Our solution to this is:
/// - The `id` attribute which represents the node's unique identifier in the cluster should be
///   dynamic on every run. This easily solves our first three requirements. The tradeoff is that
///   starting node need to always propagate their fresh state and old states are never reclaimed.
/// - Having `gossip_public_address` attribute fulfils our fourth requirements, its value is
///   expected to be from a config item or an environnement variable.
/// - Making part of the `id` attribute static and related to the node solves the last requirement.
///
/// Because Chitchat instance is not concerned about caching strategy and what needs to be
/// cached, We let the client decide what makes up the `id` attribute and how to extract its
/// components.
///
/// One such client is Quickwit where the `id` is made of
/// `{node_unique_id}/{node_generation}/`.
/// - node_unique_id: a static unique name for the node.
/// - node_generation: a monotonically increasing value (timestamp on every run)
/// More details at <https://github.com/quickwit-oss/chitchat/issues/1#issuecomment-1059029051>
///
/// Note: using timestamp to make the `id` dynamic has the potential of reusing
/// a previously used `id` in cases where the clock is reset in the past. We believe this
/// very rare and things should just work fine.
#[derive(Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct NodeId {
    // The unique identifier of this node in the cluster.
    pub id: String,
    // The SocketAddr other peers should use to communicate.
    pub gossip_public_address: SocketAddr,
}

impl NodeId {
    pub fn new(id: String, gossip_public_address: SocketAddr) -> Self {
        Self {
            id,
            gossip_public_address,
        }
    }

    pub fn for_test_localhost(port: u16) -> Self {
        NodeId::new(
            format!("node-{port}"),
            ([127u8, 0u8, 0u8, 1u8], port).into(),
        )
    }

    /// Returns the gossip public port. Useful for test assert only.
    #[cfg(test)]
    pub fn public_port(&self) -> u16 {
        self.gossip_public_address.port()
    }
}

/// A versioned value for a given Key-value pair.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct VersionedValue {
    pub value: String,
    pub version: Version,
    pub marked_for_deletion: bool,
}

pub struct Chitchat {
    config: ChitchatConfig,
    cluster_state: ClusterState,
    heartbeat: u64,
    /// The failure detector instance.
    failure_detector: FailureDetector,
    /// A notification channel (sender) for sending live nodes change feed.
    ready_nodes_watcher_tx: watch::Sender<HashSet<NodeId>>,
    /// A notification channel (receiver) for receiving `ready` nodes change feed.
    ready_nodes_watcher_rx: watch::Receiver<HashSet<NodeId>>,
}

impl Chitchat {
    pub fn with_node_id_and_seeds(
        config: ChitchatConfig,
        seed_addrs: watch::Receiver<HashSet<SocketAddr>>,
        initial_key_values: Vec<(String, String)>,
    ) -> Self {
        let (ready_nodes_watcher_tx, ready_nodes_watcher_rx) = watch::channel(HashSet::new());
        let failure_detector = FailureDetector::new(config.failure_detector_config.clone());
        let mut chitchat = Chitchat {
            config,
            cluster_state: ClusterState::with_seed_addrs(seed_addrs),
            heartbeat: 0,
            failure_detector,
            ready_nodes_watcher_tx,
            ready_nodes_watcher_rx,
        };

        let self_node_state = chitchat.self_node_state();

        // Immediately mark node as alive to ensure it responds to SYNs.
        self_node_state.set(HEARTBEAT_KEY, 0);

        // Set initial key/value pairs.
        for (key, value) in initial_key_values {
            self_node_state.set(key, value);
        }

        chitchat
    }

    pub(crate) fn create_syn_message(&self) -> ChitchatMessage {
        let dead_nodes: HashSet<_> = self.dead_nodes().collect();
        let digest = self.compute_digest(&dead_nodes);
        ChitchatMessage::Syn {
            cluster_id: self.config.cluster_id.clone(),
            digest,
        }
    }

    pub(crate) fn process_message(&mut self, msg: ChitchatMessage) -> Option<ChitchatMessage> {
        match msg {
            ChitchatMessage::Syn { cluster_id, digest } => {
                if cluster_id != self.config.cluster_id {
                    warn!(
                        cluster_id = %cluster_id,
                        "rejecting syn message with mismatching cluster name"
                    );
                    return Some(ChitchatMessage::BadCluster);
                }
                // Ensure for every reply from this node, at least the heartbeat is changed.
                let dead_nodes: HashSet<_> = self.dead_nodes().collect();
                let self_digest = self.compute_digest(&dead_nodes);
                let empty_delta = Delta::default();
                let delta_mtu = MAX_UDP_DATAGRAM_PAYLOAD_SIZE
                    - syn_ack_serialized_len(&self_digest, &empty_delta);
                let delta = self.cluster_state.compute_delta(
                    &digest,
                    delta_mtu,
                    dead_nodes,
                    self.config.marked_for_deletion_grace_period,
                );
                self.report_to_failure_detector(&delta);
                Some(ChitchatMessage::SynAck {
                    digest: self_digest,
                    delta,
                })
            }
            ChitchatMessage::SynAck { digest, delta } => {
                self.report_to_failure_detector(&delta);
                self.cluster_state.apply_delta(delta);
                let dead_nodes = self.dead_nodes().collect::<HashSet<_>>();
                let delta = self.cluster_state.compute_delta(
                    &digest,
                    MAX_UDP_DATAGRAM_PAYLOAD_SIZE - 1,
                    dead_nodes,
                    self.config.marked_for_deletion_grace_period,
                );
                Some(ChitchatMessage::Ack { delta })
            }
            ChitchatMessage::Ack { delta } => {
                self.report_to_failure_detector(&delta);
                self.cluster_state.apply_delta(delta);
                None
            }
            ChitchatMessage::BadCluster => {
                warn!("message rejected by peer: cluster name mismatch");
                None
            }
        }
    }

    fn gc_keys_marked_for_deletion(&mut self) {
        let dead_nodes = self.dead_nodes().cloned().collect::<HashSet<_>>();
        self.cluster_state
            .gc_keys_marked_for_deletion(self.config.marked_for_deletion_grace_period, &dead_nodes);
    }

    fn report_to_failure_detector(&mut self, delta: &Delta) {
        for (node_id, node_delta) in &delta.node_deltas {
            let local_max_version = self
                .cluster_state
                .node_states
                .get(node_id)
                .map(|node_state| node_state.max_version)
                .unwrap_or(0);

            let delta_max_version = node_delta.max_version();
            if local_max_version < delta_max_version {
                self.failure_detector.report_heartbeat(node_id);
            }
        }
    }

    /// Checks and marks nodes as dead / live / ready.
    pub(crate) fn update_nodes_liveliness(&mut self) {
        let cluster_nodes = self
            .cluster_state
            .nodes()
            .filter(|&node_id| node_id != self.self_node_id())
            .collect::<Vec<_>>();
        for &node_id in &cluster_nodes {
            self.failure_detector.update_node_liveliness(node_id);
        }

        let ready_nodes_before = self.ready_nodes_watcher_rx.borrow().clone();
        let ready_nodes_after = self.ready_nodes().cloned().collect::<HashSet<_>>();

        if ready_nodes_before != ready_nodes_after {
            debug!(current_node = ?self.self_node_id(), live_nodes = ?ready_nodes_after, "nodes status changed");
            if self.ready_nodes_watcher_tx.send(ready_nodes_after).is_err() {
                error!(current_node = ?self.self_node_id(), "error while reporting membership change event.")
            }
        }

        // Perform garbage collection.
        let garbage_collected_nodes = self.failure_detector.garbage_collect();
        for node_id in garbage_collected_nodes.iter() {
            self.cluster_state.remove_node(node_id);
        }
    }

    pub fn node_state(&self, node_id: &NodeId) -> Option<&NodeState> {
        self.cluster_state.node_state(node_id)
    }

    pub fn self_node_state(&mut self) -> &mut NodeState {
        self.cluster_state.node_state_mut(&self.config.node_id)
    }

    /// Retrieves the list of all live nodes.
    pub fn live_nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.failure_detector.live_nodes()
    }

    /// Retrieves the list of nodes that are ready.
    /// To be ready, a node has to be alive and pass the `is_ready_predicate` as
    /// defined in the Chitchat configuration.
    pub fn ready_nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.live_nodes().filter(|&node_id| {
            let is_ready_pred = if let Some(pred) = self.config.is_ready_predicate.as_ref() {
                pred
            } else {
                // No predicate means that we consider all nodes as ready.
                return true;
            };
            self.node_state(node_id).map(is_ready_pred).unwrap_or(false)
        })
    }

    /// Retrieve the list of all dead nodes.
    pub fn dead_nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.failure_detector.dead_nodes()
    }

    /// Retrieve a list of seed nodes.
    pub fn seed_nodes(&self) -> HashSet<SocketAddr> {
        self.cluster_state.seed_addrs()
    }

    pub fn self_node_id(&self) -> &NodeId {
        &self.config.node_id
    }

    pub fn cluster_id(&self) -> &str {
        &self.config.cluster_id
    }

    pub fn update_heartbeat(&mut self) {
        self.heartbeat += 1;
        let heartbeat = self.heartbeat;
        self.self_node_state().set(HEARTBEAT_KEY, heartbeat);
    }

    /// Computes digest.
    ///
    /// This method also increments the heartbeat, to force the presence
    /// of at least one update, and have the node liveliness propagated
    /// through the cluster.
    fn compute_digest(&self, dead_nodes: &HashSet<&NodeId>) -> Digest {
        self.cluster_state.compute_digest(dead_nodes)
    }

    pub(crate) fn cluster_state(&self) -> &ClusterState {
        &self.cluster_state
    }

    /// Returns a serializable snapshot of the ClusterState
    pub fn state_snapshot(&self) -> ClusterStateSnapshot {
        ClusterStateSnapshot::from(&self.cluster_state)
    }

    /// Returns a watch stream for monitoring changes on the cluster's live nodes.
    pub fn ready_nodes_watcher(&self) -> WatchStream<HashSet<NodeId>> {
        WatchStream::new(self.ready_nodes_watcher_rx.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Add, RangeInclusive};
    use std::sync::Arc;
    use std::time::Duration;

    use mock_instant::MockClock;
    use tokio::sync::Mutex;
    use tokio::time;
    use tokio_stream::wrappers::IntervalStream;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::server::{spawn_chitchat, ChitchatHandle};
    use crate::transport::{ChannelTransport, Transport};

    const DEAD_NODE_GRACE_PERIOD: Duration = Duration::from_secs(20);

    fn run_chitchat_handshake(initiating_node: &mut Chitchat, peer_node: &mut Chitchat) {
        let syn_message = initiating_node.create_syn_message();
        let syn_ack_message = peer_node.process_message(syn_message).unwrap();
        let ack_message = initiating_node.process_message(syn_ack_message).unwrap();
        assert!(peer_node.process_message(ack_message).is_none());
    }

    fn assert_cluster_state_eq(lhs: &NodeState, rhs: &NodeState) {
        assert_eq!(lhs.key_values.len(), rhs.key_values.len());
        for (key, value) in &lhs.key_values {
            if key == HEARTBEAT_KEY {
                // we ignore the heartbeat key
                continue;
            }
            assert_eq!(rhs.key_values.get(key), Some(value));
        }
    }

    fn assert_nodes_sync(nodes: &[&Chitchat]) {
        let first_node_states = &nodes[0].cluster_state.node_states;
        for other_node in nodes.iter().skip(1) {
            let node_states = &other_node.cluster_state.node_states;
            assert_eq!(first_node_states.len(), node_states.len());
            for (key, value) in first_node_states {
                assert_cluster_state_eq(value, node_states.get(key).unwrap());
            }
        }
    }

    async fn start_node(
        node_id: NodeId,
        seeds: &[String],
        transport: &dyn Transport,
    ) -> ChitchatHandle {
        let config = ChitchatConfig {
            node_id: node_id.clone(),
            cluster_id: "default-cluster".to_string(),
            gossip_interval: Duration::from_millis(100),
            listen_addr: node_id.gossip_public_address,
            seed_nodes: seeds.to_vec(),
            failure_detector_config: FailureDetectorConfig {
                dead_node_grace_period: DEAD_NODE_GRACE_PERIOD,
                phi_threshold: 5.0,
                initial_interval: Duration::from_millis(100),
                ..Default::default()
            },
            is_ready_predicate: None,
            marked_for_deletion_grace_period: 10_000,
        };
        let initial_kvs: Vec<(String, String)> = Vec::new();
        spawn_chitchat(config, initial_kvs, transport)
            .await
            .unwrap()
    }

    async fn setup_nodes(
        port_range: RangeInclusive<u16>,
        transport: &dyn Transport,
    ) -> Vec<ChitchatHandle> {
        let node_ids: Vec<NodeId> = port_range.map(NodeId::for_test_localhost).collect();
        let node_without_seed = start_node(node_ids[0].clone(), &[], transport).await;
        let mut chitchat_handlers: Vec<ChitchatHandle> = vec![node_without_seed];
        for node_id in &node_ids[1..] {
            let seeds = node_ids
                .iter()
                .filter(|&peer_id| peer_id != node_id)
                .map(|peer_id| peer_id.gossip_public_address.to_string())
                .collect::<Vec<_>>();
            chitchat_handlers.push(start_node(node_id.clone(), &seeds, transport).await);
        }
        // Make sure the failure detector's fake clock moves forward.
        tokio::spawn(async {
            let mut ticker = IntervalStream::new(time::interval(Duration::from_millis(50)));
            while ticker.next().await.is_some() {
                MockClock::advance(Duration::from_millis(50));
            }
        });
        chitchat_handlers
    }

    async fn shutdown_nodes(nodes: Vec<ChitchatHandle>) -> anyhow::Result<()> {
        for node in nodes {
            node.shutdown().await?;
        }
        Ok(())
    }

    async fn wait_for_chitchat_state(
        chitchat: Arc<Mutex<Chitchat>>,
        expected_node_count: usize,
        expected_nodes: &[NodeId],
    ) {
        let mut ready_nodes_watcher = chitchat
            .lock()
            .await
            .ready_nodes_watcher()
            .skip_while(|live_nodes| live_nodes.len() != expected_node_count);
        tokio::time::timeout(Duration::from_secs(50), async move {
            let live_nodes = ready_nodes_watcher.next().await.unwrap();
            assert_eq!(
                live_nodes,
                expected_nodes.iter().cloned().collect::<HashSet<_>>()
            );
        })
        .await
        .unwrap();
    }

    #[test]
    fn test_chitchat_handshake() {
        let node_config1 = ChitchatConfig::for_test(10_001);
        let empty_seeds = watch::channel(Default::default()).1;
        let mut node1 = Chitchat::with_node_id_and_seeds(
            node_config1,
            empty_seeds.clone(),
            vec![
                ("key1a".to_string(), "1".to_string()),
                ("key2a".to_string(), "2".to_string()),
            ],
        );
        let node_config2 = ChitchatConfig::for_test(10_002);
        let mut node2 = Chitchat::with_node_id_and_seeds(
            node_config2,
            empty_seeds,
            vec![
                ("key1b".to_string(), "1".to_string()),
                ("key2b".to_string(), "2".to_string()),
            ],
        );
        run_chitchat_handshake(&mut node1, &mut node2);
        assert_nodes_sync(&[&node1, &node2]);
        // useless handshake
        run_chitchat_handshake(&mut node1, &mut node2);
        assert_nodes_sync(&[&node1, &node2]);
        {
            let state1 = node1.self_node_state();
            state1.set("key1a", "3");
            state1.set("key1c", "4");
        }
        run_chitchat_handshake(&mut node1, &mut node2);
        assert_nodes_sync(&[&node1, &node2]);
    }

    #[tokio::test]
    async fn test_multiple_nodes() -> anyhow::Result<()> {
        let transport = ChannelTransport::default();
        let nodes = setup_nodes(20001..=20005, &transport).await;

        let node = nodes.get(1).unwrap();
        assert_eq!(node.node_id().public_port(), 20002);
        wait_for_chitchat_state(
            node.chitchat(),
            4,
            &[
                NodeId::for_test_localhost(20001),
                NodeId::for_test_localhost(20003),
                NodeId::for_test_localhost(20004),
                NodeId::for_test_localhost(20005),
            ],
        )
        .await;

        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_node_goes_from_live_to_down_to_live() -> anyhow::Result<()> {
        let transport = ChannelTransport::default();
        let mut nodes = setup_nodes(30001..=30006, &transport).await;
        let node = &nodes[1];
        assert_eq!(node.node_id().gossip_public_address.port(), 30002);
        wait_for_chitchat_state(
            node.chitchat(),
            5,
            &[
                NodeId::for_test_localhost(30001),
                NodeId::for_test_localhost(30003),
                NodeId::for_test_localhost(30004),
                NodeId::for_test_localhost(30005),
                NodeId::for_test_localhost(30006),
            ],
        )
        .await;

        // Take down node at localhost:30003
        let node = nodes.remove(2);
        assert_eq!(node.node_id().public_port(), 30003);
        node.shutdown().await.unwrap();

        let node = nodes.get(1).unwrap();
        assert_eq!(node.node_id().public_port(), 30002);
        wait_for_chitchat_state(
            node.chitchat(),
            4,
            &[
                NodeId::for_test_localhost(30001),
                NodeId::for_test_localhost(30004),
                NodeId::for_test_localhost(30005),
                NodeId::for_test_localhost(30006),
            ],
        )
        .await;

        // Restart node at localhost:10003
        let node_3 = NodeId::for_test_localhost(30003);
        nodes.push(
            start_node(
                node_3,
                &[NodeId::for_test_localhost(30_001)
                    .gossip_public_address
                    .to_string()],
                &transport,
            )
            .await,
        );

        let node = nodes.get(1).unwrap();
        assert_eq!(node.node_id().public_port(), 30002);
        wait_for_chitchat_state(
            node.chitchat(),
            5,
            &[
                NodeId::for_test_localhost(30001),
                NodeId::for_test_localhost(30003),
                NodeId::for_test_localhost(30004),
                NodeId::for_test_localhost(30005),
                NodeId::for_test_localhost(30006),
            ],
        )
        .await;

        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_dead_node_should_not_be_gossiped_when_node_joins() -> anyhow::Result<()> {
        let transport = ChannelTransport::default();
        let mut nodes = setup_nodes(40001..=40004, &transport).await;
        {
            let node2 = nodes.get(1).unwrap();
            assert_eq!(node2.node_id().public_port(), 40002);
            wait_for_chitchat_state(
                node2.chitchat(),
                3,
                &[
                    NodeId::for_test_localhost(40001),
                    NodeId::for_test_localhost(40003),
                    NodeId::for_test_localhost(40004),
                ],
            )
            .await;
        }

        // Take down node at localhost:40003
        let node3 = nodes.remove(2);
        assert_eq!(node3.node_id().public_port(), 40003);
        node3.shutdown().await.unwrap();

        {
            let node2 = nodes.get(1).unwrap();
            assert_eq!(node2.node_id().public_port(), 40002);
            wait_for_chitchat_state(
                node2.chitchat(),
                2,
                &[
                    NodeId::for_test_localhost(40_001),
                    NodeId::for_test_localhost(40_004),
                ],
            )
            .await;
        }

        // Restart node at localhost:40003 with new name
        let mut new_config = ChitchatConfig::for_test(40_003);

        new_config.node_id.id = "new_node".to_string();
        let seed = NodeId::for_test_localhost(40_002).gossip_public_address;
        new_config.seed_nodes = vec![seed.to_string()];
        let new_node_chitchat = spawn_chitchat(new_config, Vec::new(), &transport)
            .await
            .unwrap();

        wait_for_chitchat_state(
            new_node_chitchat.chitchat(),
            3,
            &[
                NodeId::for_test_localhost(40_001),
                NodeId::for_test_localhost(40_002),
                NodeId::for_test_localhost(40_004),
            ],
        )
        .await;

        nodes.push(new_node_chitchat);
        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_network_partition_nodes() -> anyhow::Result<()> {
        let transport = ChannelTransport::default();
        let port_range = 11_001u16..=11_006;
        let nodes = setup_nodes(port_range.clone(), &transport).await;

        // Check nodes know each other.
        for node in nodes.iter() {
            let expected_peers: Vec<NodeId> = port_range
                .clone()
                .filter(|peer_port| *peer_port != node.node_id().public_port())
                .map(NodeId::for_test_localhost)
                .collect::<Vec<_>>();
            wait_for_chitchat_state(node.chitchat(), 5, &expected_peers).await;
        }

        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_dead_node_garbage_collection() -> anyhow::Result<()> {
        let transport = ChannelTransport::default();
        let mut nodes = setup_nodes(60001..=60006, &transport).await;
        let node = nodes.get(1).unwrap();
        assert_eq!(node.node_id().public_port(), 60002);
        wait_for_chitchat_state(
            node.chitchat(),
            5,
            &[
                NodeId::for_test_localhost(60_001),
                NodeId::for_test_localhost(60_003),
                NodeId::for_test_localhost(60_004),
                NodeId::for_test_localhost(60_005),
                NodeId::for_test_localhost(60_006),
            ],
        )
        .await;

        // Take down node at localhost:60003
        let node = nodes.remove(2);
        assert_eq!(node.node_id().public_port(), 60003);
        node.shutdown().await.unwrap();

        let node = nodes.get(1).unwrap();
        assert_eq!(node.node_id().public_port(), 60002);
        wait_for_chitchat_state(
            node.chitchat(),
            4,
            &[
                NodeId::for_test_localhost(60_001),
                NodeId::for_test_localhost(60_004),
                NodeId::for_test_localhost(60_005),
                NodeId::for_test_localhost(60_006),
            ],
        )
        .await;

        // Dead node should still be known to the cluster.
        let dead_node_id = NodeId::for_test_localhost(60003);
        for node in &nodes {
            assert!(node
                .chitchat()
                .lock()
                .await
                .node_state(&dead_node_id)
                .is_some());
        }

        // Wait a bit more than `dead_node_grace_period` since all nodes will not
        // notice cluster change at the same time.
        let wait_for = DEAD_NODE_GRACE_PERIOD.add(Duration::from_secs(5));
        time::sleep(wait_for).await;

        // Dead node should no longer be known to the cluster.
        for node in &nodes {
            assert!(node
                .chitchat()
                .lock()
                .await
                .node_state(&dead_node_id)
                .is_none());
        }

        shutdown_nodes(nodes).await?;
        Ok(())
    }
}
