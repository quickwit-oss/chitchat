#![allow(clippy::type_complexity)]
#![allow(clippy::derive_partial_eq_without_eq)]

mod configuration;
mod delta;
mod digest;
mod failure_detector;
mod listener;
mod message;
pub(crate) mod serialize;
mod server;
mod state;
pub mod transport;
mod types;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::once;
use std::net::SocketAddr;

use delta::Delta;
use failure_detector::FailureDetector;
pub use failure_detector::FailureDetectorConfig;
pub use listener::ListenerHandle;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tracing::{error, warn};

pub use self::configuration::ChitchatConfig;
pub use self::state::{ClusterStateSnapshot, NodeState};
use crate::digest::Digest;
use crate::message::syn_ack_serialized_len;
pub use crate::message::ChitchatMessage;
pub use crate::server::{spawn_chitchat, ChitchatHandle};
use crate::state::ClusterState;
pub use crate::types::{ChitchatId, Heartbeat, MaxVersion, Version, VersionedValue};

/// Maximum UDP datagram payload size (in bytes).
///
/// Note that 65KB typically won't fit in a single IP packet,
/// so long messages will be sent over several IP fragments of MTU size.
///
/// We pick a large payload size because at the moment because
/// we send the self digest "in full".
/// An Ethernet frame size of 1400B would limit us to 20 nodes
/// or so.
pub(crate) const MAX_UDP_DATAGRAM_PAYLOAD_SIZE: usize = 65_507;

pub struct Chitchat {
    config: ChitchatConfig,
    cluster_state: ClusterState,
    failure_detector: FailureDetector,
    /// Notifies listeners when a change has occurred in the set of live nodes.
    previous_live_nodes: HashMap<ChitchatId, MaxVersion>,
    live_nodes_watcher_tx: watch::Sender<BTreeMap<ChitchatId, NodeState>>,
    live_nodes_watcher_rx: watch::Receiver<BTreeMap<ChitchatId, NodeState>>,
}

impl Chitchat {
    pub fn with_chitchat_id_and_seeds(
        config: ChitchatConfig,
        seed_addrs: watch::Receiver<HashSet<SocketAddr>>,
        initial_key_values: Vec<(String, String)>,
    ) -> Self {
        let failure_detector = FailureDetector::new(config.failure_detector_config.clone());
        let previous_live_nodes = HashMap::new();
        let (live_nodes_watcher_tx, live_nodes_watcher_rx) = watch::channel(BTreeMap::new());
        let mut chitchat = Chitchat {
            config,
            cluster_state: ClusterState::with_seed_addrs(seed_addrs),
            failure_detector,
            previous_live_nodes,
            live_nodes_watcher_tx,
            live_nodes_watcher_rx,
        };

        let self_node_state = chitchat.self_node_state();

        // Immediately mark the node as alive to ensure it responds to SYN messages.
        self_node_state.update_heartbeat();

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
                if cluster_id != self.cluster_id() {
                    warn!(
                        our_cluster_id=%self.cluster_id(),
                        their_cluster_id=%cluster_id,
                        "Received SYN message addressed to a different cluster."
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
                    &dead_nodes,
                    self.config.marked_for_deletion_grace_period as u64,
                );
                self.report_heartbeats(&delta);
                Some(ChitchatMessage::SynAck {
                    digest: self_digest,
                    delta,
                })
            }
            ChitchatMessage::SynAck { digest, delta } => {
                self.report_heartbeats(&delta);
                // self.config.chitchat_id.node_id, digest, delta);
                self.cluster_state.apply_delta(delta);
                let dead_nodes = self.dead_nodes().collect::<HashSet<_>>();
                let delta = self.cluster_state.compute_delta(
                    &digest,
                    MAX_UDP_DATAGRAM_PAYLOAD_SIZE - 1,
                    &dead_nodes,
                    self.config.marked_for_deletion_grace_period as u64,
                );
                Some(ChitchatMessage::Ack { delta })
            }
            ChitchatMessage::Ack { delta } => {
                self.report_heartbeats(&delta);
                self.cluster_state.apply_delta(delta);
                None
            }
            ChitchatMessage::BadCluster => {
                warn!("Message rejected by peer: wrong cluster.");
                None
            }
        }
    }

    fn gc_keys_marked_for_deletion(&mut self) {
        let dead_nodes = self.dead_nodes().cloned().collect::<HashSet<_>>();
        self.cluster_state.gc_keys_marked_for_deletion(
            self.config.marked_for_deletion_grace_period as u64,
            &dead_nodes,
        );
    }

    /// Reports heartbeats to the failure detector for nodes in the delta for which we received an
    /// update.
    fn report_heartbeats(&mut self, delta: &Delta) {
        for (chitchat_id, node_delta) in &delta.node_deltas {
            if let Some(node_state) = self.cluster_state.node_states.get(chitchat_id) {
                if node_state.heartbeat() < node_delta.heartbeat
                    || node_state.max_version() < node_delta.max_version
                {
                    self.failure_detector.report_heartbeat(chitchat_id);
                }
            }
        }
    }

    /// Marks the node as dead or alive depending on the new phi values and updates the live nodes
    /// watcher accordingly.
    pub(crate) fn update_nodes_liveness(&mut self) {
        self.cluster_state
            .nodes()
            .filter(|&chitchat_id| *chitchat_id != self.config.chitchat_id)
            .for_each(|chitchat_id| {
                self.failure_detector.update_node_liveness(chitchat_id);
            });

        let current_live_nodes = self
            .live_nodes()
            .map(|chitchat_id| {
                let node_state = self
                    .node_state(chitchat_id)
                    .expect("Node state should exist.");
                (chitchat_id.clone(), node_state.max_version())
            })
            .collect::<HashMap<_, _>>();

        if self.previous_live_nodes != current_live_nodes {
            let live_nodes = current_live_nodes
                .keys()
                .cloned()
                .map(|chitchat_id| {
                    let node_state = self
                        .node_state(&chitchat_id)
                        .expect("Node state should exist.")
                        .clone();
                    (chitchat_id, node_state)
                })
                .collect::<BTreeMap<_, _>>();
            self.previous_live_nodes = current_live_nodes;

            if self.live_nodes_watcher_tx.send(live_nodes).is_err() {
                error!(current_node = ?self.self_chitchat_id(), "error while reporting membership change event.")
            }
        }
        // Perform garbage collection.
        let garbage_collected_nodes = self.failure_detector.garbage_collect();
        for chitchat_id in &garbage_collected_nodes {
            self.cluster_state.remove_node(chitchat_id);
        }
    }

    pub fn node_states(&self) -> &BTreeMap<ChitchatId, NodeState> {
        &self.cluster_state.node_states
    }

    pub fn node_state(&self, chitchat_id: &ChitchatId) -> Option<&NodeState> {
        self.cluster_state.node_state(chitchat_id)
    }

    pub fn self_node_state(&mut self) -> &mut NodeState {
        self.cluster_state.node_state_mut(&self.config.chitchat_id)
    }

    /// Returns the set of nodes considered alive by the failure detector. It includes the
    /// current node (also called "self node"), which is always considered alive.
    pub fn live_nodes(&self) -> impl Iterator<Item = &ChitchatId> {
        once(self.self_chitchat_id()).chain(self.failure_detector.live_nodes())
    }

    /// Returns a watch stream for monitoring changes in the cluster.
    ///
    /// The stream will emit a new value whenever a node:
    /// - joins the cluster
    /// - leaves the cluster
    /// - updates its max version
    ///
    /// Heartbeats are not notified.
    pub fn live_nodes_watcher(&self) -> WatchStream<BTreeMap<ChitchatId, NodeState>> {
        WatchStream::new(self.live_nodes_watcher_rx.clone())
    }

    /// Returns the set of nodes considered dead by the failure detector.
    pub fn dead_nodes(&self) -> impl Iterator<Item = &ChitchatId> {
        self.failure_detector.dead_nodes()
    }

    /// Returns the set of seed nodes.
    pub fn seed_nodes(&self) -> HashSet<SocketAddr> {
        self.cluster_state.seed_addrs()
    }

    pub fn cluster_id(&self) -> &str {
        &self.config.cluster_id
    }

    /// Returns the current node's Chitchat ID.
    pub fn self_chitchat_id(&self) -> &ChitchatId {
        &self.config.chitchat_id
    }

    /// Returns a serializable snapshot of the cluster state.
    pub fn state_snapshot(&self) -> ClusterStateSnapshot {
        ClusterStateSnapshot::from(&self.cluster_state)
    }

    pub(crate) fn update_heartbeat(&mut self) {
        self.self_node_state().update_heartbeat();
    }

    pub(crate) fn cluster_state(&self) -> &ClusterState {
        &self.cluster_state
    }

    /// Computes the node's digest.
    fn compute_digest(&self, dead_nodes: &HashSet<&ChitchatId>) -> Digest {
        self.cluster_state.compute_digest(dead_nodes)
    }

    /// Subscribes a callback that will be called every time a key matching the supplied prefix
    /// is inserted or updated.
    ///
    /// Disclaimer:
    /// The callback is required to be as light as possible.
    /// In particular,
    /// - it should not access the cluster state (as it is locked at the moment of the
    /// execution of the callback.
    /// - it should be fast: the callback is executed in an async context.
    ///
    /// The callback is called with a [`KeyChangeEvent`] that contains the key stripped of the
    /// prefix, the new value and the node that owns this key/value.
    ///
    /// Deleted keys are not notified.
    #[must_use]
    pub fn subscribe_event(
        &self,
        key_prefix: impl ToString,
        callback: impl Fn(KeyChangeEvent) + 'static + Send + Sync,
    ) -> ListenerHandle {
        self.cluster_state()
            .listeners
            .subscribe_event(key_prefix, callback)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct KeyChangeEvent<'a> {
    pub key: &'a str,
    pub value: &'a str,
    pub node: &'a ChitchatId,
}

impl<'a> KeyChangeEvent<'a> {
    fn strip_key_prefix(&self, prefix: &str) -> Option<KeyChangeEvent> {
        let key_without_prefix = self.key.strip_prefix(prefix)?;
        Some(KeyChangeEvent {
            key: key_without_prefix,
            value: self.value,
            node: self.node,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Add, RangeInclusive};
    use std::sync::atomic::{AtomicUsize, Ordering};
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
        assert_eq!(lhs.num_key_values(), rhs.num_key_values());
        for (key, value) in lhs.key_values() {
            assert_eq!(rhs.get_versioned(key), Some(value));
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
        chitchat_id: ChitchatId,
        seeds: &[String],
        transport: &dyn Transport,
    ) -> ChitchatHandle {
        let config = ChitchatConfig {
            chitchat_id: chitchat_id.clone(),
            cluster_id: "default-cluster".to_string(),
            gossip_interval: Duration::from_millis(100),
            listen_addr: chitchat_id.gossip_advertise_addr,
            seed_nodes: seeds.to_vec(),
            failure_detector_config: FailureDetectorConfig {
                dead_node_grace_period: DEAD_NODE_GRACE_PERIOD,
                phi_threshold: 5.0,
                initial_interval: Duration::from_millis(100),
                ..Default::default()
            },
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
        let chitchat_ids: Vec<ChitchatId> = port_range.map(ChitchatId::for_local_test).collect();
        let node_without_seed = start_node(chitchat_ids[0].clone(), &[], transport).await;
        let mut chitchat_handlers: Vec<ChitchatHandle> = vec![node_without_seed];
        for chitchat_id in &chitchat_ids[1..] {
            let seeds = chitchat_ids
                .iter()
                .filter(|&peer_id| peer_id != chitchat_id)
                .map(|peer_id| peer_id.gossip_advertise_addr.to_string())
                .collect::<Vec<_>>();
            chitchat_handlers.push(start_node(chitchat_id.clone(), &seeds, transport).await);
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
        expected_nodes: &[ChitchatId],
    ) {
        let expected_nodes = expected_nodes.iter().collect::<HashSet<_>>();
        let mut live_nodes_watcher =
            chitchat
                .lock()
                .await
                .live_nodes_watcher()
                .skip_while(|live_nodes| {
                    if live_nodes.len() != expected_nodes.len() {
                        return true;
                    }
                    let live_nodes = live_nodes.keys().collect::<HashSet<_>>();
                    live_nodes != expected_nodes
                });
        tokio::time::timeout(Duration::from_secs(60), async move {
            live_nodes_watcher.next().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[test]
    fn test_chitchat_handshake() {
        let node_config1 = ChitchatConfig::for_test(10_001);
        let empty_seeds = watch::channel(Default::default()).1;
        let mut node1 = Chitchat::with_chitchat_id_and_seeds(
            node_config1,
            empty_seeds.clone(),
            vec![
                ("key1a".to_string(), "1".to_string()),
                ("key2a".to_string(), "2".to_string()),
            ],
        );
        let node_config2 = ChitchatConfig::for_test(10_002);
        let mut node2 = Chitchat::with_chitchat_id_and_seeds(
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
        let transport = ChannelTransport::with_mtu(MAX_UDP_DATAGRAM_PAYLOAD_SIZE);
        let nodes = setup_nodes(20001..=20005, &transport).await;

        let node2 = nodes.get(1).unwrap();
        assert_eq!(node2.chitchat_id().advertise_port(), 20002);
        wait_for_chitchat_state(
            node2.chitchat(),
            &[
                ChitchatId::for_local_test(20001),
                ChitchatId::for_local_test(20002),
                ChitchatId::for_local_test(20003),
                ChitchatId::for_local_test(20004),
                ChitchatId::for_local_test(20005),
            ],
        )
        .await;

        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_node_goes_from_live_to_down_to_live() -> anyhow::Result<()> {
        let transport = ChannelTransport::with_mtu(MAX_UDP_DATAGRAM_PAYLOAD_SIZE);
        let mut nodes = setup_nodes(30001..=30006, &transport).await;
        wait_for_chitchat_state(
            nodes[0].chitchat(),
            &[
                ChitchatId::for_local_test(30001),
                ChitchatId::for_local_test(30002),
                ChitchatId::for_local_test(30003),
                ChitchatId::for_local_test(30004),
                ChitchatId::for_local_test(30005),
                ChitchatId::for_local_test(30006),
            ],
        )
        .await;

        // Take node 3 down.
        let node3 = nodes.remove(2);
        assert_eq!(node3.chitchat_id().advertise_port(), 30003);
        node3.shutdown().await.unwrap();

        let node2 = nodes.get(1).unwrap();
        assert_eq!(node2.chitchat_id().advertise_port(), 30002);
        wait_for_chitchat_state(
            node2.chitchat(),
            &[
                ChitchatId::for_local_test(30001),
                ChitchatId::for_local_test(30002),
                ChitchatId::for_local_test(30004),
                ChitchatId::for_local_test(30005),
                ChitchatId::for_local_test(30006),
            ],
        )
        .await;

        // Restart node 3.
        let node_3 = ChitchatId::for_local_test(30003);
        nodes.push(
            start_node(
                node_3,
                &[ChitchatId::for_local_test(30_001)
                    .gossip_advertise_addr
                    .to_string()],
                &transport,
            )
            .await,
        );
        wait_for_chitchat_state(
            nodes[0].chitchat(),
            &[
                ChitchatId::for_local_test(30001),
                ChitchatId::for_local_test(30002),
                ChitchatId::for_local_test(30003),
                ChitchatId::for_local_test(30004),
                ChitchatId::for_local_test(30005),
                ChitchatId::for_local_test(30006),
            ],
        )
        .await;

        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_dead_node_should_not_be_gossiped_when_node_joins() -> anyhow::Result<()> {
        let transport = ChannelTransport::with_mtu(MAX_UDP_DATAGRAM_PAYLOAD_SIZE);
        let mut nodes = setup_nodes(40001..=40004, &transport).await;
        {
            let node2 = nodes.get(1).unwrap();
            assert_eq!(node2.chitchat_id().advertise_port(), 40002);
            wait_for_chitchat_state(
                node2.chitchat(),
                &[
                    ChitchatId::for_local_test(40001),
                    ChitchatId::for_local_test(40002),
                    ChitchatId::for_local_test(40003),
                    ChitchatId::for_local_test(40004),
                ],
            )
            .await;
        }

        // Take node 3 down.
        let node3 = nodes.remove(2);
        assert_eq!(node3.chitchat_id().advertise_port(), 40003);
        node3.shutdown().await.unwrap();

        {
            let node2 = nodes.get(1).unwrap();
            assert_eq!(node2.chitchat_id().advertise_port(), 40002);
            wait_for_chitchat_state(
                node2.chitchat(),
                &[
                    ChitchatId::for_local_test(40_001),
                    ChitchatId::for_local_test(40002),
                    ChitchatId::for_local_test(40_004),
                ],
            )
            .await;
        }

        // Restart node at localhost:40003 with new name
        let mut new_config = ChitchatConfig::for_test(40_003);
        new_config.chitchat_id.node_id = "new_node".to_string();
        let new_chitchat_id = new_config.chitchat_id.clone();
        let seed_addr = ChitchatId::for_local_test(40_002).gossip_advertise_addr;
        new_config.seed_nodes = vec![seed_addr.to_string()];
        let new_node_chitchat = spawn_chitchat(new_config, Vec::new(), &transport)
            .await
            .unwrap();

        wait_for_chitchat_state(
            new_node_chitchat.chitchat(),
            &[
                ChitchatId::for_local_test(40_001),
                ChitchatId::for_local_test(40_002),
                new_chitchat_id,
                ChitchatId::for_local_test(40_004),
            ],
        )
        .await;

        nodes.push(new_node_chitchat);
        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_network_partition_nodes() -> anyhow::Result<()> {
        let transport = ChannelTransport::with_mtu(MAX_UDP_DATAGRAM_PAYLOAD_SIZE);
        let port_range = 11_001u16..=11_006;
        let nodes = setup_nodes(port_range.clone(), &transport).await;

        // Check nodes know each other.
        for node in &nodes {
            let expected_peers: Vec<ChitchatId> = port_range
                .clone()
                .map(ChitchatId::for_local_test)
                .collect::<Vec<_>>();
            wait_for_chitchat_state(node.chitchat(), &expected_peers).await;
        }
        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_dead_node_garbage_collection() -> anyhow::Result<()> {
        let transport = ChannelTransport::with_mtu(MAX_UDP_DATAGRAM_PAYLOAD_SIZE);
        let mut nodes = setup_nodes(60001..=60006, &transport).await;
        let node2 = nodes.get(1).unwrap();
        assert_eq!(node2.chitchat_id().advertise_port(), 60002);
        wait_for_chitchat_state(
            node2.chitchat(),
            &[
                ChitchatId::for_local_test(60_001),
                ChitchatId::for_local_test(60_002),
                ChitchatId::for_local_test(60_003),
                ChitchatId::for_local_test(60_004),
                ChitchatId::for_local_test(60_005),
                ChitchatId::for_local_test(60_006),
            ],
        )
        .await;

        // Take node 3 down.
        let node3 = nodes.remove(2);
        assert_eq!(node3.chitchat_id().advertise_port(), 60003);
        node3.shutdown().await.unwrap();

        let node2 = nodes.get(1).unwrap();
        assert_eq!(node2.chitchat_id().advertise_port(), 60002);
        wait_for_chitchat_state(
            node2.chitchat(),
            &[
                ChitchatId::for_local_test(60_001),
                ChitchatId::for_local_test(60_002),
                ChitchatId::for_local_test(60_004),
                ChitchatId::for_local_test(60_005),
                ChitchatId::for_local_test(60_006),
            ],
        )
        .await;

        // Dead node should still be known to the cluster.
        let dead_chitchat_id = ChitchatId::for_local_test(60003);
        for node in &nodes {
            assert!(node
                .chitchat()
                .lock()
                .await
                .node_state(&dead_chitchat_id)
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
                .node_state(&dead_chitchat_id)
                .is_none());
        }
        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[test]
    fn test_chitchat_listener() {
        let node_config1 = ChitchatConfig::for_test(10_001);
        let empty_seeds = watch::channel(Default::default()).1;
        let mut node1 = Chitchat::with_chitchat_id_and_seeds(
            node_config1,
            empty_seeds.clone(),
            vec![("self1:suffix1".to_string(), "hello1".to_string())],
        );
        let counter_self_key: Arc<AtomicUsize> = Default::default();
        let counter_other_key: Arc<AtomicUsize> = Default::default();

        let counter_self_key_clone = counter_self_key.clone();
        node1
            .subscribe_event("self1:", move |evt| {
                assert_eq!(evt.key, "suffix1");
                assert_eq!(evt.value, "updated");
                counter_self_key_clone.fetch_add(1, Ordering::SeqCst);
            })
            .forever();
        let counter_other_key_clone = counter_other_key.clone();
        node1
            .subscribe_event("other:", move |evt| {
                assert_eq!(evt.key, "suffix");
                assert_eq!(evt.value, "hello");
                counter_other_key_clone.fetch_add(1, Ordering::SeqCst);
            })
            .forever();

        let counter_self_key_clone = counter_self_key.clone();
        node1
            .subscribe_event("self2:", move |evt| {
                assert_eq!(evt.key, "suffix2");
                assert_eq!(evt.value, "hello2");
                counter_self_key_clone.fetch_add(1, Ordering::SeqCst);
            })
            .forever();

        let node_config2 = ChitchatConfig::for_test(10_002);
        let mut node2 = Chitchat::with_chitchat_id_and_seeds(
            node_config2,
            empty_seeds,
            vec![("other:suffix".to_string(), "hello".to_string())],
        );

        assert_eq!(counter_self_key.load(Ordering::SeqCst), 0);
        assert_eq!(counter_other_key.load(Ordering::SeqCst), 0);

        run_chitchat_handshake(&mut node1, &mut node2);
        assert_nodes_sync(&[&node1, &node2]);

        assert_eq!(counter_self_key.load(Ordering::SeqCst), 0);
        assert_eq!(counter_other_key.load(Ordering::SeqCst), 1);

        // useless handshake
        run_chitchat_handshake(&mut node1, &mut node2);

        assert_eq!(counter_self_key.load(Ordering::SeqCst), 0);
        assert_eq!(counter_other_key.load(Ordering::SeqCst), 1);

        node1.self_node_state().set("self2:suffix2", "hello2");
        assert_eq!(counter_self_key.load(Ordering::SeqCst), 1);
        assert_eq!(counter_other_key.load(Ordering::SeqCst), 1);

        run_chitchat_handshake(&mut node1, &mut node2);

        assert_eq!(counter_self_key.load(Ordering::SeqCst), 1);
        assert_eq!(counter_other_key.load(Ordering::SeqCst), 1);

        node1.self_node_state().set("self1:suffix1", "updated");
        assert_eq!(counter_self_key.load(Ordering::SeqCst), 2);

        node1.self_node_state().mark_for_deletion("self1:suffix1");
        node2.self_node_state().mark_for_deletion("other:suffix");

        run_chitchat_handshake(&mut node1, &mut node2);

        assert_eq!(counter_self_key.load(Ordering::SeqCst), 2);
        assert_eq!(counter_other_key.load(Ordering::SeqCst), 1);
    }
}
