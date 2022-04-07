// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

pub mod server;

mod delta;
mod digest;
mod failure_detector;
mod message;
pub(crate) mod serialize;
mod state;

use std::collections::HashSet;

use delta::Delta;
use failure_detector::FailureDetector;
pub use failure_detector::FailureDetectorConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tracing::{debug, error};

use crate::digest::Digest;
use crate::message::ScuttleButtMessage;
use crate::serialize::Serializable;
use crate::state::NodeState;
pub use crate::state::{ClusterState, SerializableClusterState};

/// Map key for the heartbeat node value.
pub(crate) const HEARTBEAT_KEY: &str = "heartbeat";

pub type Version = u64;

/// [`NodeId`] represents a ScuttleButt Node identifier.
///
/// For the lifetime of a cluster, nodes can go down and back up, they may
/// permanently die. These are couple of issues we want to solve with [`NodeId`] struct:
/// - We want a fresh local scuttlebutt state for every run of a node.
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
/// Because ScuttleButt instance is not concerned about caching strategy and what needs to be
/// cached, We let the client decide what makes up the `id` attribute and how to extract its
/// components.
///
/// One such client is Quickwit where the `id` is made of
/// `{node_unique_id}/{node_generation}/`.
/// - node_unique_id: a static unique name for the node.
/// - node_generation: a monotonically increasing value (timestamp on every run)
/// More details at https://github.com/quickwit-oss/scuttlebutt/issues/1#issuecomment-1059029051
///
/// Note: using timestamp to make the `id` dynamic has the potential of reusing
/// a previously used `id` in cases where the clock is reset in the past. We believe this
/// very rare and things should just work fine.
#[derive(Default, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct NodeId {
    // The unique identifier of this node in the cluster.
    pub id: String,
    // The SocketAddr other peers should use to communicate.
    pub gossip_public_address: String,
}

impl NodeId {
    pub fn new(id: String, gossip_public_address: String) -> Self {
        Self {
            id,
            gossip_public_address,
        }
    }
}

impl From<&str> for NodeId {
    fn from(id: &str) -> Self {
        Self::new(id.to_string(), id.to_string())
    }
}

impl From<String> for NodeId {
    fn from(id: String) -> Self {
        Self::new(id.clone(), id)
    }
}

/// A versioned value for a given Key-value pair.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct VersionedValue {
    pub value: String,
    pub version: Version,
}

pub struct ScuttleButt {
    mtu: usize,
    address: String,
    self_node_id: NodeId,
    cluster_name: String,
    cluster_state: ClusterState,
    heartbeat: u64,
    /// The failure detector instance.
    failure_detector: FailureDetector,
    /// A notification channel (sender) for sending live nodes change feed.
    live_nodes_watcher_tx: watch::Sender<HashSet<NodeId>>,
    /// A notification channel (receiver) for receiving live nodes change feed.
    live_nodes_watcher_rx: watch::Receiver<HashSet<NodeId>>,
}

impl ScuttleButt {
    pub fn with_node_id_and_seeds(
        self_node_id: NodeId,
        seed_ids: HashSet<String>,
        address: String,
        cluster_name: String,
        initial_key_values: Vec<(impl ToString, impl ToString)>,
        failure_detector_config: FailureDetectorConfig,
    ) -> Self {
        let (live_nodes_watcher_tx, live_nodes_watcher_rx) = watch::channel(HashSet::new());
        let mut scuttlebutt = ScuttleButt {
            mtu: 60_000,
            address,
            self_node_id,
            cluster_name,
            cluster_state: ClusterState::with_seed_ids(seed_ids),
            heartbeat: 0,
            failure_detector: FailureDetector::new(failure_detector_config),
            live_nodes_watcher_tx,
            live_nodes_watcher_rx,
        };

        let self_node_state = scuttlebutt.self_node_state();

        // Immediately mark node as alive to ensure it responds to SYNs.
        self_node_state.set(HEARTBEAT_KEY, 0);

        // Set initial key/value pairs.
        for (key, value) in initial_key_values {
            self_node_state.set(key, value);
        }

        scuttlebutt
    }

    pub fn set_mtu(&mut self, mtu: usize) {
        self.mtu = mtu;
    }

    pub fn create_syn_message(&mut self) -> ScuttleButtMessage {
        let digest = self.compute_digest();
        ScuttleButtMessage::Syn { digest }
    }

    pub fn process_message(&mut self, msg: ScuttleButtMessage) -> Option<ScuttleButtMessage> {
        match msg {
            ScuttleButtMessage::Syn { digest } => {
                let self_digest = self.compute_digest();
                let dead_nodes = self.dead_nodes().collect::<HashSet<_>>();
                let delta = self.cluster_state.compute_delta(
                    &digest,
                    self.mtu - 1 - self_digest.serialized_len(),
                    dead_nodes,
                );
                self.report_to_failure_detector(&delta);
                Some(ScuttleButtMessage::SynAck {
                    delta,
                    digest: self_digest,
                })
            }
            ScuttleButtMessage::SynAck { digest, delta } => {
                self.report_to_failure_detector(&delta);
                self.cluster_state.apply_delta(delta);
                let dead_nodes = self.dead_nodes().collect::<HashSet<_>>();
                let delta = self
                    .cluster_state
                    .compute_delta(&digest, self.mtu - 1, dead_nodes);
                Some(ScuttleButtMessage::Ack { delta })
            }
            ScuttleButtMessage::Ack { delta } => {
                self.report_to_failure_detector(&delta);
                self.cluster_state.apply_delta(delta);
                None
            }
        }
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

    /// Checks and marks nodes as dead or live.
    pub fn update_nodes_liveliness(&mut self) {
        let live_nodes_before = self.live_nodes().cloned().collect::<HashSet<_>>();
        let cluster_nodes = self
            .cluster_state
            .nodes()
            .filter(|node_id| *node_id != &self.self_node_id)
            .collect::<Vec<_>>();
        for node_id in &cluster_nodes {
            self.failure_detector.update_node_liveliness(node_id);
        }

        let live_nodes_after = self.live_nodes().cloned().collect::<HashSet<_>>();
        if live_nodes_before != live_nodes_after {
            debug!(current_node = ?self.self_node_id, live_nodes = ?live_nodes_after, "nodes status changed");
            if self.live_nodes_watcher_tx.send(live_nodes_after).is_err() {
                error!(current_node = ?self.self_node_id, "error while reporting membership change event.")
            }
        }

        // Perform garbage collection.
        let garbage_collected_nodes = self.failure_detector.garbage_collect();
        for node_id in garbage_collected_nodes.iter() {
            self.cluster_state.remove_node(node_id)
        }
    }

    pub fn node_state(&self, node_id: &NodeId) -> Option<&NodeState> {
        self.cluster_state.node_state(node_id)
    }

    pub fn self_node_state(&mut self) -> &mut NodeState {
        self.cluster_state.node_state_mut(&self.self_node_id)
    }

    /// Retrieves the list of all live nodes.
    pub fn live_nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.failure_detector.live_nodes()
    }

    /// Retrieve the list of all dead nodes.
    pub fn dead_nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.failure_detector.dead_nodes()
    }

    /// Retrieve a list of seed nodes.
    pub fn seed_nodes(&self) -> impl Iterator<Item = &str> {
        self.cluster_state.seed_nodes()
    }

    pub fn self_node_id(&self) -> &NodeId {
        &self.self_node_id
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    /// Computes digest.
    ///
    /// This method also increments the heartbeat, to force the presence
    /// of at least one update, and have the node liveliness propagated
    /// through the cluster.
    fn compute_digest(&mut self) -> Digest {
        // Ensure for every reply from this node, at least the heartbeat is changed.
        self.heartbeat += 1;
        let heartbeat = self.heartbeat;
        self.self_node_state().set(HEARTBEAT_KEY, heartbeat);
        let dead_nodes: HashSet<_> = self.dead_nodes().collect();
        self.cluster_state.compute_digest(dead_nodes)
    }

    pub fn cluster_state(&self) -> ClusterState {
        self.cluster_state.clone()
    }

    /// Returns a watch stream for monitoring changes on the cluster's live nodes.
    pub fn live_nodes_watcher(&self) -> WatchStream<HashSet<NodeId>> {
        WatchStream::new(self.live_nodes_watcher_rx.clone())
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
    use crate::server::ScuttleServer;

    const DEAD_NODE_GRACE_PERIOD: Duration = Duration::from_secs(25);

    fn run_scuttlebutt_handshake(initiating_node: &mut ScuttleButt, peer_node: &mut ScuttleButt) {
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

    fn assert_nodes_sync(nodes: &[&ScuttleButt]) {
        let first_node_states = &nodes[0].cluster_state.node_states;
        for other_node in nodes.iter().skip(1) {
            let node_states = &other_node.cluster_state.node_states;
            assert_eq!(first_node_states.len(), node_states.len());
            for (key, value) in first_node_states {
                assert_cluster_state_eq(value, node_states.get(key).unwrap());
            }
        }
    }

    fn start_node(port: u32, seeds: &[String]) -> ScuttleServer {
        let address = format!("localhost:{}", port);
        ScuttleServer::spawn(
            NodeId::from(address.as_str()),
            seeds,
            address,
            "test-cluster".to_string(),
            Vec::<(&str, &str)>::new(),
            FailureDetectorConfig {
                dead_node_grace_period: DEAD_NODE_GRACE_PERIOD,
                ..Default::default()
            },
        )
    }

    async fn setup_nodes(port_range: RangeInclusive<u32>) -> Vec<(String, ScuttleServer)> {
        let mut tasks = Vec::new();
        let mut ports = port_range.clone();
        let seed_port = ports.next().unwrap();
        tasks.push((seed_port.to_string(), start_node(seed_port, &[])));
        for port in ports {
            let seeds = port_range
                .clone()
                .filter(|peer_port| peer_port != &port)
                .map(|peer_port| format!("localhost:{}", peer_port))
                .collect::<Vec<_>>();

            tasks.push((port.to_string(), start_node(port, &seeds)));
        }
        // Make sure the failure detector's fake clock moves forward.
        tokio::spawn(async {
            let mut ticker = IntervalStream::new(time::interval(Duration::from_millis(50)));
            while ticker.next().await.is_some() {
                MockClock::advance(Duration::from_millis(50));
            }
        });
        tasks
    }

    async fn shutdown_nodes(nodes: Vec<(String, ScuttleServer)>) -> anyhow::Result<()> {
        for (_, node) in nodes {
            node.shutdown().await?;
        }
        Ok(())
    }

    async fn wait_for_scuttlebutt_state(
        scuttlebutt: Arc<Mutex<ScuttleButt>>,
        expected_node_count: usize,
        expected_nodes: &[NodeId],
    ) {
        let mut live_nodes_watcher = scuttlebutt
            .lock()
            .await
            .live_nodes_watcher()
            .skip_while(|live_nodes| live_nodes.len() != expected_node_count);
        tokio::time::timeout(Duration::from_secs(50), async move {
            let live_nodes = live_nodes_watcher.next().await.unwrap();
            assert_eq!(
                live_nodes,
                expected_nodes.iter().cloned().collect::<HashSet<_>>()
            );
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_scuttlebutt_spawn() -> anyhow::Result<()> {
        let node = start_node(20001, &["localhost:20001".to_string()]);
        assert_eq!(
            node.scuttlebutt().lock().await.cluster_name(),
            "test-cluster"
        );
        Ok(())
    }

    #[test]
    fn test_scuttlebutt_handshake() {
        let mut node1 = ScuttleButt::with_node_id_and_seeds(
            NodeId::from("node1"),
            HashSet::new(),
            "node1".to_string(),
            "test-cluster".to_string(),
            vec![("key1a", "1"), ("key2a", "2")],
            FailureDetectorConfig::default(),
        );
        let mut node2 = ScuttleButt::with_node_id_and_seeds(
            NodeId::from("node2"),
            HashSet::new(),
            "node2".to_string(),
            "test-cluster".to_string(),
            vec![("key1b", "1"), ("key2b", "2")],
            FailureDetectorConfig::default(),
        );
        run_scuttlebutt_handshake(&mut node1, &mut node2);
        dbg!(&node1.cluster_state());
        dbg!(&node2.cluster_state());
        assert_nodes_sync(&[&node1, &node2]);
        // useless handshake
        run_scuttlebutt_handshake(&mut node1, &mut node2);
        assert_nodes_sync(&[&node1, &node2]);
        {
            let state1 = node1.self_node_state();
            state1.set("key1a", "3");
            state1.set("key1c", "4");
        }
        run_scuttlebutt_handshake(&mut node1, &mut node2);
        assert_nodes_sync(&[&node1, &node2]);
    }

    #[tokio::test]
    async fn test_multiple_nodes() -> anyhow::Result<()> {
        let nodes = setup_nodes(20001..=20005).await;

        let (id, node) = nodes.get(1).unwrap();
        assert_eq!(id, "20002");
        wait_for_scuttlebutt_state(
            node.scuttlebutt(),
            4,
            &[
                NodeId::from("localhost:20001"),
                NodeId::from("localhost:20003"),
                NodeId::from("localhost:20004"),
                NodeId::from("localhost:20005"),
            ],
        )
        .await;

        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_node_goes_from_live_to_down_to_live() -> anyhow::Result<()> {
        let mut nodes = setup_nodes(30001..=30006).await;

        let (id, node) = nodes.get(1).unwrap();
        assert_eq!(id, "30002");
        wait_for_scuttlebutt_state(
            node.scuttlebutt(),
            5,
            &[
                NodeId::from("localhost:30001"),
                NodeId::from("localhost:30003"),
                NodeId::from("localhost:30004"),
                NodeId::from("localhost:30005"),
                NodeId::from("localhost:30006"),
            ],
        )
        .await;

        // Take down node at localhost:30003
        let (id, node) = nodes.remove(2);
        assert_eq!(id, "30003");
        node.shutdown().await.unwrap();

        let (id, node) = nodes.get(1).unwrap();
        assert_eq!(id, "30002");
        wait_for_scuttlebutt_state(
            node.scuttlebutt(),
            4,
            &[
                NodeId::from("localhost:30001"),
                NodeId::from("localhost:30004"),
                NodeId::from("localhost:30005"),
                NodeId::from("localhost:30006"),
            ],
        )
        .await;

        // Restart node at localhost:10003
        let port = 30003;
        nodes.push((
            port.to_string(),
            start_node(port, &["localhost:30001".to_string()]),
        ));

        let (id, node) = nodes.get(1).unwrap();
        assert_eq!(id, "30002");
        wait_for_scuttlebutt_state(
            node.scuttlebutt(),
            5,
            &[
                NodeId::from("localhost:30001"),
                NodeId::from("localhost:30003"),
                NodeId::from("localhost:30004"),
                NodeId::from("localhost:30005"),
                NodeId::from("localhost:30006"),
            ],
        )
        .await;

        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_dead_node_should_not_be_gossiped_when_node_joins() -> anyhow::Result<()> {
        let mut nodes = setup_nodes(40001..=40004).await;

        let (id, node) = nodes.get(1).unwrap();
        assert_eq!(id, "40002");
        wait_for_scuttlebutt_state(
            node.scuttlebutt(),
            3,
            &[
                NodeId::from("localhost:40001"),
                NodeId::from("localhost:40003"),
                NodeId::from("localhost:40004"),
            ],
        )
        .await;

        // Take down node at localhost:40003
        let (id, node) = nodes.remove(2);
        assert_eq!(id, "40003");
        node.shutdown().await.unwrap();

        let (id, node) = nodes.get(1).unwrap();
        assert_eq!(id, "40002");
        wait_for_scuttlebutt_state(
            node.scuttlebutt(),
            2,
            &[
                NodeId::from("localhost:40001"),
                NodeId::from("localhost:40004"),
            ],
        )
        .await;

        // Restart node at localhost:40003 with new name
        let port = 40003;
        let address = format!("localhost:{}", port);
        let new_node_scuttlebutt = ScuttleServer::spawn(
            NodeId::new("new_node".to_string(), address.clone()),
            &["localhost:40001".to_string()],
            address,
            "test-cluster".to_string(),
            Vec::<(&str, &str)>::new(),
            FailureDetectorConfig::default(),
        );
        nodes.push((port.to_string(), new_node_scuttlebutt));

        let (id, node) = nodes.get(3).unwrap();
        assert_eq!(id, "40003");
        wait_for_scuttlebutt_state(
            node.scuttlebutt(),
            3,
            &[
                NodeId::from("localhost:40001"),
                NodeId::from("localhost:40002"),
                NodeId::from("localhost:40004"),
            ],
        )
        .await;

        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_network_partition_nodes() -> anyhow::Result<()> {
        let nodes = setup_nodes(11001..=11006).await;

        // Check nodes know each other.
        for (id, node) in nodes.iter() {
            let peers = (11001u32..=11006)
                .filter(|peer_port| peer_port.to_string() != *id)
                .map(|peer_port| NodeId::from(format!("localhost:{}", peer_port)))
                .collect::<Vec<_>>();

            wait_for_scuttlebutt_state(node.scuttlebutt(), 5, &peers.to_vec()).await;
        }

        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_dead_node_garbage_collection() -> anyhow::Result<()> {
        let mut nodes = setup_nodes(60001..=60006).await;

        let (id, node) = nodes.get(1).unwrap();
        assert_eq!(id, "60002");
        wait_for_scuttlebutt_state(
            node.scuttlebutt(),
            5,
            &[
                NodeId::from("localhost:60001"),
                NodeId::from("localhost:60003"),
                NodeId::from("localhost:60004"),
                NodeId::from("localhost:60005"),
                NodeId::from("localhost:60006"),
            ],
        )
        .await;

        // Take down node at localhost:60003
        let (id, node) = nodes.remove(2);
        assert_eq!(id, "60003");
        node.shutdown().await.unwrap();

        let (id, node) = nodes.get(1).unwrap();
        assert_eq!(id, "60002");
        wait_for_scuttlebutt_state(
            node.scuttlebutt(),
            4,
            &[
                NodeId::from("localhost:60001"),
                NodeId::from("localhost:60004"),
                NodeId::from("localhost:60005"),
                NodeId::from("localhost:60006"),
            ],
        )
        .await;

        // Dead node should still be known to the cluster.
        let dead_node_id = NodeId::from("localhost:60003");
        for (_, node) in nodes.iter() {
            assert!(node
                .scuttlebutt()
                .lock()
                .await
                .node_state(&dead_node_id)
                .is_some());
        }

        // Wait a bit more than `dead_node_grace_period` since all nodes will not
        // notice cluster change at the same time.
        let wait_for = DEAD_NODE_GRACE_PERIOD.add(Duration::from_secs(15));
        time::sleep(wait_for).await;

        // Dead node should no longer be known to the cluster.
        for (_, node) in nodes.iter() {
            assert!(node
                .scuttlebutt()
                .lock()
                .await
                .node_state(&dead_node_id)
                .is_none());
        }

        shutdown_nodes(nodes).await?;
        Ok(())
    }
}
