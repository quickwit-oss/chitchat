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
mod message;
pub(crate) mod serialize;
mod state;
mod failure_detector;


use serde::{Deserialize, Serialize};
use std::time::Duration;
use delta::Delta;
use failure_detector::FailureDetector;
use serde::Serialize;


use crate::digest::Digest;
use crate::message::ScuttleButtMessage;
use crate::serialize::Serializable;
pub use crate::state::ClusterState;
use crate::state::NodeState;

/// Map key for the heartbeat node value.
pub(crate) const HEARTBEAT_KEY: &str = "heartbeat";

pub type Version = u64;

/// A versioned value for a given Key-value pair.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct VersionedValue {
    pub value: String,
    pub version: Version,
}

pub struct ScuttleButt {
    mtu: usize,
    self_node_id: String,
    cluster_state: ClusterState,
    heartbeat: u64,
    failure_detector: FailureDetector,
}

impl ScuttleButt {
    pub fn with_node_id_and_seeds(self_node_id: String, seed_ids: Vec<String>) -> Self {
        let mut scuttlebutt = ScuttleButt {
            mtu: 60_000,
            heartbeat: 0,
            self_node_id,
            cluster_state: ClusterState::with_seed_ids(seed_ids),
            failure_detector: FailureDetector::new(1000, Duration::from_secs(10), Duration::from_secs(5))
        };

        // Immediately mark node as alive to ensure it responds to SYNs.
        scuttlebutt.self_node_state().set(HEARTBEAT_KEY, 0);

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
                let delta = self
                    .cluster_state
                    .compute_delta(&digest, self.mtu - 1 - self_digest.serialized_len());
                Some(ScuttleButtMessage::SynAck {
                    delta,
                    digest: self_digest,
                })
            }
            ScuttleButtMessage::SynAck { digest, delta } => {
                self.notify_failure_detector(&delta);
                self.cluster_state.apply_delta(delta);
                let delta = self.cluster_state.compute_delta(&digest, self.mtu - 1);
                Some(ScuttleButtMessage::Ack { delta })
            }
            ScuttleButtMessage::Ack { delta } => {
                self.notify_failure_detector(&delta);
                self.cluster_state.apply_delta(delta);
                None
            }
        }
    }

    fn notify_failure_detector(&mut self, delta: &Delta) {
        for (node_id, node_delta) in &delta.node_deltas {
            //TODO we need to clear existing state if we detect the node has restarted from a failure
            // discuss generation?
            let local_gen = self.cluster_state
                .node_state(node_id)
                .unwrap()
                .get_versioned("generation").unwrap();
            let node_gen = node_delta.key_values.get("generation").unwrap();
            //TODO convert and comapare
            if local_gen.value < node_gen.value {
                
                self.failure_detector.remove(node_id)
            }

            self.failure_detector.report_heartbeat(node_id);
        }
    }

    pub fn update_status(&mut self) {
        //request phi values and mark/unmark node as dead
        // self.dead_nodes.remove(node_id)
        // self.live_nodes.remove(node_id)
    }

    pub fn node_state(&self, node_id: &str) -> Option<&NodeState> {
        self.cluster_state.node_state(node_id)
    }

    pub fn self_node_state(&mut self) -> &mut NodeState {
        self.cluster_state.node_state_mut(&self.self_node_id)
    }

    /// Retrieve a list of all living nodes.
    pub fn living_nodes(&self) -> impl Iterator<Item = &str> {
        //TODO now this should use self.failure_detector.phi(node_id)
        self.cluster_state.living_nodes()
    }

    /// Compute digest.
    ///
    /// This method also increments the heartbeat, to force the presence
    /// of at least one update, and have the node liveliness propagated
    /// through the cluster.
    fn compute_digest(&mut self) -> Digest {
        // Ensure for every reply from this node, at least the heartbeat is changed.
        self.heartbeat += 1;
        let heartbeat = self.heartbeat;
        self.self_node_state().set(HEARTBEAT_KEY, heartbeat);
        self.cluster_state.compute_digest()
    }

    pub fn cluster_state(&self) -> ClusterState {
        self.cluster_state.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_scuttlebutt_handshake() {
        let mut node1 = ScuttleButt::with_node_id_and_seeds("node1".to_string(), Vec::new());
        {
            let state1 = node1.self_node_state();
            state1.set("key1a", "1");
            state1.set("key2a", "2");
        }
        let mut node2 = ScuttleButt::with_node_id_and_seeds("node2".to_string(), Vec::new());
        {
            let state2 = node2.self_node_state();
            state2.set("key1b", "1");
            state2.set("key2b", "2");
        }
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
}
