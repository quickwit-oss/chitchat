pub mod server;

mod model;

use std::io::BufRead;

use anyhow::bail;

use crate::model::{ClusterState, Digest, NodeState, ScuttleButtMessage};

// https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf

/// HashMap key for the heartbeat node value.
pub(crate) const HEARTBEAT_KEY: &str = "heartbeat";

pub struct ScuttleButt {
    max_transmitted_key_values: usize, // mtu in the paper
    self_node_id: String,
    cluster_state_map: ClusterState,
    heartbeat: u64,
}

impl ScuttleButt {
    pub fn with_node_id(self_node_id: String) -> Self {
        let mut scuttlebutt = ScuttleButt {
            max_transmitted_key_values: 10,
            heartbeat: 0,
            self_node_id,
            cluster_state_map: ClusterState::default(),
        };

        // Immediately mark node as alive to ensure it responds to SYNs.
        scuttlebutt.self_node_state().set(HEARTBEAT_KEY, 0);

        scuttlebutt
    }

    pub fn set_max_num_key_values(&mut self, max_transmitted_key_values: usize) {
        self.max_transmitted_key_values = max_transmitted_key_values;
    }

    pub fn create_syn_message(&mut self) -> ScuttleButtMessage {
        let digest = self.compute_digest();
        ScuttleButtMessage::Syn { digest }
    }

    pub fn process_message(&mut self, msg: ScuttleButtMessage) -> Option<ScuttleButtMessage> {
        match msg {
            ScuttleButtMessage::Syn { digest } => {
                let delta = self
                    .cluster_state_map
                    .compute_delta(&digest, self.max_transmitted_key_values);
                let digest = self.compute_digest();
                Some(ScuttleButtMessage::SynAck { delta, digest })
            }
            ScuttleButtMessage::SynAck { digest, delta } => {
                self.cluster_state_map.apply_delta(delta);
                let delta = self
                    .cluster_state_map
                    .compute_delta(&digest, self.max_transmitted_key_values);
                Some(ScuttleButtMessage::Ack { delta })
            }
            ScuttleButtMessage::Ack { delta } => {
                self.cluster_state_map.apply_delta(delta);
                None
            }
        }
    }

    pub fn node_state(&self, node_id: &str) -> Option<&NodeState> {
        self.cluster_state_map.node_state(node_id)
    }

    pub fn self_node_state(&mut self) -> &mut NodeState {
        self.cluster_state_map.node_state_mut(&self.self_node_id)
    }

    /// Retrieve a list of all living nodes.
    pub fn living_nodes(&self) -> impl Iterator<Item = &str> {
        self.cluster_state_map.living_nodes()
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

        self.cluster_state_map.compute_digest()
    }
}

pub(crate) fn read_u64(buf: &mut &[u8]) -> anyhow::Result<u64> {
    if buf.len() < 8 {
        bail!("Buffer two short");
    }
    let val_bytes: [u8; 8] = buf[0..8].try_into()?;
    let val = u64::from_le_bytes(val_bytes);
    buf.consume(8);
    Ok(val)
}

pub(crate) fn read_u16(buf: &mut &[u8]) -> anyhow::Result<u16> {
    if buf.len() < 2 {
        bail!("Buffer two short");
    }
    let val = u16::from_le_bytes([buf[0], buf[1]]);
    buf.consume(2);
    Ok(val)
}

pub(crate) fn read_str<'a>(buf: &mut &'a [u8]) -> anyhow::Result<&'a str> {
    let len = read_u16(buf)? as usize;
    let s = std::str::from_utf8(&buf[..len])?;
    buf.consume(len as usize);
    Ok(s)
}

pub(crate) fn write_u16(val: u16, buf: &mut Vec<u8>) {
    buf.extend(&val.to_le_bytes());
}

pub(crate) fn write_u64(val: u64, buf: &mut Vec<u8>) {
    buf.extend(&val.to_le_bytes());
}

pub(crate) fn write_str<'a>(s: &str, buf: &mut Vec<u8>) {
    write_u16(s.len() as u16, buf);
    buf.extend(s.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::hash::Hash;

    fn run_scuttlebutt_handshake(initiating_node: &mut ScuttleButt, peer_node: &mut ScuttleButt) {
        let syn_message = initiating_node.create_syn_message();
        let syn_ack_message = peer_node.process_message(syn_message).unwrap();
        let ack_message = initiating_node.process_message(syn_ack_message).unwrap();
        assert!(peer_node.process_message(ack_message).is_none());
    }

    fn assert_map_eq<K, V>(lhs: &HashMap<K, V>, rhs: &HashMap<K, V>)
    where
        K: Eq + Hash,
        V: PartialEq + Debug,
    {
        assert_eq!(lhs.len(), rhs.len());
        for (key, value) in lhs {
            assert_eq!(rhs.get(key), Some(value));
        }
    }

    fn assert_nodes_sync(nodes: &[&ScuttleButt]) {
        let first_node_states = &nodes[0].cluster_state_map.node_states;
        for other_node in nodes.iter().skip(1) {
            let node_states = &other_node.cluster_state_map.node_states;

            assert_eq!(first_node_states.len(), node_states.len());
            for (key, value) in first_node_states {
                assert_map_eq(&value.key_values, &node_states.get(key).unwrap().key_values);
            }
        }
    }

    #[test]
    fn test_scuttlebutt_handshake() {
        let mut node1 = ScuttleButt::with_node_id("node1".to_string());
        {
            let state1 = node1.self_node_state();
            state1.set("key1a", "1");
            state1.set("key2a", "2");
        }
        let mut node2 = ScuttleButt::with_node_id("node2".to_string());
        {
            let state2 = node2.self_node_state();
            state2.set("key1b", "1");
            state2.set("key2b", "2");
        }
        run_scuttlebutt_handshake(&mut node1, &mut node2);
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
