use anyhow::Context;
use rand::prelude::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::hash_map::{Entry, Keys};
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::io::BufRead;
use std::time::{Duration, Instant};
use std::{iter, mem};

use crate::{read_str, read_u16, read_u64, write_str, write_u16, write_u64};

/// Maximum heartbeat age before a node is considered dead.
const MAX_HEARTBEAT_DELTA: Duration = Duration::from_secs(10);

pub type Version = u64;

/// A versioned value for a given Key-value pair.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct VersionedValue {
    pub value: String,
    pub version: Version,
}

#[derive(Serialize, Deserialize, Default, Eq, PartialEq, Debug)]
pub(crate) struct NodeDelta {
    pub key_values: BTreeMap<String, VersionedValue>,
}

pub struct DeltaWriter {
    delta: Delta,
    mtu: usize,
    num_bytes: usize,
    current_node_id: String,
    current_node_delta: NodeDelta,
    reached_capacity: bool,
}

impl DeltaWriter {
    fn with_mtu(mtu: usize) -> Self {
        DeltaWriter {
            delta: Delta::default(),
            mtu,
            num_bytes: 2,
            current_node_id: String::new(),
            current_node_delta: NodeDelta::default(),
            reached_capacity: false,
        }
    }

    fn flush(&mut self) {
        let node_id = mem::replace(&mut self.current_node_id, String::new());
        let node_delta = mem::replace(&mut self.current_node_delta, NodeDelta::default());
        if node_id.is_empty() {
            return;
        }
        self.delta.node_deltas.insert(node_id, node_delta);
    }

    pub fn add_node(&mut self, node_id: &str) -> bool {
        assert!(!node_id.is_empty());
        assert!(node_id != &self.current_node_id);
        assert!(!self.delta.node_deltas.contains_key(node_id));
        self.flush();
        if !self.attempt_add_bytes(2 + node_id.len() + 2) {
            return false;
        }
        self.current_node_id = node_id.to_string();
        true
    }

    fn attempt_add_bytes(&mut self, num_bytes: usize) -> bool {
        assert!(self.reached_capacity);
        let new_num_bytes = self.num_bytes + num_bytes;
        if new_num_bytes > self.mtu {
            self.reached_capacity = true;
            return false;
        }
        self.num_bytes = new_num_bytes;
        true
    }

    pub fn add_kv(&mut self, key: &str, versioned_value: VersionedValue) -> bool {
        assert!(!self.current_node_delta.key_values.contains_key(key));
        if !self.attempt_add_bytes(2 + key.len() + 2 + versioned_value.value.len() + 8) {
            return false;
        }
        self.current_node_delta
            .key_values
            .insert(key.to_string(), versioned_value);
        true
    }

    pub fn to_delta(mut self) -> Delta {
        self.flush();
        if cfg!(debug_assertions) {
            let mut buf = Vec::new();
            self.delta.serialize(&mut buf);
            assert_eq!(buf.len(), self.num_bytes);
        }
        self.delta
    }
}

impl NodeDelta {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        write_u16(self.key_values.len() as u16, buf);
        for (key, VersionedValue { value, version }) in &self.key_values {
            write_str(key, buf);
            write_str(value, buf);
            write_u64(*version, buf);
        }
    }

    pub fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let mut key_values: BTreeMap<String, VersionedValue> = Default::default();
        let num_kvs = read_u16(buf)?;
        for _ in 0..num_kvs {
            let key = read_str(buf)?.to_string();
            let value = read_str(buf)?.to_string();
            let version = read_u64(buf)?;
            key_values.insert(key, VersionedValue { value, version });
        }
        Ok(NodeDelta { key_values })
    }
}

#[derive(Serialize, Deserialize, Default, Eq, PartialEq, Debug)]
pub struct Delta {
    pub(crate) node_deltas: BTreeMap<String, NodeDelta>,
}

impl Delta {
    #[cfg(test)]
    pub fn add_node_delta(&mut self, node_id: &str, key: &str, value: &str, version: Version) {
        self.node_deltas
            .entry(node_id.to_string())
            .or_default()
            .key_values
            .insert(
                key.to_string(),
                VersionedValue {
                    value: value.to_string(),
                    version,
                },
            );
    }

    pub fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let mut node_deltas: BTreeMap<String, NodeDelta> = Default::default();
        let num_nodes = read_u16(buf)?;
        for _ in 0..num_nodes {
            let node_id = read_str(buf)?;
            let node_delta = NodeDelta::deserialize(buf)?;
            node_deltas.insert(node_id.to_string(), node_delta);
        }
        Ok(Delta { node_deltas })
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        write_u16(self.node_deltas.len() as u16, buf);
        for (node_id, node_delta) in &self.node_deltas {
            write_str(node_id, buf);
            node_delta.serialize(buf);
        }
    }
}

/// A digest represents is a piece of information summarizing
/// the staleness of one peer's data.
///
/// It is equivalent to a map
/// peer -> max version.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Digest {
    pub(crate) node_max_version: BTreeMap<String, Version>,
}

impl Digest {
    #[cfg(test)]
    pub fn add_node(&mut self, node: &str, max_version: Version) {
        self.node_max_version.insert(node.to_string(), max_version);
    }

    pub fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let num_nodes = read_u16(buf)?;
        let mut node_max_version: BTreeMap<String, Version> = Default::default();
        for _ in 0..num_nodes {
            let node_id = read_str(buf)?;
            let version = read_u64(buf)?;
            node_max_version.insert(node_id.to_string(), version);
        }
        Ok(Digest { node_max_version })
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        write_u16(self.node_max_version.len() as u16, buf);
        for (node_id, version) in &self.node_max_version {
            write_str(node_id, buf);
            write_u64(*version, buf);
        }
    }
}

/// ScuttleButt message.
///
/// Each variant represents a step of the gossip "handshake"
/// between node A and node B.
/// The names {Syn, SynAck, Ack} of the different steps are borrowed from
/// TCP Handshake.
#[derive(Debug, Serialize, Deserialize)]
pub enum ScuttleButtMessage {
    /// Node A initiates handshakes.
    Syn { digest: Digest },
    /// Node B returns a partial update as described
    /// in the scuttlebutt reconcialiation algorithm,
    /// and returns its own checksum.
    SynAck { digest: Digest, delta: Delta },
    /// Node A returns a partial update for B.
    Ack { delta: Delta },
}

#[repr(u8)]
enum MessageType {
    Syn = 0,
    SynAck = 1u8,
    Ack = 2u8,
}

impl MessageType {
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Syn),
            1 => Some(Self::SynAck),
            2 => Some(Self::Ack),
            _ => None,
        }
    }
}

impl ScuttleButtMessage {
    pub fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let code = buf
            .get(0)
            .cloned()
            .and_then(MessageType::from_code)
            .context("Invalid message type")?;
        buf.consume(1);
        match code {
            MessageType::Syn => {
                let digest = Digest::deserialize(buf)?;
                Ok(Self::Syn { digest })
            }
            MessageType::SynAck => {
                let digest = Digest::deserialize(buf)?;
                let delta = Delta::deserialize(buf)?;
                Ok(Self::SynAck { digest, delta })
            }
            MessageType::Ack => {
                let delta = Delta::deserialize(buf)?;
                Ok(Self::Ack { delta })
            }
        }
    }
}

pub struct NodeState {
    pub(crate) key_values: HashMap<String, VersionedValue>,
    last_heartbeat: Instant,
    max_version: u64,
}

impl Default for NodeState {
    fn default() -> Self {
        Self {
            last_heartbeat: Instant::now(),
            max_version: Default::default(),
            key_values: Default::default(),
        }
    }
}

impl NodeState {
    /// Returns an iterator over the version values that are older than `floor_version`.
    fn iter_stale_key_values(
        &self,
        floor_version: u64,
    ) -> impl Iterator<Item = (&str, &VersionedValue)> {
        // TODO optimize by checking the max version.
        self.key_values
            .iter()
            .filter(move |&(_key, versioned_value)| versioned_value.version > floor_version)
            .map(|(key, record)| (key.as_str(), record))
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.get_versioned(key.borrow())
            .map(|versioned_value| versioned_value.value.as_str())
    }

    pub fn get_versioned(&self, key: &str) -> Option<&VersionedValue> {
        self.key_values.get(key)
    }

    /// Sets a new value for a given key.
    ///
    /// Setting a new value automatically increments the
    /// version of the entire NodeState regardless of whether the
    /// value is really changed or not.
    pub fn set<K: ToString, V: ToString>(&mut self, key: K, value: V) {
        let new_version = self.max_version + 1;
        self.set_with_version(key.to_string(), value.to_string(), new_version);
    }

    fn set_with_version(&mut self, key: String, value: String, version: Version) {
        assert!(version > self.max_version);
        self.max_version = version;
        self.key_values
            .insert(key, VersionedValue { version, value });
    }
}

#[derive(Default)]
pub(crate) struct ClusterState {
    pub(crate) node_states: HashMap<String, NodeState>,
}

impl ClusterState {
    pub fn node_state_mut(&mut self, node_id: &str) -> &mut NodeState {
        // TODO use the `hash_raw_entry` feature once it gets stabilized.
        self.node_states.entry(node_id.to_string()).or_default()
    }

    pub fn node_state(&self, node_id: &str) -> Option<&NodeState> {
        self.node_states.get(node_id)
    }

    /// Retrieve a list of all living nodes.
    pub fn living_nodes(&self) -> impl Iterator<Item = &str> {
        self.node_states.iter().filter_map(|(node_id, node_state)| {
            (node_state.last_heartbeat.elapsed() <= MAX_HEARTBEAT_DELTA).then(|| node_id.as_str())
        })
    }

    pub fn nodes(&self) -> Keys<'_, String, NodeState> {
        self.node_states.keys()
    }

    pub fn apply_delta(&mut self, delta: Delta) {
        for (node_id, node_delta) in delta.node_deltas {
            let mut node_state_map = self
                .node_states
                .entry(node_id)
                .or_insert_with(NodeState::default);

            for (key, versioned_value) in node_delta.key_values {
                node_state_map.max_version =
                    node_state_map.max_version.max(versioned_value.version);
                let entry = node_state_map.key_values.entry(key);
                match entry {
                    Entry::Occupied(mut record) => {
                        if record.get().version >= versioned_value.version {
                            // Due to the message passing being totally asynchronous, it is not an
                            // error to receive updates that are already obsolete.
                            continue;
                        }
                        record.insert(versioned_value);
                    }
                    Entry::Vacant(vacant) => {
                        vacant.insert(versioned_value);
                    }
                }
            }

            node_state_map.last_heartbeat = Instant::now();
        }
    }

    pub fn compute_digest(&self) -> Digest {
        Digest {
            node_max_version: self
                .node_states
                .iter()
                .map(|(node_id, node_state)| (node_id.clone(), node_state.max_version))
                .collect(),
        }
    }

    /// Implements the scuttlebutt reconcialiation with the scuttle-depth ordering.
    pub fn compute_delta(&self, digest: &Digest, mtu: usize) -> Delta {
        let mut delta_writer = DeltaWriter::with_mtu(mtu);

        let mut node_sorted_by_stale_length = NodeSortedByStaleLength::default();
        for (node_id, node_state_map) in &self.node_states {
            let floor_version = digest.node_max_version.get(node_id).cloned().unwrap_or(0);
            let stale_kv_count = node_state_map.iter_stale_key_values(floor_version).count();
            if stale_kv_count > 0 {
                node_sorted_by_stale_length.insert(node_id, stale_kv_count);
            }
        }

        for node_id in node_sorted_by_stale_length.into_iter() {
            if !delta_writer.add_node(node_id) {
                break;
            }
            let node_state_map = self.node_states.get(node_id).unwrap();
            let floor_version = digest.node_max_version.get(node_id).cloned().unwrap_or(0);
            let mut stale_kvs: Vec<(&str, &VersionedValue)> = node_state_map
                .iter_stale_key_values(floor_version)
                .collect();
            assert!(!stale_kvs.is_empty());
            stale_kvs.sort_unstable_by_key(|(_, record)| record.version);
            for (key, versioned_value) in stale_kvs {
                if !delta_writer.add_kv(key, versioned_value.clone()) {
                    return delta_writer.to_delta();
                }
            }
        }
        delta_writer.to_delta()
    }
}

#[derive(Default)]
struct NodeSortedByStaleLength<'a> {
    node_per_stale_length: HashMap<usize, Vec<&'a str>>,
    stale_lengths: BinaryHeap<usize>,
}

#[cfg(not(test))]
fn random_generator() -> impl Rng {
    rand::thread_rng()
}

// We use a deterministic random generator in tests.
#[cfg(test)]
fn random_generator() -> impl Rng {
    use rand::prelude::StdRng;
    use rand::SeedableRng;
    StdRng::seed_from_u64(9u64)
}

impl<'a> NodeSortedByStaleLength<'a> {
    fn insert(&mut self, node_id: &'a str, stale_length: usize) {
        self.node_per_stale_length
            .entry(stale_length)
            .or_insert_with(|| {
                self.stale_lengths.push(stale_length);
                Vec::new()
            })
            .push(node_id);
    }

    fn into_iter(mut self) -> impl Iterator<Item = &'a str> {
        let mut rng = random_generator();
        iter::from_fn(move || self.stale_lengths.pop()).flat_map(move |length| {
            let mut nodes = self.node_per_stale_length.remove(&length).unwrap();
            nodes.shuffle(&mut rng);
            nodes.into_iter()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::NodeSortedByStaleLength;
    use crate::model::ClusterState;
    use crate::model::Delta;
    use crate::model::DeltaWriter;
    use crate::model::Digest;
    use crate::model::Version;
    use crate::model::VersionedValue;
    use std::collections::BTreeMap;

    #[test]
    fn test_node_sorted_by_stale_length_empty() {
        let node_sorted_by_stale_length = NodeSortedByStaleLength::default();
        assert!(node_sorted_by_stale_length.into_iter().next().is_none());
    }

    #[test]
    fn test_node_sorted_by_stale_length_simple() {
        let mut node_sorted_by_stale_length = NodeSortedByStaleLength::default();
        node_sorted_by_stale_length.insert("node1", 1);
        node_sorted_by_stale_length.insert("node2", 2);
        node_sorted_by_stale_length.insert("node3", 3);
        let nodes: Vec<&str> = node_sorted_by_stale_length.into_iter().collect();
        assert_eq!(&nodes, &["node3", "node2", "node1"]);
    }

    #[test]
    fn test_node_sorted_by_stale_length_doubles() {
        let mut node_sorted_by_stale_length = NodeSortedByStaleLength::default();
        node_sorted_by_stale_length.insert("node1", 1);
        node_sorted_by_stale_length.insert("node2a", 2);
        node_sorted_by_stale_length.insert("node2b", 2);
        node_sorted_by_stale_length.insert("node2c", 2);
        let nodes: Vec<&str> = node_sorted_by_stale_length.into_iter().collect();
        assert_eq!(&nodes, &["node2b", "node2a", "node2c", "node1"]);
    }

    #[test]
    fn test_cluster_state_missing_node() {
        let cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state("node");
        assert!(node_state.is_none());
    }

    #[test]
    fn test_cluster_state_first_version_is_one() {
        let mut cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state_mut("node");
        node_state.set("key_a", "");
        assert_eq!(
            node_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "".to_string(),
                version: 1
            }
        );
    }

    #[test]
    fn test_cluster_state_set() {
        let mut cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state_mut("node");
        node_state.set("key_a", "1");
        assert_eq!(
            node_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "1".to_string(),
                version: 1
            }
        );
        node_state.set("key_b", "2");
        assert_eq!(
            node_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "1".to_string(),
                version: 1
            }
        );
        assert_eq!(
            node_state.get_versioned("key_b").unwrap(),
            &VersionedValue {
                value: "2".to_string(),
                version: 2
            }
        );
        node_state.set("key_a", "3");
        assert_eq!(
            node_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "3".to_string(),
                version: 3
            }
        );
    }

    #[test]
    fn test_cluster_state_set_with_same_value_updates_version() {
        let mut cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state_mut("node");
        node_state.set("key", "1");
        assert_eq!(
            node_state.get_versioned("key").unwrap(),
            &VersionedValue {
                value: "1".to_string(),
                version: 1
            }
        );
        node_state.set("key", "1");
        assert_eq!(
            node_state.get_versioned("key").unwrap(),
            &VersionedValue {
                value: "1".to_string(),
                version: 2
            }
        );
    }

    #[test]
    fn test_cluster_state_compute_digest() {
        let mut cluster_state = ClusterState::default();

        let node1_state = cluster_state.node_state_mut("node1");
        node1_state.set("key_a", "");
        node1_state.set("key_b", "");

        let node2_state = cluster_state.node_state_mut("node2");
        node2_state.set("key_a", "");

        let digest = cluster_state.compute_digest();
        let mut node_max_version_map = BTreeMap::default();
        node_max_version_map.insert("node1".to_string(), 2);
        node_max_version_map.insert("node2".to_string(), 1);
        assert_eq!(&digest.node_max_version, &node_max_version_map);
    }

    #[test]
    fn test_cluster_state_apply_delta() {
        let mut cluster_state = ClusterState::default();

        let node1_state = cluster_state.node_state_mut("node1");
        node1_state.set_with_version("key_a".to_string(), "1".to_string(), 1); // 1
        node1_state.set_with_version("key_b".to_string(), "3".to_string(), 3); // 3

        let mut delta = Delta::default();
        delta.add_node_delta("node1", "key_a", "4", 4);
        delta.add_node_delta("node1", "key_b", "2", 2);
        cluster_state.apply_delta(delta);

        let node1_state = cluster_state.node_state("node1").unwrap();
        assert_eq!(
            node1_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "4".to_string(),
                version: 4
            }
        );
        // We ignore stale values.
        assert_eq!(
            node1_state.get_versioned("key_b").unwrap(),
            &VersionedValue {
                value: "3".to_string(),
                version: 3
            }
        );
    }

    // This helper test function will test all possible mtu version, and check that the resulting
    // delta matches the expectation.
    fn test_with_varying_max_transmitted_kv_helper(
        cluster_state: &ClusterState,
        digest: &Digest,
        expected_delta_atoms: &[(&str, &str, &str, Version)],
    ) {
        let max_delta = cluster_state.compute_delta(&digest, usize::MAX);
        for max_transmitted_key_values in 1..=expected_delta_atoms.len() {
            let delta = cluster_state.compute_delta(&digest, max_transmitted_key_values);
            let mut expected_delta = Delta::default();
            for &(node, key, val, version) in &expected_delta_atoms[..max_transmitted_key_values] {
                expected_delta.add_node_delta(node, key, val, version);
            }
            assert_eq!(&delta, &expected_delta);
        }
    }

    fn test_cluster_state() -> ClusterState {
        let mut cluster_state = ClusterState::default();

        let node1_state = cluster_state.node_state_mut("node1");
        node1_state.set_with_version("key_a".to_string(), "1".to_string(), 1); // 1
        node1_state.set_with_version("key_b".to_string(), "2".to_string(), 2); // 3

        let node2_state = cluster_state.node_state_mut("node2");
        node2_state.set_with_version("key_a".to_string(), "1".to_string(), 1); // 1
        node2_state.set_with_version("key_b".to_string(), "2".to_string(), 2); // 2
        node2_state.set_with_version("key_c".to_string(), "3".to_string(), 3); // 3
        node2_state.set_with_version("key_d".to_string(), "4".to_string(), 4); // 4

        cluster_state
    }

    #[test]
    fn test_cluster_state_compute_delta_depth_first_single_node() {
        let cluster_state = test_cluster_state();
        let mut digest = Digest::default();
        digest.add_node("node1", 1);
        digest.add_node("node2", 2);
        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            &[
                ("node2", "key_c", "3", 3),
                ("node2", "key_d", "4", 4),
                ("node1", "key_b", "2", 2),
            ],
        );
    }

    #[test]
    fn test_cluster_state_compute_delta_depth_first_scuttlebutt() {
        let cluster_state = test_cluster_state();
        let mut digest = Digest::default();
        digest.add_node("node1", 1);
        digest.add_node("node2", 2);
        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            &[
                ("node2", "key_c", "3", 3),
                ("node2", "key_d", "4", 4),
                ("node1", "key_b", "2", 2),
            ],
        );
    }

    #[test]
    fn test_cluster_state_compute_delta_missing_node() {
        let cluster_state = test_cluster_state();
        let mut digest = Digest::default();
        digest.add_node("node2", 3);
        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            &[
                ("node1", "key_a", "1", 1),
                ("node1", "key_b", "2", 2),
                ("node2", "key_d", "4", 4),
            ],
        );
    }

    fn test_delta_serder_aux(delta: &Delta, num_bytes: usize) {
        let mut buf = Vec::new();
        delta.serialize(&mut buf);
        assert_eq!(buf.len(), num_bytes);
        let delta_serdeser = Delta::deserialize(&mut &buf[..]).unwrap();
        assert_eq!(delta, &delta_serdeser);
    }

    #[test]
    fn test_delta_serialization_default() {
        test_delta_serder_aux(&Default::default(), 2);
    }

    #[test]
    fn test_delta_serialization_simple() {
        let mut delta_writer = DeltaWriter::with_mtu(108);
        delta_writer.add_node("node1");
        delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
            },
        );
        delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
            },
        );
        delta_writer.add_node("node2");
        delta_writer.add_kv(
            "key21",
            VersionedValue {
                value: "val21".to_string(),
                version: 2,
            },
        );
        delta_writer.add_kv(
            "key22",
            VersionedValue {
                value: "val22".to_string(),
                version: 3,
            },
        );
        let delta = delta_writer.to_delta();
        test_delta_serder_aux(&delta, 108);
    }

    #[test]
    fn test_delta_serialization_simple_node() {
        let mut delta_writer = DeltaWriter::with_mtu(64);
        assert!(delta_writer.add_node("node1"));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1
            }
        ));
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2
            }
        ));
        assert!(delta_writer.add_node("node2"));
        let delta = delta_writer.to_delta();
        test_delta_serder_aux(&delta, 64);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_node() {
        let mut delta_writer = DeltaWriter::with_mtu(63);
        assert!(delta_writer.add_node("node1"));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1
            }
        ));
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2
            }
        ));
        assert!(!delta_writer.add_node("node2"));
        let delta = delta_writer.to_delta();
        test_delta_serder_aux(&delta, 55);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_kv() {
        let mut delta_writer = DeltaWriter::with_mtu(54);
        assert!(delta_writer.add_node("node1"));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1
            }
        ));
        assert!(!delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2
            }
        ));
        let delta = delta_writer.to_delta();
        test_delta_serder_aux(&delta, 33);
    }

    #[test]
    #[should_panic]
    fn test_delta_serialization_panic_if_add_after_exceed() {
        let mut delta_writer = DeltaWriter::with_mtu(54);
        assert!(delta_writer.add_node("node1"));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1
            }
        ));
        assert!(!delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2
            }
        ));
        delta_writer.add_kv(
            "key13",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
            },
        );
    }
}
