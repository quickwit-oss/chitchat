use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::net::SocketAddr;
use std::time::Instant;

use rand::prelude::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::delta::{Delta, DeltaWriter};
use crate::digest::Digest;
use crate::{NodeId, Version, VersionedValue};

/// Maximum value size (in bytes) for a key-value item.
const MAX_KV_VALUE_SIZE: usize = 500;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct NodeState {
    pub(crate) key_values: BTreeMap<String, VersionedValue>,
    #[serde(skip)]
    #[serde(default = "Instant::now")]
    last_heartbeat: Instant,
    pub(crate) max_version: u64,
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
        self.get_versioned(key)
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
        let value_size = value.bytes().len();
        assert!(
            value_size <= MAX_KV_VALUE_SIZE,
            "Value for key `{}` is too large (actual: {}, maximum: {})",
            key,
            value_size,
            MAX_KV_VALUE_SIZE
        );
        self.max_version = version;
        self.key_values
            .insert(key, VersionedValue { version, value });
    }
}

#[derive(Debug)]
pub(crate) struct ClusterState {
    pub(crate) node_states: BTreeMap<NodeId, NodeState>,
    seed_addrs: watch::Receiver<HashSet<SocketAddr>>,
}

#[cfg(test)]
impl Default for ClusterState {
    fn default() -> Self {
        let (_seed_addrs_tx, seed_addrs_rx) = watch::channel(Default::default());
        Self {
            node_states: Default::default(),
            seed_addrs: seed_addrs_rx,
        }
    }
}

impl ClusterState {
    pub fn with_seed_addrs(seed_addrs: watch::Receiver<HashSet<SocketAddr>>) -> ClusterState {
        ClusterState {
            seed_addrs,
            node_states: BTreeMap::new(),
        }
    }

    pub(crate) fn node_state_mut(&mut self, node_id: &NodeId) -> &mut NodeState {
        // TODO use the `hash_raw_entry` feature once it gets stabilized.
        self.node_states.entry(node_id.clone()).or_default()
    }

    pub fn node_state(&self, node_id: &NodeId) -> Option<&NodeState> {
        self.node_states.get(node_id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.node_states.keys()
    }

    pub fn seed_addrs(&self) -> HashSet<SocketAddr> {
        self.seed_addrs.borrow().clone()
    }

    pub(crate) fn remove_node(&mut self, node_id: &NodeId) {
        self.node_states.remove(node_id);
    }

    pub(crate) fn apply_delta(&mut self, delta: Delta) {
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

    pub fn compute_digest(&self, dead_nodes: HashSet<&NodeId>) -> Digest {
        Digest {
            node_max_version: self
                .node_states
                .iter()
                .filter(|(node_id, _)| !dead_nodes.contains(node_id))
                .map(|(node_id, node_state)| (node_id.clone(), node_state.max_version))
                .collect(),
        }
    }

    /// Implements the scuttlebutt reconciliation with the scuttle-depth ordering.
    pub fn compute_delta(
        &self,
        digest: &Digest,
        mtu: usize,
        dead_nodes: HashSet<&NodeId>,
    ) -> Delta {
        let mut delta_writer = DeltaWriter::with_mtu(mtu);

        let mut node_sorted_by_stale_length = NodeSortedByStaleLength::default();
        for (node_id, node_state_map) in &self.node_states {
            if dead_nodes.contains(node_id) {
                continue;
            }
            let floor_version = digest.node_max_version.get(node_id).cloned().unwrap_or(0);
            let stale_kv_count = node_state_map.iter_stale_key_values(floor_version).count();
            if stale_kv_count > 0 {
                node_sorted_by_stale_length.insert(node_id, stale_kv_count);
            }
        }

        for node_id in node_sorted_by_stale_length.into_iter() {
            if !delta_writer.add_node(node_id.clone()) {
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
                    return delta_writer.into();
                }
            }
        }
        delta_writer.into()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClusterStateSnapshot {
    pub seed_addrs: HashSet<SocketAddr>,
    pub node_states: BTreeMap<String, NodeState>,
}

impl<'a> From<&'a ClusterState> for ClusterStateSnapshot {
    fn from(state: &'a ClusterState) -> Self {
        ClusterStateSnapshot {
            seed_addrs: state.seed_addrs(),
            node_states: state
                .node_states
                .iter()
                .map(|(node_id, node_state)| (node_id.id.clone(), node_state.clone()))
                .collect(),
        }
    }
}

#[derive(Default)]
struct NodeSortedByStaleLength<'a> {
    node_per_stale_length: BTreeMap<usize, Vec<&'a NodeId>>,
    stale_lengths: BinaryHeap<usize>,
}

impl<'a> NodeSortedByStaleLength<'a> {
    fn insert(&mut self, node_id: &'a NodeId, stale_length: usize) {
        self.node_per_stale_length
            .entry(stale_length)
            .or_insert_with(|| {
                self.stale_lengths.push(stale_length);
                Vec::new()
            })
            .push(node_id);
    }

    fn into_iter(mut self) -> impl Iterator<Item = &'a NodeId> {
        let mut rng = random_generator();
        std::iter::from_fn(move || self.stale_lengths.pop()).flat_map(move |length| {
            let mut nodes = self.node_per_stale_length.remove(&length).unwrap();
            nodes.shuffle(&mut rng);
            nodes.into_iter()
        })
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialize::Serializable;

    #[test]
    fn test_node_sorted_by_stale_length_empty() {
        let node_sorted_by_stale_length = NodeSortedByStaleLength::default();
        assert!(node_sorted_by_stale_length.into_iter().next().is_none());
    }

    #[test]
    fn test_node_sorted_by_stale_length_simple() {
        let mut node_sorted_by_stale_length = NodeSortedByStaleLength::default();
        let node_ids = vec![(10_001, 1), (10_002, 2), (10_003, 3)]
            .into_iter()
            .map(|(port, state_length)| (NodeId::for_test_localhost(port), state_length))
            .collect::<Vec<_>>();
        for (node_id, state_length) in node_ids.iter() {
            node_sorted_by_stale_length.insert(node_id, *state_length);
        }
        let nodes: Vec<&NodeId> = node_sorted_by_stale_length.into_iter().collect();
        let expected_nodes: Vec<NodeId> = [10_003, 10_002, 10_001]
            .into_iter()
            .map(NodeId::for_test_localhost)
            .collect();
        assert_eq!(nodes, expected_nodes.iter().collect::<Vec<_>>());
    }

    #[test]
    fn test_node_sorted_by_stale_length_doubles() {
        let mut node_sorted_by_stale_length = NodeSortedByStaleLength::default();
        let node_ids = vec![(10_001, 1), (20_001, 2), (20_002, 2), (20_003, 2)]
            .into_iter()
            .map(|(port, state_length)| (NodeId::for_test_localhost(port), state_length))
            .collect::<Vec<_>>();

        for (node_id, state_length) in node_ids.iter() {
            node_sorted_by_stale_length.insert(node_id, *state_length);
        }

        let nodes: Vec<NodeId> = node_sorted_by_stale_length.into_iter().cloned().collect();
        let expected_nodes: Vec<NodeId> = vec![20_002, 20_001, 20_003, 10_001]
            .into_iter()
            .map(NodeId::for_test_localhost)
            .collect();
        assert_eq!(nodes, expected_nodes);
    }

    #[test]
    fn test_cluster_state_missing_node() {
        let cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state(&NodeId::for_test_localhost(10_001));
        assert!(node_state.is_none());
    }

    #[test]
    fn test_cluster_state_first_version_is_one() {
        let mut cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state_mut(&NodeId::for_test_localhost(10_001));
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
        let node_state = cluster_state.node_state_mut(&NodeId::for_test_localhost(10_001));
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
    #[should_panic(expected = "Value for key `text` is too large (actual: 528, maximum: 500)")]
    fn test_cluster_state_set_with_large_value() {
        let mut cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state_mut(&NodeId::for_test_localhost(10_001));
        let large_value = "The quick brown fox jumps over the lazy dog.".repeat(12);
        node_state.set("text", large_value);
    }

    #[test]
    fn test_cluster_state_set_with_same_value_updates_version() {
        let mut cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state_mut(&NodeId::for_test_localhost(10_001));
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
        let node1 = NodeId::for_test_localhost(10_001);
        let node1_state = cluster_state.node_state_mut(&node1);
        node1_state.set("key_a", "");
        node1_state.set("key_b", "");

        let node2 = NodeId::for_test_localhost(10_002);
        let node2_state = cluster_state.node_state_mut(&node2);
        node2_state.set("key_a", "");

        let digest = cluster_state.compute_digest(HashSet::new());
        let mut node_max_version_map = BTreeMap::default();
        node_max_version_map.insert(node1.clone(), 2);
        node_max_version_map.insert(node2.clone(), 1);
        assert_eq!(&digest.node_max_version, &node_max_version_map);

        // exclude node1
        let dead_nodes = vec![node1];
        let digest = cluster_state.compute_digest(dead_nodes.iter().collect());
        let mut node_max_version_map = BTreeMap::default();
        node_max_version_map.insert(node2, 1);
        assert_eq!(&digest.node_max_version, &node_max_version_map);
    }

    #[test]
    fn test_cluster_state_apply_delta() {
        let mut cluster_state = ClusterState::default();

        let node1 = NodeId::for_test_localhost(10_001);
        let node1_state = cluster_state.node_state_mut(&node1);
        node1_state.set_with_version("key_a".to_string(), "1".to_string(), 1); // 1
        node1_state.set_with_version("key_b".to_string(), "3".to_string(), 3); // 3

        let mut delta = Delta::default();
        delta.add_node_delta(node1.clone(), "key_a", "4", 4);
        delta.add_node_delta(node1.clone(), "key_b", "2", 2);
        cluster_state.apply_delta(delta);

        let node1_state = cluster_state.node_state(&node1).unwrap();
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
        exclude_node_ids: HashSet<&NodeId>,
        expected_delta_atoms: &[(&NodeId, &str, &str, Version)],
    ) {
        let max_delta = cluster_state.compute_delta(digest, usize::MAX, exclude_node_ids.clone());
        let mut buf = Vec::new();
        max_delta.serialize(&mut buf);
        let mut mtu_per_num_entries = Vec::new();
        for mtu in 2..buf.len() {
            let delta = cluster_state.compute_delta(digest, mtu, exclude_node_ids.clone());
            let num_tuples = delta.num_tuples();
            if mtu_per_num_entries.len() == num_tuples + 1 {
                continue;
            }
            buf.clear();
            delta.serialize(&mut buf);
            mtu_per_num_entries.push(buf.len());
        }
        for (num_entries, &mtu) in mtu_per_num_entries.iter().enumerate() {
            let mut expected_delta = Delta::default();
            for &(node, key, val, version) in &expected_delta_atoms[..num_entries] {
                expected_delta.add_node_delta(node.clone(), key, val, version);
            }
            {
                let delta = cluster_state.compute_delta(digest, mtu, exclude_node_ids.clone());
                assert_eq!(&delta, &expected_delta);
            }
            {
                let delta = cluster_state.compute_delta(digest, mtu + 1, exclude_node_ids.clone());
                assert_eq!(&delta, &expected_delta);
            }
        }
    }

    fn test_cluster_state() -> ClusterState {
        let mut cluster_state = ClusterState::default();

        let node1 = NodeId::for_test_localhost(10_001);
        let node1_state = cluster_state.node_state_mut(&node1);
        node1_state.set_with_version("key_a".to_string(), "1".to_string(), 1); // 1
        node1_state.set_with_version("key_b".to_string(), "2".to_string(), 2); // 3

        let node2 = NodeId::for_test_localhost(10_002);
        let node2_state = cluster_state.node_state_mut(&node2);
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
        let node1 = NodeId::for_test_localhost(10_001);
        let node2 = NodeId::for_test_localhost(10_002);
        digest.add_node(node1.clone(), 1);
        digest.add_node(node2.clone(), 2);
        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            HashSet::new(),
            &[
                (&node2, "key_c", "3", 3),
                (&node2, "key_d", "4", 4),
                (&node1, "key_b", "2", 2),
            ],
        );
    }

    #[test]
    fn test_cluster_state_compute_delta_depth_first_chitchat() {
        let cluster_state = test_cluster_state();
        let mut digest = Digest::default();
        let node1 = NodeId::for_test_localhost(10_001);
        let node2 = NodeId::for_test_localhost(10_002);
        digest.add_node(node1.clone(), 1);
        digest.add_node(node2.clone(), 2);
        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            HashSet::new(),
            &[
                (&node2, "key_c", "3", 3),
                (&node2, "key_d", "4", 4),
                (&node1, "key_b", "2", 2),
            ],
        );
    }

    #[test]
    fn test_cluster_state_compute_delta_missing_node() {
        let cluster_state = test_cluster_state();
        let mut digest = Digest::default();
        let node1 = NodeId::for_test_localhost(10_001);
        let node2 = NodeId::for_test_localhost(10_002);
        digest.add_node(node2.clone(), 3);
        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            HashSet::new(),
            &[
                (&node1, "key_a", "1", 1),
                (&node1, "key_b", "2", 2),
                (&node2, "key_d", "4", 4),
            ],
        );
    }

    #[test]
    fn test_cluster_state_compute_delta_should_ignore_dead_nodes() {
        let cluster_state = test_cluster_state();
        let digest = Digest::default();
        let node1 = NodeId::for_test_localhost(10_001);
        let node2 = NodeId::for_test_localhost(10_002);
        let dead_nodes = vec![node2];
        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            dead_nodes.iter().collect(),
            &[(&node1, "key_a", "1", 1), (&node1, "key_b", "2", 2)],
        );
    }
}
