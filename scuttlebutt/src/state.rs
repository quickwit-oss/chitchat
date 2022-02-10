// Copyright (C) 2021 Quickwit, Inc.
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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BinaryHeap};
use std::time::Instant;

use rand::prelude::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::delta::{Delta, DeltaWriter};
use crate::digest::Digest;
use crate::{Version, VersionedValue};

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
        self.max_version = version;
        self.key_values
            .insert(key, VersionedValue { version, value });
    }
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct ClusterState {
    seed_nodes: Vec<String>,
    pub(crate) node_states: BTreeMap<String, NodeState>,
}

impl ClusterState {
    pub fn with_seed_ids(node_ids: Vec<String>) -> ClusterState {
        ClusterState {
            seed_nodes: node_ids.clone(),
            node_states: node_ids
                .into_iter()
                .map(|node_id| (node_id, NodeState::default()))
                .collect(),
        }
    }

    pub fn node_state_mut(&mut self, node_id: &str) -> &mut NodeState {
        // TODO use the `hash_raw_entry` feature once it gets stabilized.
        self.node_states.entry(node_id.to_string()).or_default()
    }

    pub fn node_state(&self, node_id: &str) -> Option<&NodeState> {
        self.node_states.get(node_id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &str> {
        self.node_states.keys().map(|k| k.as_str())
    }

    pub fn seed_nodes(&self) -> impl Iterator<Item = &str> {
        self.seed_nodes.iter().map(|node_id| node_id.as_str())
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
                    return delta_writer.into();
                }
            }
        }
        delta_writer.into()
    }
}

#[derive(Default)]
struct NodeSortedByStaleLength<'a> {
    node_per_stale_length: BTreeMap<usize, Vec<&'a str>>,
    stale_lengths: BinaryHeap<usize>,
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
        let max_delta = cluster_state.compute_delta(digest, usize::MAX);
        let mut buf = Vec::new();
        max_delta.serialize(&mut buf);
        let mut mtu_per_num_entries = Vec::new();
        for mtu in 2..buf.len() {
            let delta = cluster_state.compute_delta(digest, mtu);
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
                expected_delta.add_node_delta(node, key, val, version);
            }
            {
                let delta = cluster_state.compute_delta(digest, mtu);
                assert_eq!(&delta, &expected_delta);
            }
            {
                let delta = cluster_state.compute_delta(digest, mtu + 1);
                assert_eq!(&delta, &expected_delta);
            }
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
}
