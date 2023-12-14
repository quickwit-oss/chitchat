use std::collections::{BTreeMap, HashSet};
use std::mem;

use crate::serialize::*;
use crate::{ChitchatId, ChitchatIdGenerationEq, Heartbeat, MaxVersion, VersionedValue};

#[derive(Debug, Default, Eq, PartialEq)]
pub struct Delta {
    pub(crate) node_deltas: BTreeMap<ChitchatIdGenerationEq, NodeDelta>,
    pub(crate) nodes_to_reset: HashSet<ChitchatIdGenerationEq>,
}

impl Serializable for Delta {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.node_deltas.len() as u16).serialize(buf);
        for (chitchat_id, node_delta) in &self.node_deltas {
            chitchat_id.0.serialize(buf);
            node_delta.serialize(buf);
        }
        (self.nodes_to_reset.len() as u16).serialize(buf);
        for chitchat_id in &self.nodes_to_reset {
            chitchat_id.0.serialize(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let mut node_deltas: BTreeMap<ChitchatIdGenerationEq, NodeDelta> = Default::default();
        let num_nodes = u16::deserialize(buf)?;
        for _ in 0..num_nodes {
            let chitchat_id = ChitchatId::deserialize(buf)?;
            let node_delta = NodeDelta::deserialize(buf)?;
            node_deltas.insert(ChitchatIdGenerationEq(chitchat_id), node_delta);
        }
        let num_nodes_to_reset = u16::deserialize(buf)?;
        let mut nodes_to_reset = HashSet::with_capacity(num_nodes_to_reset as usize);
        for _ in 0..num_nodes_to_reset {
            let chitchat_id = ChitchatId::deserialize(buf)?;
            nodes_to_reset.insert(ChitchatIdGenerationEq(chitchat_id));
        }
        Ok(Delta {
            node_deltas,
            nodes_to_reset,
        })
    }

    fn serialized_len(&self) -> usize {
        let mut len = 2;
        for (chitchat_id, node_delta) in &self.node_deltas {
            len += chitchat_id.0.serialized_len();
            len += node_delta.serialized_len();
        }
        len += 2;
        for chitchat_id in &self.nodes_to_reset {
            len += chitchat_id.0.serialized_len();
        }
        len
    }
}

#[cfg(test)]
impl Delta {
    pub fn num_tuples(&self) -> usize {
        self.node_deltas
            .values()
            .map(|node_delta| node_delta.num_tuples())
            .sum()
    }

    pub fn add_node(&mut self, chitchat_id: ChitchatId, heartbeat: Heartbeat) {
        self.node_deltas
            .entry(ChitchatIdGenerationEq(chitchat_id))
            .or_insert_with(|| NodeDelta {
                heartbeat,
                ..Default::default()
            });
    }

    pub fn add_kv(
        &mut self,
        chitchat_id: &ChitchatId,
        key: &str,
        value: &str,
        version: crate::Version,
        tombstone: Option<u64>,
    ) {
        let node_delta = self
            .node_deltas
            .get_mut(&ChitchatIdGenerationEq(chitchat_id.clone()))
            .unwrap();

        node_delta.max_version = node_delta.max_version.max(version);
        node_delta.key_values.insert(
            key.to_string(),
            VersionedValue {
                value: value.to_string(),
                version,
                tombstone,
            },
        );
    }

    pub fn add_node_to_reset(&mut self, chitchat_id: ChitchatId) {
        self.nodes_to_reset
            .insert(ChitchatIdGenerationEq(chitchat_id));
    }
}

#[derive(Debug, Default, Eq, PartialEq, serde::Serialize)]
pub(crate) struct NodeDelta {
    pub heartbeat: Heartbeat,
    pub key_values: BTreeMap<String, VersionedValue>,
    // This attribute is computed upon deserialization. 0 if `key_values` is empty.
    pub max_version: MaxVersion,
}

#[cfg(test)]
impl NodeDelta {
    pub fn num_tuples(&self) -> usize {
        self.key_values.len()
    }
}

pub struct DeltaWriter {
    delta: Delta,
    mtu: usize,
    num_bytes: usize,
    current_chitchat_id: Option<ChitchatId>,
    current_node_delta: NodeDelta,
    reached_capacity: bool,
}

impl DeltaWriter {
    pub fn with_mtu(mtu: usize) -> Self {
        DeltaWriter {
            delta: Delta::default(),
            mtu,
            num_bytes: 2 + 2, /* 2 bytes for `nodes_to_reset.len()` + 2 bytes for
                               * `node_deltas.len()` */
            current_chitchat_id: None,
            current_node_delta: NodeDelta::default(),
            reached_capacity: false,
        }
    }
    fn flush(&mut self) {
        let chitchat_id_opt = mem::take(&mut self.current_chitchat_id);
        let node_delta = mem::take(&mut self.current_node_delta);
        if let Some(chitchat_id) = chitchat_id_opt {
            self.delta
                .node_deltas
                .insert(ChitchatIdGenerationEq(chitchat_id), node_delta);
        }
    }

    pub fn add_node_to_reset(&mut self, chitchat_id: ChitchatId) -> bool {
        let chitchat_id = ChitchatIdGenerationEq(chitchat_id);
        assert!(!self.delta.nodes_to_reset.contains(&chitchat_id));
        if !self.attempt_add_bytes(chitchat_id.0.serialized_len()) {
            return false;
        }
        self.delta.nodes_to_reset.insert(chitchat_id);
        true
    }

    pub fn add_node(&mut self, chitchat_id: ChitchatId, heartbeat: Heartbeat) -> bool {
        assert!(self
            .current_chitchat_id
            .as_ref()
            .map(|current_node| !current_node.eq_generation(&chitchat_id))
            .unwrap_or(true));
        let chitchat_id = ChitchatIdGenerationEq(chitchat_id);
        assert!(!self.delta.node_deltas.contains_key(&chitchat_id));
        self.flush();
        // Reserve bytes for [`ChitchatId`], [`Hearbeat`], and for an empty [`NodeDelta`] which has
        // a size of 2 bytes.
        if !self.attempt_add_bytes(chitchat_id.0.serialized_len() + heartbeat.serialized_len() + 2)
        {
            return false;
        }
        self.current_chitchat_id = Some(chitchat_id.0);
        self.current_node_delta.heartbeat = heartbeat;
        true
    }

    fn attempt_add_bytes(&mut self, num_bytes: usize) -> bool {
        assert!(!self.reached_capacity);
        let new_num_bytes = self.num_bytes + num_bytes;
        if new_num_bytes > self.mtu {
            self.reached_capacity = true;
            return false;
        }
        self.num_bytes = new_num_bytes;
        true
    }

    /// Returns false if the KV could not be added because mtu was reached.
    pub fn add_kv(&mut self, key: &str, versioned_value: VersionedValue) -> bool {
        assert!(!self.current_node_delta.key_values.contains_key(key));
        // Reserve bytes for the key (2 bytes are used to store the key length) and versioned value.
        if !self.attempt_add_bytes(
            2 + key.len()
                + versioned_value.value.serialized_len()
                + versioned_value.version.serialized_len()
                + versioned_value.tombstone.serialized_len(),
        ) {
            return false;
        }
        self.current_node_delta.max_version = self
            .current_node_delta
            .max_version
            .max(versioned_value.version);
        self.current_node_delta
            .key_values
            .insert(key.to_string(), versioned_value);
        true
    }
}

impl From<DeltaWriter> for Delta {
    fn from(mut delta_writer: DeltaWriter) -> Delta {
        delta_writer.flush();
        if cfg!(debug_assertions) {
            let mut buf = Vec::new();
            assert_eq!(delta_writer.num_bytes, delta_writer.delta.serialized_len());
            delta_writer.delta.serialize(&mut buf);
            assert_eq!(buf.len(), delta_writer.num_bytes);
        }
        delta_writer.delta
    }
}

impl Serializable for NodeDelta {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.heartbeat.serialize(buf);
        (self.key_values.len() as u16).serialize(buf);
        for (
            key,
            VersionedValue {
                value,
                version,
                tombstone,
            },
        ) in &self.key_values
        {
            key.serialize(buf);
            value.serialize(buf);
            version.serialize(buf);
            tombstone.serialize(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let heartbeat = Heartbeat::deserialize(buf)?;
        let mut key_values: BTreeMap<String, VersionedValue> = Default::default();
        let mut max_version = 0;
        let num_key_values = u16::deserialize(buf)?;
        for _ in 0..num_key_values {
            let key = String::deserialize(buf)?;
            let value = String::deserialize(buf)?;
            let version = u64::deserialize(buf)?;
            let tombstone = <Option<u64>>::deserialize(buf)?;
            max_version = max_version.max(version);
            key_values.insert(
                key,
                VersionedValue {
                    value,
                    version,
                    tombstone,
                },
            );
        }
        Ok(Self {
            heartbeat,
            key_values,
            max_version,
        })
    }

    fn serialized_len(&self) -> usize {
        let mut len = 2;
        len += self.heartbeat.serialized_len();

        for (
            key,
            VersionedValue {
                value,
                version,
                tombstone,
            },
        ) in &self.key_values
        {
            len += key.serialized_len();
            len += value.serialized_len();
            len += version.serialized_len();
            len += tombstone.serialized_len();
        }
        len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_serialization_default() {
        test_serdeser_aux(&Delta::default(), 4);
    }

    #[test]
    fn test_delta_serialization_simple_foo() {
        // 4 bytes
        let mut delta_writer = DeltaWriter::with_mtu(198);

        // ChitchatId takes 27 bytes = 15 bytes + 2 bytes for node length + "node-10001".len().
        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (node).
        assert!(delta_writer.add_node(node1, heartbeat));

        // +23 bytes: 2 bytes (key length) + 5 bytes (key) + 7 bytes (values) + 8 bytes (version) +
        // 1 bytes (empty tombstone).
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            },
        ));
        // +26 bytes: 2 bytes (key length) + 5 bytes (key) + 8 bytes (version) +
        // 9 bytes (empty tombstone).
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "".to_string(),
                version: 2,
                tombstone: Some(0),
            },
        ));

        let node2 = ChitchatId::for_local_test(10_002);
        let heartbeat = Heartbeat(0);
        // +37 bytes
        assert!(delta_writer.add_node(node2, heartbeat));

        // +23 bytes.
        assert!(delta_writer.add_kv(
            "key21",
            VersionedValue {
                value: "val21".to_string(),
                version: 2,
                tombstone: None,
            },
        ));
        // +23 bytes.
        assert!(delta_writer.add_kv(
            "key22",
            VersionedValue {
                value: "val22".to_string(),
                version: 3,
                tombstone: None,
            },
        ));
        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 173);
    }

    #[test]
    fn test_delta_serialization_simple_node() {
        // 4 bytes
        let mut delta_writer = DeltaWriter::with_mtu(124);

        // ChitchatId takes 27 bytes = 15 bytes + 2 bytes for node length + "node-10001".len().
        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (node).
        assert!(delta_writer.add_node(node1, heartbeat));

        // +23 bytes.
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));
        // +23 bytes.
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                tombstone: None,
            }
        ));

        let node2 = ChitchatId::for_local_test(10_002);
        let heartbeat = Heartbeat(0);
        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (node).
        assert!(delta_writer.add_node(node2, heartbeat));

        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 124);
    }

    #[test]
    fn test_delta_serialization_simple_with_nodes_to_reset() {
        // 4 bytes
        let mut delta_writer = DeltaWriter::with_mtu(151);
        // +27 bytes (ChitchatId).
        assert!(delta_writer.add_node_to_reset(ChitchatId::for_local_test(10_000)));

        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (ChitchatId).
        assert!(delta_writer.add_node(node1, heartbeat));

        // +23 bytes.
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));
        // +23 bytes.
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                tombstone: None,
            }
        ));

        let node2 = ChitchatId::for_local_test(10_002);
        let heartbeat = Heartbeat(0);
        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (ChitchatId).
        assert!(delta_writer.add_node(node2, heartbeat));

        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 151);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_node() {
        // 4 bytes.
        let mut delta_writer = DeltaWriter::with_mtu(87);

        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (ChitchatId).
        assert!(delta_writer.add_node(node1, heartbeat));

        // +23 bytes.
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));
        // +23 bytes.
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                tombstone: None,
            }
        ));

        let node2 = ChitchatId::for_local_test(10_002);
        let heartbeat = Heartbeat(0);
        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (ChitchatId).
        assert!(!delta_writer.add_node(node2, heartbeat));

        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 87);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_node_to_reset() {
        // 4 bytes.
        let mut delta_writer = DeltaWriter::with_mtu(90);

        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        // +37 bytes.
        assert!(delta_writer.add_node(node1, heartbeat));

        // +23 bytes.
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));
        // +23 bytes.
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                tombstone: None,
            }
        ));

        let node2 = ChitchatId::for_local_test(10_002);
        assert!(!delta_writer.add_node_to_reset(node2));

        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 87);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_kv() {
        // 4 bytes.
        let mut delta_writer = DeltaWriter::with_mtu(86);

        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        // +37 bytes.
        assert!(delta_writer.add_node(node1, heartbeat));
        // +23 bytes.
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));
        // +23 bytes.
        assert!(!delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                tombstone: None,
            }
        ));

        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 64);
    }

    #[test]
    #[should_panic]
    fn test_delta_serialization_panic_if_add_after_exceed() {
        let mut delta_writer = DeltaWriter::with_mtu(62);

        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        assert!(delta_writer.add_node(node1, heartbeat));

        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));
        assert!(!delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                tombstone: None,
            }
        ));
        delta_writer.add_kv(
            "key13",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                tombstone: None,
            },
        );
    }
}
