use std::collections::{BTreeMap, HashSet};
use std::mem;

use crate::serialize::*;
use crate::{ChitchatId, Version, VersionedValue};

#[derive(Debug, Default, Eq, PartialEq)]
pub struct Delta {
    pub(crate) node_deltas: BTreeMap<ChitchatId, NodeDelta>,
    pub(crate) nodes_to_reset: HashSet<ChitchatId>,
}

impl Serializable for Delta {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.node_deltas.len() as u16).serialize(buf);
        for (chitchat_id, node_delta) in &self.node_deltas {
            chitchat_id.serialize(buf);
            node_delta.serialize(buf);
        }
        (self.nodes_to_reset.len() as u16).serialize(buf);
        for chitchat_id in &self.nodes_to_reset {
            chitchat_id.serialize(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let mut node_deltas: BTreeMap<ChitchatId, NodeDelta> = Default::default();
        let num_nodes = u16::deserialize(buf)?;
        for _ in 0..num_nodes {
            let chitchat_id = ChitchatId::deserialize(buf)?;
            let node_delta = NodeDelta::deserialize(buf)?;
            node_deltas.insert(chitchat_id, node_delta);
        }
        let num_nodes_to_reset = u16::deserialize(buf)?;
        let mut nodes_to_reset = HashSet::with_capacity(num_nodes_to_reset as usize);
        for _ in 0..num_nodes_to_reset {
            let chitchat_id = ChitchatId::deserialize(buf)?;
            nodes_to_reset.insert(chitchat_id);
        }
        Ok(Delta {
            node_deltas,
            nodes_to_reset,
        })
    }

    fn serialized_len(&self) -> usize {
        let mut len = 2;
        for (chitchat_id, node_delta) in &self.node_deltas {
            len += chitchat_id.serialized_len();
            len += node_delta.serialized_len();
        }
        len += 2;
        for chitchat_id in &self.nodes_to_reset {
            len += chitchat_id.serialized_len();
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

    pub fn add_node_to_reset(&mut self, chitchat_id: ChitchatId) {
        self.nodes_to_reset.insert(chitchat_id);
    }

    pub fn add_node_delta(
        &mut self,
        chitchat_id: ChitchatId,
        key: &str,
        value: &str,
        version: Version,
        marked_for_deletion: bool,
    ) {
        self.node_deltas
            .entry(chitchat_id)
            .or_default()
            .key_values
            .insert(
                key.to_string(),
                VersionedValue {
                    value: value.to_string(),
                    version,
                    marked_for_deletion,
                },
            );
    }
}

#[derive(serde::Serialize, Default, Eq, PartialEq, Debug)]
pub(crate) struct NodeDelta {
    pub key_values: BTreeMap<String, VersionedValue>,
}

impl NodeDelta {
    pub fn max_version(&self) -> Version {
        self.key_values
            .values()
            .map(|value| value.version)
            .max()
            .unwrap_or(0)
    }
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
            self.delta.node_deltas.insert(chitchat_id, node_delta);
        }
    }

    pub fn add_node_to_reset(&mut self, chitchat_id: ChitchatId) -> bool {
        assert!(!self.delta.nodes_to_reset.contains(&chitchat_id));
        if !self.attempt_add_bytes(chitchat_id.serialized_len()) {
            return false;
        }
        self.delta.nodes_to_reset.insert(chitchat_id);
        true
    }

    pub fn add_node(&mut self, chitchat_id: ChitchatId) -> bool {
        assert!(self.current_chitchat_id.as_ref() != Some(&chitchat_id));
        assert!(!self.delta.node_deltas.contains_key(&chitchat_id));
        self.flush();
        // Reserve bytes for [`ChitchatId`] and for an empty [`NodeDelta`] which has a size of 2
        // bytes.
        if !self.attempt_add_bytes(chitchat_id.serialized_len() + 2) {
            return false;
        }
        self.current_chitchat_id = Some(chitchat_id);
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
                + versioned_value.marked_for_deletion.serialized_len(),
        ) {
            return false;
        }
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
        (self.key_values.len() as u16).serialize(buf);
        for (
            key,
            VersionedValue {
                value,
                version,
                marked_for_deletion,
            },
        ) in &self.key_values
        {
            key.serialize(buf);
            value.serialize(buf);
            version.serialize(buf);
            marked_for_deletion.serialize(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let mut key_values: BTreeMap<String, VersionedValue> = Default::default();
        let num_kvs = u16::deserialize(buf)?;
        for _ in 0..num_kvs {
            let key = String::deserialize(buf)?;
            let value = String::deserialize(buf)?;
            let version = u64::deserialize(buf)?;
            let marked_for_deletion = bool::deserialize(buf)?;
            key_values.insert(
                key,
                VersionedValue {
                    value,
                    version,
                    marked_for_deletion,
                },
            );
        }
        Ok(NodeDelta { key_values })
    }

    fn serialized_len(&self) -> usize {
        let mut len = 2;
        for (
            key,
            VersionedValue {
                value,
                version,
                marked_for_deletion,
            },
        ) in &self.key_values
        {
            len += key.serialized_len();
            len += value.serialized_len();
            len += version.serialized_len();
            len += marked_for_deletion.serialized_len();
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
    fn test_delta_serialization_simple() {
        let mut delta_writer = DeltaWriter::with_mtu(154);
        delta_writer.add_node(ChitchatId::for_local_test(10_001));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                marked_for_deletion: false,
            },
        ));
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                marked_for_deletion: false,
            },
        ));
        assert!(delta_writer.add_node(ChitchatId::for_local_test(10_002)));
        assert!(delta_writer.add_kv(
            "key21",
            VersionedValue {
                value: "val21".to_string(),
                version: 2,
                marked_for_deletion: false,
            },
        ));
        assert!(delta_writer.add_kv(
            "key22",
            VersionedValue {
                value: "val22".to_string(),
                version: 3,
                marked_for_deletion: false,
            },
        ));
        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 154);
    }

    #[test]
    fn test_delta_serialization_simple_node() {
        let mut delta_writer = DeltaWriter::with_mtu(118);
        assert!(delta_writer.add_node(ChitchatId::for_local_test(10_001)));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                marked_for_deletion: false,
            }
        ));
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                marked_for_deletion: false,
            }
        ));
        assert!(delta_writer.add_node(ChitchatId::for_local_test(10_002)));
        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 108);
    }

    #[test]
    fn test_delta_serialization_simple_with_nodes_to_reset() {
        let mut delta_writer = DeltaWriter::with_mtu(136);
        assert!(delta_writer.add_node_to_reset(ChitchatId::for_local_test(10_000))); // Chitchat ID takes 27 bytes
        assert!(delta_writer.add_node(ChitchatId::for_local_test(10_001)));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                marked_for_deletion: false,
            }
        ));
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                marked_for_deletion: false,
            }
        ));
        assert!(delta_writer.add_node(ChitchatId::for_local_test(10_002)));
        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 135);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_node() {
        let mut delta_writer = DeltaWriter::with_mtu(87);
        assert!(delta_writer.add_node(ChitchatId::for_local_test(10_001)));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                marked_for_deletion: false,
            }
        ));
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                marked_for_deletion: false,
            }
        ));
        assert!(!delta_writer.add_node(ChitchatId::for_local_test(10_002)));
        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 79);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_node_to_reset() {
        let mut delta_writer = DeltaWriter::with_mtu(87);
        assert!(delta_writer.add_node(ChitchatId::for_local_test(10_001)));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                marked_for_deletion: false,
            }
        ));
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                marked_for_deletion: false,
            }
        ));
        assert!(!delta_writer.add_node_to_reset(ChitchatId::for_local_test(10_002)));
        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 79);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_kv() {
        let mut delta_writer = DeltaWriter::with_mtu(66);
        assert!(delta_writer.add_node(ChitchatId::for_local_test(10_001)));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                marked_for_deletion: false,
            }
        ));
        assert!(!delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                marked_for_deletion: false,
            }
        ));
        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 56);
    }

    #[test]
    #[should_panic]
    fn test_delta_serialization_panic_if_add_after_exceed() {
        let mut delta_writer = DeltaWriter::with_mtu(54);
        assert!(delta_writer.add_node(ChitchatId::for_local_test(10_001)));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                marked_for_deletion: false,
            }
        ));
        assert!(!delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                marked_for_deletion: false,
            }
        ));
        delta_writer.add_kv(
            "key13",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                marked_for_deletion: false,
            },
        );
    }
}
