use std::collections::{BTreeMap, HashSet};
use std::mem;

use anyhow::ensure;

use crate::serialize::*;
use crate::{ChitchatId, Heartbeat, MaxVersion, VersionedValue};

#[derive(Debug, Default)]
pub struct Delta {
    pub(crate) node_deltas: BTreeMap<ChitchatId, NodeDelta>,
    pub(crate) nodes_to_reset: HashSet<ChitchatId>,
    // Transient: if we have precomputed the serialized from the delta, we store it here.
    pre_serialized_delta: Option<Vec<u8>>,
}

impl PartialEq for Delta {
    fn eq(&self, other: &Self) -> bool {
        self.node_deltas.eq(&other.node_deltas) && self.nodes_to_reset.eq(&other.nodes_to_reset)
    }
}

impl Eq for Delta {}

enum DeltaOp {
    Node {
        chitchat_id: ChitchatId,
        heartbeat: Heartbeat,
    },
    KeyValue {
        key: String,
        versioned_value: VersionedValue,
    },
    NodesToReset(ChitchatId),
}

#[repr(u8)]
enum DeltaOpTag {
    Node = 0u8,
    KeyValue = 1u8,
    NodeToReset = 2u8,
}

impl TryFrom<u8> for DeltaOpTag {
    type Error = anyhow::Error;

    fn try_from(tag_byte: u8) -> anyhow::Result<DeltaOpTag> {
        match tag_byte {
            0u8 => Ok(DeltaOpTag::Node),
            1u8 => Ok(DeltaOpTag::KeyValue),
            2u8 => Ok(DeltaOpTag::NodeToReset),
            _ => {
                anyhow::bail!("Unknown tag: {tag_byte}")
            }
        }
    }
}

impl From<DeltaOpTag> for u8 {
    fn from(tag: DeltaOpTag) -> u8 {
        tag as u8
    }
}

impl Deserializable for DeltaOp {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let tag_bytes: [u8; 1] = Deserializable::deserialize(buf)?;
        let tag = DeltaOpTag::try_from(tag_bytes[0])?;
        match tag {
            DeltaOpTag::Node => {
                let chitchat_id = ChitchatId::deserialize(buf)?;
                let heartbeat = Heartbeat::deserialize(buf)?;
                Ok(DeltaOp::Node {
                    chitchat_id,
                    heartbeat,
                })
            }
            DeltaOpTag::KeyValue => {
                let key = String::deserialize(buf)?;
                let value = String::deserialize(buf)?;
                let version = u64::deserialize(buf)?;
                let tombstone = Option::<u64>::deserialize(buf)?;
                let versioned_value: VersionedValue = VersionedValue {
                    value,
                    version,
                    tombstone,
                };
                Ok(DeltaOp::KeyValue {
                    key,
                    versioned_value,
                })
            }
            DeltaOpTag::NodeToReset => {
                let chitchat_id = ChitchatId::deserialize(buf)?;
                Ok(DeltaOp::NodesToReset(chitchat_id))
            }
        }
    }
}

impl Serializable for DeltaOp {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            DeltaOp::Node {
                chitchat_id,
                heartbeat,
            } => {
                buf.push(DeltaOpTag::Node.into());
                chitchat_id.serialize(buf);
                heartbeat.serialize(buf);
            }
            DeltaOp::KeyValue {
                key,
                versioned_value,
            } => {
                buf.push(DeltaOpTag::KeyValue.into());
                key.serialize(buf);
                versioned_value.value.serialize(buf);
                versioned_value.version.serialize(buf);
                versioned_value.tombstone.serialize(buf);
            }
            DeltaOp::NodesToReset(chitchat_id) => {
                buf.push(DeltaOpTag::NodeToReset.into());
                chitchat_id.serialize(buf);
            }
        }
    }

    fn serialized_len(&self) -> usize {
        1 + match self {
            DeltaOp::Node {
                chitchat_id,
                heartbeat,
            } => chitchat_id.serialized_len() + heartbeat.serialized_len(),
            DeltaOp::KeyValue {
                key,
                versioned_value,
            } => {
                key.serialized_len()
                    + versioned_value.value.serialized_len()
                    + versioned_value.version.serialized_len()
                    + versioned_value.tombstone.serialized_len()
            }
            DeltaOp::NodesToReset(chitchat_id) => chitchat_id.serialized_len(),
        }
    }
}

impl Serializable for Delta {
    fn serialize(&self, buf: &mut Vec<u8>) {
        if let Some(serialized_ref) = self.pre_serialized_delta.as_ref() {
            buf.extend(serialized_ref);
        } else {
            // Slow, but never called in practise
            let ops: Vec<DeltaOp> = self.get_ops();
            ops.serialize(buf)
        }
    }

    fn serialized_len(&self) -> usize {
        if let Some(serialized_ref) = self.pre_serialized_delta.as_ref() {
            serialized_ref.len()
        } else {
            // Slow, but never called in practise
            let ops: Vec<DeltaOp> = self.get_ops();
            ops.serialized_len()
        }
    }
}

impl Deserializable for Delta {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let ops: Vec<DeltaOp> = crate::serialize::deserialize_stream(buf)?;
        ops.try_into()
    }
}

impl Delta {
    fn get_ops(&self) -> Vec<DeltaOp> {
        let mut ops = Vec::new();
        for (chitchat_id, node_delta) in &self.node_deltas {
            ops.push(DeltaOp::Node {
                chitchat_id: chitchat_id.clone(),
                heartbeat: node_delta.heartbeat,
            });
            for (key, versioned_value) in &node_delta.key_values {
                ops.push(DeltaOp::KeyValue {
                    key: key.clone(),
                    versioned_value: versioned_value.clone(),
                });
            }
        }
        for chitchat_id in &self.nodes_to_reset {
            ops.push(DeltaOp::NodesToReset(chitchat_id.clone()));
        }
        ops
    }
}

impl TryFrom<Vec<DeltaOp>> for Delta {
    type Error = anyhow::Error;

    fn try_from(delta_ops: Vec<DeltaOp>) -> anyhow::Result<Delta> {
        let mut delta_builder = DeltaBuilder::default();
        for op in delta_ops {
            delta_builder.apply_op(op)?;
        }
        Ok(delta_builder.finish())
    }
}

impl Serializable for Vec<DeltaOp> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        // This is slow, but it is only used in tests.
        let mut compressed_stream_writer = CompressedStreamWriter::with_block_threshold(16_384);
        for op in self {
            compressed_stream_writer.append(op);
        }
        let payload = compressed_stream_writer.finalize();
        buf.extend(&payload);
    }

    fn serialized_len(&self) -> usize {
        // This is slow, but it is only used in tests.
        let mut buf = Vec::new();
        self.serialize(&mut buf);
        buf.len()
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
            .entry(chitchat_id)
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
        let node_delta = self.node_deltas.get_mut(chitchat_id).unwrap();

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
        self.nodes_to_reset.insert(chitchat_id);
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

#[derive(Default)]
struct DeltaBuilder {
    delta: Delta,
    current_chitchat_id: Option<ChitchatId>,
    current_node_delta: NodeDelta,
}

impl DeltaBuilder {
    pub fn finish(mut self) -> Delta {
        self.flush();
        self.delta
    }

    fn apply_op(&mut self, op: DeltaOp) -> anyhow::Result<()> {
        match op {
            DeltaOp::Node {
                chitchat_id,
                heartbeat,
            } => {
                ensure!(self.current_chitchat_id.as_ref() != Some(&chitchat_id));
                ensure!(!self.delta.node_deltas.contains_key(&chitchat_id));
                self.flush();
                self.current_chitchat_id = Some(chitchat_id);
                self.current_node_delta.heartbeat = heartbeat;
            }
            DeltaOp::KeyValue {
                key,
                versioned_value,
            } => {
                ensure!(!self.current_node_delta.key_values.contains_key(&key));
                self.current_node_delta.max_version = self
                    .current_node_delta
                    .max_version
                    .max(versioned_value.version);
                self.current_node_delta
                    .key_values
                    .insert(key.to_string(), versioned_value);
            }
            DeltaOp::NodesToReset(chitchat_id) => {
                ensure!(!self.delta.nodes_to_reset.contains(&chitchat_id));
                self.delta.nodes_to_reset.insert(chitchat_id);
            }
        }
        Ok(())
    }

    fn flush(&mut self) {
        let Some(chitchat_id) = mem::take(&mut self.current_chitchat_id) else {
            // There are no nodes in the builder.
            // (this happens when the delta builder is freshly created and no ops have been received
            // yet.)
            return;
        };
        let node_delta = mem::take(&mut self.current_node_delta);
        self.delta.node_deltas.insert(chitchat_id, node_delta);
    }
}

pub struct DeltaWriter {
    mtu: usize,
    delta_builder: DeltaBuilder,
    compressed_stream_writer: CompressedStreamWriter,
}

const BLOCK_THRESHOLD: u16 = 16_384u16;

impl DeltaWriter {
    pub fn with_mtu(mtu: usize) -> Self {
        assert!(mtu >= 100);
        let block_threshold = u16::try_from((BLOCK_THRESHOLD as usize).min(mtu)).unwrap();
        DeltaWriter {
            mtu,
            delta_builder: DeltaBuilder::default(),
            compressed_stream_writer: CompressedStreamWriter::with_block_threshold(block_threshold),
        }
    }

    fn add_op(&mut self, delta_op: DeltaOp) -> bool {
        if self
            .compressed_stream_writer
            .serialized_len_upperbound_after(&delta_op)
            > self.mtu
        {
            return false;
        }
        self.compressed_stream_writer.append(&delta_op);
        assert!(self.delta_builder.apply_op(delta_op).is_ok());
        true
    }

    /// Returns false if the node could not be added because the payload would exceed the mtu.
    pub fn add_node(&mut self, chitchat_id: ChitchatId, heartbeat: Heartbeat) -> bool {
        let new_node_op = DeltaOp::Node {
            chitchat_id,
            heartbeat,
        };
        self.add_op(new_node_op)
    }

    /// Returns false if the KV could not be added because the payload would exceed the mtu.
    pub fn add_kv(&mut self, key: &str, versioned_value: VersionedValue) -> bool {
        let key_value_op = DeltaOp::KeyValue {
            key: key.to_string(),
            versioned_value,
        };
        self.add_op(key_value_op)
    }

    /// Returns false if the node to reset could not be added because the payload would exceed the
    /// mtu.
    pub fn add_node_to_reset(&mut self, chitchat_id: ChitchatId) -> bool {
        let delta_op = DeltaOp::NodesToReset(chitchat_id);
        self.add_op(delta_op)
    }
}

impl From<DeltaWriter> for Delta {
    fn from(delta_writer: DeltaWriter) -> Delta {
        let mut delta = delta_writer.delta_builder.finish();
        delta.pre_serialized_delta = Some(delta_writer.compressed_stream_writer.finalize());
        delta
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_serialization_default() {
        test_serdeser_aux(&Delta::default(), 1);
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
        test_serdeser_aux(&delta, 99);
    }

    #[test]
    fn test_delta_serialization_simple_node() {
        // 1 bytes (End tag)
        let mut delta_writer = DeltaWriter::with_mtu(128);

        // ChitchatId takes 27 bytes = 15 bytes + 2 bytes for node length + "node-10001".len().
        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        // +37 bytes = 8 bytes (heartbeat) + 27 bytes (node) +  2bytes (block length)
        assert!(delta_writer.add_node(node1, heartbeat));

        // +24 bytes (kv + op tag)
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));

        // +24 bytes. (kv + op tag)
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
        test_serdeser_aux(&delta, 80);
    }

    #[test]
    fn test_delta_serialization_simple_with_nodes_to_reset() {
        // 1 bytes (end tag)
        let mut delta_writer = DeltaWriter::with_mtu(155);

        // +27 bytes (ChitchatId) + 1 (op tag) + 3 bytes (block len)
        // = 32 bytes
        assert!(delta_writer.add_node_to_reset(ChitchatId::for_local_test(10_000)));

        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);

        // +8 bytes (heartbeat) + 27 bytes (ChitchatId) + (1 op tag) + 3 bytes (pessimistic new
        // block) = 71
        assert!(delta_writer.add_node(node1, heartbeat));

        // +23 bytes (kv) + 1 (op tag)
        // = 95
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));
        // +23 bytes (kv) + 1 (op tag)
        // = 119
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
        // +8 bytes (heartbeat) + 27 bytes (ChitchatId) + 1 byte (op tag)
        // = 155
        assert!(delta_writer.add_node(node2, heartbeat));

        let delta: Delta = delta_writer.into();
        // The block got compressed.
        test_serdeser_aux(&delta, 85);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_node() {
        // 4 bytes.
        let mut delta_writer = DeltaWriter::with_mtu(100);

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
        test_serdeser_aux(&delta, 72);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_node_to_reset() {
        // 4 bytes.
        let mut delta_writer = DeltaWriter::with_mtu(100);

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
        test_serdeser_aux(&delta, 72);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_kv() {
        // 1 bytes.
        let mut delta_writer = DeltaWriter::with_mtu(100);

        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);

        // + 3 bytes (block tag) + 35 bytes (node) + 1 byte (op tag)
        // = 40
        assert!(delta_writer.add_node(node1, heartbeat));

        // +23 bytes (kv) + 1 (op tag) + 3 bytes (pessimistic block tag)
        // = 67
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));

        // +33 bytes (kv) + 1 (op tag)
        // = 101 (exceeding mtu!)
        assert!(!delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12aaaaaaaaaa".to_string(),
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

    #[test]
    fn test_delta_op_tag() {
        let mut num_valid_tags = 0;
        for b in 0..=u8::MAX {
            if let Ok(tag) = DeltaOpTag::try_from(b) {
                let tag_byte = tag.into();
                assert_eq!(b, tag_byte);
                num_valid_tags += 1;
            }
        }
        assert_eq!(num_valid_tags, 3);
    }
}
