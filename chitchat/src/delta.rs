use std::collections::HashSet;

use tokio::time::Instant;

use crate::serialize::*;
use crate::{ChitchatId, Heartbeat, VersionedValue};

/// A delta is the message we send to another node to update it.
///
/// Its serialization is done by transforming it into a sequence of operations,
/// encoded one after the other in a compressed stream.
#[derive(Debug, Eq, PartialEq)]
pub struct Delta {
    pub(crate) nodes_to_reset: Vec<ChitchatId>,
    pub(crate) node_deltas: Vec<NodeDelta>,
    serialized_len: usize,
}

impl Default for Delta {
    fn default() -> Self {
        Delta {
            nodes_to_reset: Vec::new(),
            node_deltas: Vec::new(),
            serialized_len: 1,
        }
    }
}

impl Delta {
    fn get_operations(&self) -> impl Iterator<Item = DeltaOpRef<'_>> {
        let nodes_to_reset_ops = self.nodes_to_reset.iter().map(DeltaOpRef::NodeToReset);
        let node_deltas = self.node_deltas.iter().flat_map(|node_delta| {
            std::iter::once(DeltaOpRef::Node {
                chitchat_id: &node_delta.chitchat_id,
                heartbeat: node_delta.heartbeat,
            })
            .chain(node_delta.key_values.iter().map(|(key, versioned_value)| {
                DeltaOpRef::KeyValue {
                    key,
                    versioned_value,
                }
            }))
        });
        nodes_to_reset_ops.chain(node_deltas)
    }
}

enum DeltaOp {
    NodeToReset(ChitchatId),
    Node {
        chitchat_id: ChitchatId,
        heartbeat: Heartbeat,
    },
    KeyValue {
        key: String,
        versioned_value: VersionedValue,
    },
}

enum DeltaOpRef<'a> {
    NodeToReset(&'a ChitchatId),
    Node {
        chitchat_id: &'a ChitchatId,
        heartbeat: Heartbeat,
    },
    KeyValue {
        key: &'a str,
        versioned_value: &'a VersionedValue,
    },
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
            DeltaOpTag::NodeToReset => {
                let chitchat_id = ChitchatId::deserialize(buf)?;
                Ok(DeltaOp::NodeToReset(chitchat_id))
            }
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
                let deleted = bool::deserialize(buf)?;
                let tombstone = if deleted { Some(Instant::now()) } else { None };
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
        }
    }
}

impl DeltaOp {
    fn as_ref(&self) -> DeltaOpRef {
        match self {
            DeltaOp::Node {
                chitchat_id,
                heartbeat,
            } => DeltaOpRef::Node {
                chitchat_id,
                heartbeat: *heartbeat,
            },
            DeltaOp::KeyValue {
                key,
                versioned_value,
            } => DeltaOpRef::KeyValue {
                key,
                versioned_value,
            },
            DeltaOp::NodeToReset(node_to_reset) => DeltaOpRef::NodeToReset(node_to_reset),
        }
    }
}

impl Serializable for DeltaOp {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.as_ref().serialize(buf)
    }

    fn serialized_len(&self) -> usize {
        self.as_ref().serialized_len()
    }
}

impl<'a> Serializable for DeltaOpRef<'a> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Self::Node {
                chitchat_id,
                heartbeat,
            } => {
                buf.push(DeltaOpTag::Node.into());
                chitchat_id.serialize(buf);
                heartbeat.serialize(buf);
            }
            Self::KeyValue {
                key,
                versioned_value,
            } => {
                buf.push(DeltaOpTag::KeyValue.into());
                key.serialize(buf);
                versioned_value.value.serialize(buf);
                versioned_value.version.serialize(buf);
                versioned_value.tombstone.is_some().serialize(buf);
            }
            Self::NodeToReset(chitchat_id) => {
                buf.push(DeltaOpTag::NodeToReset.into());
                chitchat_id.serialize(buf);
            }
        }
    }

    fn serialized_len(&self) -> usize {
        1 + match self {
            Self::Node {
                chitchat_id,
                heartbeat,
            } => chitchat_id.serialized_len() + heartbeat.serialized_len(),
            Self::KeyValue {
                key,
                versioned_value,
            } => {
                key.serialized_len()
                    + versioned_value.value.serialized_len()
                    + versioned_value.version.serialized_len()
                    + 1
            }
            Self::NodeToReset(chitchat_id) => chitchat_id.serialized_len(),
        }
    }
}

impl Serializable for Delta {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut compressed_stream_writer = CompressedStreamWriter::with_block_threshold(16_384);
        for op in self.get_operations() {
            compressed_stream_writer.append(&op);
        }
        let payload = compressed_stream_writer.finish();
        assert_eq!(payload.len(), self.serialized_len);
        buf.extend(&payload);
    }

    fn serialized_len(&self) -> usize {
        self.serialized_len
    }
}

impl Deserializable for Delta {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let original_len = buf.len();
        let ops: Vec<DeltaOp> = crate::serialize::deserialize_stream(buf)?;
        let consumed_len = original_len - buf.len();
        let mut delta_builder = DeltaBuilder::default();
        for op in ops {
            delta_builder.apply_op(op)?;
        }
        Ok(delta_builder.finish(consumed_len))
    }
}

#[cfg(test)]
impl Delta {
    pub(crate) fn num_tuples(&self) -> usize {
        self.node_deltas
            .iter()
            .map(|node_delta| node_delta.num_tuples())
            .sum()
    }

    pub(crate) fn add_node(&mut self, chitchat_id: ChitchatId, heartbeat: Heartbeat) {
        assert!(!self
            .node_deltas
            .iter()
            .any(|node_delta| { node_delta.chitchat_id == chitchat_id }));
        self.node_deltas.push(NodeDelta {
            chitchat_id,
            heartbeat,
            key_values: Vec::new(),
        });
    }

    pub(crate) fn add_kv(
        &mut self,
        chitchat_id: &ChitchatId,
        key: &str,
        value: &str,
        version: crate::Version,
        deleted: bool,
    ) {
        let node_delta = self
            .node_deltas
            .iter_mut()
            .find(|node_delta| &node_delta.chitchat_id == chitchat_id)
            .unwrap();
        let tombstone = if deleted { Some(Instant::now()) } else { None };
        node_delta.key_values.push((
            key.to_string(),
            VersionedValue {
                value: value.to_string(),
                version,
                tombstone,
            },
        ));
    }

    pub(crate) fn set_serialized_len(&mut self, serialized_len: usize) {
        self.serialized_len = serialized_len;
    }

    pub(crate) fn get(&self, chitchat_id: &ChitchatId) -> Option<&NodeDelta> {
        self.node_deltas
            .iter()
            .find(|node_delta| &node_delta.chitchat_id == chitchat_id)
    }

    pub(crate) fn add_node_to_reset(&mut self, chitchat_id: ChitchatId) {
        self.nodes_to_reset.push(chitchat_id);
    }
}

#[derive(Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) struct NodeDelta {
    pub chitchat_id: ChitchatId,
    pub heartbeat: Heartbeat,
    pub key_values: Vec<(String, VersionedValue)>,
}

#[cfg(test)]
impl NodeDelta {
    pub fn num_tuples(&self) -> usize {
        self.key_values.len()
    }
}

#[derive(Default)]
struct DeltaBuilder {
    existing_nodes: HashSet<ChitchatId>,
    delta: Delta,
    current_node_delta: Option<NodeDelta>,
}

impl DeltaBuilder {
    fn finish(mut self, len: usize) -> Delta {
        self.flush();
        self.delta.serialized_len = len;
        self.delta
    }

    fn apply_op(&mut self, op: DeltaOp) -> anyhow::Result<()> {
        match op {
            DeltaOp::Node {
                chitchat_id,
                heartbeat,
            } => {
                self.flush();
                anyhow::ensure!(!self.existing_nodes.contains(&chitchat_id));
                self.existing_nodes.insert(chitchat_id.clone());
                self.current_node_delta = Some(NodeDelta {
                    chitchat_id,
                    heartbeat,
                    key_values: Vec::new(),
                });
            }
            DeltaOp::KeyValue {
                key,
                versioned_value,
            } => {
                let Some(current_node_delta) = self.current_node_delta.as_mut() else {
                    anyhow::bail!("received a key-value op without a node op before.");
                };
                if let Some((_last_key, last_versioned_value)) =
                    current_node_delta.key_values.last()
                {
                    anyhow::ensure!(
                        last_versioned_value.version < versioned_value.version,
                        "kv version should be increasing"
                    );
                }
                current_node_delta
                    .key_values
                    .push((key.to_string(), versioned_value));
            }
            DeltaOp::NodeToReset(chitchat_id) => {
                anyhow::ensure!(
                    self.delta.node_deltas.is_empty(),
                    "nodes_to_reset should be encoded before node_deltas"
                );
                self.delta.nodes_to_reset.push(chitchat_id);
            }
        }
        Ok(())
    }

    fn flush(&mut self) {
        let Some(node_delta) = self.current_node_delta.take() else {
            // There are no nodes in the builder.
            // (this happens when the delta builder is freshly created and no ops have been received
            // yet.)
            return;
        };
        self.delta.node_deltas.push(node_delta);
    }
}

/// The delta serializer is just helping us with the task of serializing
/// part of a delta, while respecting a given `mtu`.
///
/// We do it by calling `try_add_node_reset`, `try_add_node`, and `try_add_kv`
/// and stopping as soon as one of this methods returns `false`.
pub struct DeltaSerializer {
    mtu: usize,
    delta_builder: DeltaBuilder,
    compressed_stream_writer: CompressedStreamWriter,
}

const BLOCK_THRESHOLD: u16 = 16_384u16;

impl DeltaSerializer {
    pub fn with_mtu(mtu: usize) -> Self {
        assert!(mtu >= 100);
        let block_threshold = u16::try_from((BLOCK_THRESHOLD as usize).min(mtu)).unwrap();
        DeltaSerializer {
            mtu,
            delta_builder: DeltaBuilder::default(),
            compressed_stream_writer: CompressedStreamWriter::with_block_threshold(block_threshold),
        }
    }

    /// Returns false if the node to reset could not be added because the payload would exceed the
    /// mtu.
    pub fn try_add_node_to_reset(&mut self, chitchat_id: ChitchatId) -> bool {
        let delta_op = DeltaOp::NodeToReset(chitchat_id);
        self.try_add_op(delta_op)
    }

    fn try_add_op(&mut self, delta_op: DeltaOp) -> bool {
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

    /// Returns false if the KV could not be added because the payload would exceed the mtu.
    pub fn try_add_kv(&mut self, key: &str, versioned_value: VersionedValue) -> bool {
        let key_value_op = DeltaOp::KeyValue {
            key: key.to_string(),
            versioned_value,
        };
        self.try_add_op(key_value_op)
    }

    /// Returns false if the node could not be added because the payload would exceed the mtu.
    pub fn try_add_node(&mut self, chitchat_id: ChitchatId, heartbeat: Heartbeat) -> bool {
        let new_node_op = DeltaOp::Node {
            chitchat_id,
            heartbeat,
        };
        self.try_add_op(new_node_op)
    }

    pub fn finish(self) -> Delta {
        let buffer = self.compressed_stream_writer.finish();
        self.delta_builder.finish(buffer.len())
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
        let mut delta_writer = DeltaSerializer::with_mtu(198);

        // ChitchatId takes 27 bytes = 15 bytes + 2 bytes for node length + "node-10001".len().
        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (node).
        assert!(delta_writer.try_add_node(node1, heartbeat));

        // +23 bytes: 2 bytes (key length) + 5 bytes (key) + 7 bytes (values) + 8 bytes (version) +
        // 1 bytes (empty tombstone).
        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            },
        ));
        // +26 bytes: 2 bytes (key length) + 5 bytes (key) + 8 bytes (version) +
        // 9 bytes (empty tombstone).
        assert!(delta_writer.try_add_kv(
            "key12",
            VersionedValue {
                value: "".to_string(),
                version: 2,
                tombstone: Some(Instant::now()),
            },
        ));

        let node2 = ChitchatId::for_local_test(10_002);
        let heartbeat = Heartbeat(0);
        // +37 bytes
        assert!(delta_writer.try_add_node(node2, heartbeat));

        // +23 bytes.
        assert!(delta_writer.try_add_kv(
            "key21",
            VersionedValue {
                value: "val21".to_string(),
                version: 2,
                tombstone: None,
            },
        ));
        // +23 bytes.
        assert!(delta_writer.try_add_kv(
            "key22",
            VersionedValue {
                value: "val22".to_string(),
                version: 3,
                tombstone: None,
            },
        ));
        test_aux_delta_writer(delta_writer, 98);
    }

    #[test]
    fn test_delta_serialization_simple_node() {
        // 1 bytes (End tag)
        let mut delta_writer = DeltaSerializer::with_mtu(128);

        // ChitchatId takes 27 bytes = 15 bytes + 2 bytes for node length + "node-10001".len().
        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        // +37 bytes = 8 bytes (heartbeat) + 27 bytes (node) +  2bytes (block length)
        assert!(delta_writer.try_add_node(node1, heartbeat));

        // +24 bytes (kv + op tag)
        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));

        // +24 bytes. (kv + op tag)
        assert!(delta_writer.try_add_kv(
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
        assert!(delta_writer.try_add_node(node2, heartbeat));
        test_aux_delta_writer(delta_writer, 80);
    }

    #[track_caller]
    fn test_aux_delta_writer(delta_writer: DeltaSerializer, expected_len: usize) {
        let delta: Delta = delta_writer.finish();
        test_serdeser_aux(&delta, expected_len)
    }

    #[test]
    fn test_delta_serialization_simple_with_nodes_to_reset() {
        // 1 bytes (end tag)
        let mut delta_writer = DeltaSerializer::with_mtu(155);

        // +27 bytes (ChitchatId) + 1 (op tag) + 3 bytes (block len)
        // = 32 bytes
        assert!(delta_writer.try_add_node_to_reset(ChitchatId::for_local_test(10_000)));

        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);

        // +8 bytes (heartbeat) + 27 bytes (ChitchatId) + (1 op tag) + 3 bytes (pessimistic new
        // block) = 71
        assert!(delta_writer.try_add_node(node1, heartbeat));

        // +23 bytes (kv) + 1 (op tag)
        // = 95
        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));
        // +23 bytes (kv) + 1 (op tag)
        // = 119
        assert!(delta_writer.try_add_kv(
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
        assert!(delta_writer.try_add_node(node2, heartbeat));
        // The block got compressed.
        test_aux_delta_writer(delta_writer, 85);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_node() {
        // 4 bytes.
        let mut delta_writer = DeltaSerializer::with_mtu(100);

        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (ChitchatId).
        assert!(delta_writer.try_add_node(node1, heartbeat));

        // +23 bytes.
        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));
        // +23 bytes.
        assert!(delta_writer.try_add_kv(
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
        assert!(!delta_writer.try_add_node(node2, heartbeat));

        // The block got compressed.
        test_aux_delta_writer(delta_writer, 72);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_node_to_reset() {
        // 4 bytes.
        let mut delta_writer = DeltaSerializer::with_mtu(100);

        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        // +37 bytes.
        assert!(delta_writer.try_add_node(node1, heartbeat));

        // +23 bytes.
        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));
        // +23 bytes.
        assert!(delta_writer.try_add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                tombstone: None,
            }
        ));

        let node2 = ChitchatId::for_local_test(10_002);
        assert!(!delta_writer.try_add_node_to_reset(node2));

        // The block got compressed.
        test_aux_delta_writer(delta_writer, 72);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_kv() {
        // 1 bytes.
        let mut delta_writer = DeltaSerializer::with_mtu(100);

        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);

        // + 3 bytes (block tag) + 35 bytes (node) + 1 byte (op tag)
        // = 40
        assert!(delta_writer.try_add_node(node1, heartbeat));

        // +23 bytes (kv) + 1 (op tag) + 3 bytes (pessimistic block tag)
        // = 67
        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));

        // +33 bytes (kv) + 1 (op tag)
        // = 101 (exceeding mtu!)
        assert!(!delta_writer.try_add_kv(
            "key12",
            VersionedValue {
                value: "val12aaaaaaaaaabcc".to_string(),
                version: 2,
                tombstone: None,
            }
        ));
        test_aux_delta_writer(delta_writer, 64);
    }

    #[test]
    #[should_panic]
    fn test_delta_serialization_panic_if_add_after_exceed() {
        let mut delta_writer = DeltaSerializer::with_mtu(62);

        let node1 = ChitchatId::for_local_test(10_001);
        let heartbeat = Heartbeat(0);
        assert!(delta_writer.try_add_node(node1, heartbeat));

        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                tombstone: None,
            }
        ));
        assert!(!delta_writer.try_add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                tombstone: None,
            }
        ));
        delta_writer.try_add_kv(
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
                let tag_byte: u8 = tag.into();
                assert_eq!(b, tag_byte);
                num_valid_tags += 1;
            }
        }
        assert_eq!(num_valid_tags, 3);
    }
}
