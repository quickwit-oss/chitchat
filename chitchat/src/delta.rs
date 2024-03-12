use std::collections::HashSet;

use crate::serialize::*;
use crate::types::{DeletionStatusMutation, KeyValueMutation, KeyValueMutationRef};
use crate::{ChitchatId, Version, VersionedValue};

/// A delta is the message we send to another node to update it.
///
/// Its serialization is done by transforming it into a sequence of operations,
/// encoded one after the other in a compressed stream.
#[derive(Debug, Eq, PartialEq)]
pub struct Delta {
    pub(crate) node_deltas: Vec<NodeDelta>,
    serialized_len: usize,
}

impl Default for Delta {
    fn default() -> Self {
        Delta {
            node_deltas: Vec::new(),
            serialized_len: 1,
        }
    }
}

impl Delta {
    fn get_operations(&self) -> impl Iterator<Item = DeltaOpRef<'_>> {
        self.node_deltas.iter().flat_map(|node_delta| {
            std::iter::once(DeltaOpRef::Node {
                chitchat_id: &node_delta.chitchat_id,
                last_gc_version: node_delta.last_gc_version,
                from_version_excluded: node_delta.from_version_excluded,
            })
            .chain(
                node_delta
                    .key_values
                    .iter()
                    .map(|key_value_mutation| DeltaOpRef::KeyValue(key_value_mutation.into())),
            )
            .chain({
                node_delta
                    .max_version
                    .map(|max_version| DeltaOpRef::SetMaxVersion { max_version })
            })
        })
    }
}

enum DeltaOp {
    Node {
        chitchat_id: ChitchatId,
        last_gc_version: Version,
        from_version_excluded: u64,
    },
    KeyValue(KeyValueMutation),
    SetMaxVersion {
        max_version: Version,
    },
}

enum DeltaOpRef<'a> {
    Node {
        chitchat_id: &'a ChitchatId,
        last_gc_version: Version,
        from_version_excluded: u64,
    },
    KeyValue(KeyValueMutationRef<'a>),
    SetMaxVersion {
        max_version: Version,
    },
}

#[repr(u8)]
enum DeltaOpTag {
    Node = 0u8,
    KeyValue = 1u8,
    SetMaxVersion = 2u8,
}

impl TryFrom<u8> for DeltaOpTag {
    type Error = anyhow::Error;

    fn try_from(tag_byte: u8) -> anyhow::Result<DeltaOpTag> {
        match tag_byte {
            0u8 => Ok(DeltaOpTag::Node),
            1u8 => Ok(DeltaOpTag::KeyValue),
            2u8 => Ok(DeltaOpTag::SetMaxVersion),
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
                let last_gc_version = Version::deserialize(buf)?;
                let from_version_excluded = u64::deserialize(buf)?;
                Ok(DeltaOp::Node {
                    chitchat_id,
                    last_gc_version,
                    from_version_excluded,
                })
            }
            DeltaOpTag::KeyValue => {
                let key = String::deserialize(buf)?;
                let value = String::deserialize(buf)?;
                let version = u64::deserialize(buf)?;
                let deleted = DeletionStatusMutation::deserialize(buf)?;
                Ok(DeltaOp::KeyValue(KeyValueMutation {
                    key,
                    value,
                    version,
                    status: deleted,
                }))
            }
            DeltaOpTag::SetMaxVersion => {
                let max_version = Version::deserialize(buf)?;
                Ok(DeltaOp::SetMaxVersion { max_version })
            }
        }
    }
}

impl DeltaOp {
    fn as_ref(&self) -> DeltaOpRef {
        match self {
            DeltaOp::Node {
                chitchat_id,
                last_gc_version,
                from_version_excluded,
            } => DeltaOpRef::Node {
                chitchat_id,
                last_gc_version: *last_gc_version,
                from_version_excluded: *from_version_excluded,
            },
            DeltaOp::KeyValue(key_value_mutation) => {
                DeltaOpRef::KeyValue(key_value_mutation.into())
            }
            DeltaOp::SetMaxVersion { max_version } => DeltaOpRef::SetMaxVersion {
                max_version: *max_version,
            },
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
                last_gc_version,
                from_version_excluded: from_version,
            } => {
                buf.push(DeltaOpTag::Node.into());
                chitchat_id.serialize(buf);
                last_gc_version.serialize(buf);
                from_version.serialize(buf);
            }
            Self::KeyValue(key_value_mutation_ref) => {
                buf.push(DeltaOpTag::KeyValue.into());
                key_value_mutation_ref.serialize(buf);
            }
            Self::SetMaxVersion { max_version } => {
                buf.push(DeltaOpTag::SetMaxVersion.into());
                max_version.serialize(buf);
            }
        }
    }

    fn serialized_len(&self) -> usize {
        1 + match self {
            Self::Node {
                chitchat_id,
                last_gc_version,
                from_version_excluded: from_version,
            } => {
                chitchat_id.serialized_len()
                    + last_gc_version.serialized_len()
                    + from_version.serialized_len()
            }
            Self::KeyValue(key_value_mutation_ref) => key_value_mutation_ref.serialized_len(),
            Self::SetMaxVersion { max_version } => max_version.serialized_len(),
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

    pub(crate) fn add_node(
        &mut self,
        chitchat_id: ChitchatId,
        last_gc_version: Version,
        from_version: Version,
    ) {
        assert!(!self
            .node_deltas
            .iter()
            .any(|node_delta| { node_delta.chitchat_id == chitchat_id }));
        self.node_deltas.push(NodeDelta {
            chitchat_id,
            last_gc_version,
            from_version_excluded: from_version,
            key_values: Vec::new(),
            max_version: None,
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
        node_delta.key_values.push(KeyValueMutation {
            key: key.to_string(),
            value: value.to_string(),
            version,
            status: if deleted {
                DeletionStatusMutation::Delete
            } else {
                DeletionStatusMutation::Set
            },
        });
    }

    pub(crate) fn set_serialized_len(&mut self, serialized_len: usize) {
        self.serialized_len = serialized_len;
    }

    pub(crate) fn get(&self, chitchat_id: &ChitchatId) -> Option<&NodeDelta> {
        self.node_deltas
            .iter()
            .find(|node_delta| &node_delta.chitchat_id == chitchat_id)
    }
}

#[derive(Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) struct NodeDelta {
    pub chitchat_id: ChitchatId,
    // `from_version_excluded` and `last_gc_version` are here to express on which states
    // this delta can be applied to.
    //
    // `last_gc_version` expresses that this delta was computed from a state where no
    // keys > `last_gc_version` have been removed.
    //
    // `from_version` expresses that from this state, ALL of the records in
    // (`from_version`.. `max_version`] are present in this delta
    // (where max_version is maximum version in the delta.)
    //
    // In other words, the only gaps in this interval are deleted key-values with a version
    // <= `last gc version`.
    //
    // Inspect the code in `prepare_apply_delta(..)` to see the rules on how `from_version`
    // and `last_gc_version` are used.
    pub from_version_excluded: Version,
    pub last_gc_version: Version,
    pub key_values: Vec<KeyValueMutation>,
    pub max_version: Option<Version>,
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
                last_gc_version,
                from_version_excluded,
            } => {
                self.flush();
                anyhow::ensure!(!self.existing_nodes.contains(&chitchat_id));
                self.existing_nodes.insert(chitchat_id.clone());
                self.current_node_delta = Some(NodeDelta {
                    chitchat_id,
                    last_gc_version,
                    from_version_excluded,
                    key_values: Vec::new(),
                    max_version: None,
                });
            }
            DeltaOp::KeyValue(key_value_mutation) => {
                let Some(current_node_delta) = self.current_node_delta.as_mut() else {
                    anyhow::bail!("received a key-value op without a node op before.");
                };
                if let Some(previous_key_value_mutation) = current_node_delta.key_values.last() {
                    anyhow::ensure!(
                        previous_key_value_mutation.version < key_value_mutation.version,
                        "kv version should be increasing"
                    );
                }
                current_node_delta.key_values.push(key_value_mutation);
            }
            DeltaOp::SetMaxVersion { max_version } => {
                let Some(current_node_delta) = self.current_node_delta.as_mut() else {
                    anyhow::bail!("received a key-value op without a node op before.");
                };
                current_node_delta.max_version = Some(max_version);
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

    #[must_use]
    pub fn try_set_max_version(&mut self, max_version: Version) -> bool {
        let key_value_op = DeltaOp::SetMaxVersion { max_version };
        self.try_add_op(key_value_op)
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
        let key_value_mutation = KeyValueMutation {
            key: key.to_string(),
            value: versioned_value.value,
            version: versioned_value.version,
            status: versioned_value.status.into(),
        };
        let key_value_op = DeltaOp::KeyValue(key_value_mutation);
        self.try_add_op(key_value_op)
    }

    /// Returns false if the node could not be added because the payload would exceed the mtu.
    pub fn try_add_node(
        &mut self,
        chitchat_id: ChitchatId,
        last_gc_version: Version,
        from_version: Version,
    ) -> bool {
        let new_node_op = DeltaOp::Node {
            chitchat_id,
            last_gc_version,
            from_version_excluded: from_version,
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
    use tokio::time::Instant;

    use super::*;
    use crate::types::DeletionStatus;

    #[test]
    fn test_delta_serialization_default() {
        test_serdeser_aux(&Delta::default(), 1);
    }

    #[test]
    fn test_delta_serialization_with_set_max_version() {
        // 4 bytes
        let mut delta_writer = DeltaSerializer::with_mtu(198);

        // ChitchatId takes 27 bytes = 15 bytes + 2 bytes for node length + "node-10001".len().
        let node1 = ChitchatId::for_local_test(10_001);

        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (node).
        assert!(delta_writer.try_add_node(node1, 80u64, 50u64));

        // +9 bytes: +1 bytes + 8 bytes (version)
        assert!(delta_writer.try_set_max_version(100));

        test_aux_delta_writer(delta_writer, 1 + 56);
    }

    #[test]
    fn test_delta_serialization_simple_foo() {
        // 4 bytes
        let mut delta_writer = DeltaSerializer::with_mtu(198);

        // ChitchatId takes 27 bytes = 15 bytes + 2 bytes for node length + "node-10001".len().
        let node1 = ChitchatId::for_local_test(10_001);
        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (node).
        assert!(delta_writer.try_add_node(node1, 0u64, 0u64));

        // +23 bytes: 2 bytes (key length) + 5 bytes (key) + 7 bytes (values) + 8 bytes (version) +
        // 1 bytes (empty tombstone).
        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                status: DeletionStatus::Set,
            },
        ));
        // +26 bytes: 2 bytes (key length) + 5 bytes (key) + 8 bytes (version) +
        // 9 bytes (empty tombstone).
        assert!(delta_writer.try_add_kv(
            "key12",
            VersionedValue {
                value: "".to_string(),
                version: 2,
                status: DeletionStatus::Deleted(Instant::now()),
            },
        ));

        let node2 = ChitchatId::for_local_test(10_002);
        // +37 bytes
        assert!(delta_writer.try_add_node(node2, 0, 0u64));

        // +23 bytes.
        assert!(delta_writer.try_add_kv(
            "key21",
            VersionedValue {
                value: "val21".to_string(),
                version: 2,
                status: DeletionStatus::Set,
            },
        ));
        // +23 bytes.
        assert!(delta_writer.try_add_kv(
            "key22",
            VersionedValue {
                value: "val22".to_string(),
                version: 3,
                status: DeletionStatus::Set,
            },
        ));
        test_aux_delta_writer(delta_writer, 98);
    }

    #[test]
    fn test_delta_serialization_simple_node() {
        // 1 bytes (End tag)
        let mut delta_writer = DeltaSerializer::with_mtu(140);

        // ChitchatId takes 27 bytes = 15 bytes + 2 bytes for node length + "node-10001".len().
        let node1 = ChitchatId::for_local_test(10_001);
        // +37 bytes = 8 bytes (last gc version) + 27 bytes (node) +  2bytes (block length)
        assert!(delta_writer.try_add_node(node1, 0, 0u64));

        // +24 bytes (kv + op tag)
        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                status: DeletionStatus::Set,
            }
        ));

        // +24 bytes. (kv + op tag)
        assert!(delta_writer.try_add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                status: DeletionStatus::Set,
            }
        ));

        let node2 = ChitchatId::for_local_test(10_002);
        // +37 bytes = 8 bytes (last gc version) + 2 bytes (empty node delta) + 27 bytes (node).
        assert!(delta_writer.try_add_node(node2, 0, 0u64));
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

        let node1 = ChitchatId::for_local_test(10_001);

        // +8 bytes (last gc version) + 27 bytes (ChitchatId) + (1 op tag) + 3 bytes (pessimistic
        // new block) = 71
        assert!(delta_writer.try_add_node(node1, 0u64, 0u64));

        // +23 bytes (kv) + 1 (op tag)
        // = 95
        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                status: DeletionStatus::Set,
            }
        ));
        // +23 bytes (kv) + 1 (op tag)
        // = 119
        assert!(delta_writer.try_add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                status: DeletionStatus::Set,
            }
        ));

        let node2 = ChitchatId::for_local_test(10_002);
        // +8 bytes (last gc version) + 27 bytes (ChitchatId) + 1 byte (op tag)
        // = 155
        assert!(delta_writer.try_add_node(node2, 0u64, 0));
        // The block got compressed.
        test_aux_delta_writer(delta_writer, 80);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_node() {
        // 4 bytes.
        let mut delta_writer = DeltaSerializer::with_mtu(100);

        let node1 = ChitchatId::for_local_test(10_001);
        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (ChitchatId).
        assert!(delta_writer.try_add_node(node1, 0, 0));

        // +23 bytes.
        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                status: DeletionStatus::Set,
            }
        ));
        // +23 bytes.
        assert!(delta_writer.try_add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                status: DeletionStatus::Set,
            }
        ));

        let node2 = ChitchatId::for_local_test(10_002);
        // +37 bytes = 8 bytes (heartbeat) + 2 bytes (empty node delta) + 27 bytes (ChitchatId).
        assert!(!delta_writer.try_add_node(node2, 0u64, 1u64));

        // The block got compressed.
        test_aux_delta_writer(delta_writer, 72);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_kv() {
        // 1 bytes.
        let mut delta_writer = DeltaSerializer::with_mtu(100);

        let node1 = ChitchatId::for_local_test(10_001);

        // + 3 bytes (block tag) + 35 bytes (node) + 1 byte (op tag)
        // = 40
        assert!(delta_writer.try_add_node(node1, 0u64, 1u64));

        // +23 bytes (kv) + 1 (op tag) + 3 bytes (pessimistic block tag)
        // = 67
        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                status: DeletionStatus::Set,
            }
        ));

        // +33 bytes (kv) + 1 (op tag)
        // = 101 (exceeding mtu!)
        assert!(!delta_writer.try_add_kv(
            "key12",
            VersionedValue {
                value: "val12aaaaaaaaaabcc".to_string(),
                version: 2,
                status: DeletionStatus::Set,
            }
        ));
        test_aux_delta_writer(delta_writer, 72);
    }

    #[test]
    #[should_panic]
    fn test_delta_serialization_panic_if_add_after_exceed() {
        let mut delta_writer = DeltaSerializer::with_mtu(62);

        let node1 = ChitchatId::for_local_test(10_001);
        assert!(delta_writer.try_add_node(node1, 0u64, 1u64));

        assert!(delta_writer.try_add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
                status: DeletionStatus::Set,
            }
        ));
        assert!(!delta_writer.try_add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                status: DeletionStatus::Set,
            }
        ));
        delta_writer.try_add_kv(
            "key13",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
                status: DeletionStatus::Set,
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
