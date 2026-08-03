use std::collections::BTreeMap;

use crate::message::ProtocolVersion;
use crate::serialize::*;
use crate::{ChitchatId, Heartbeat, Version};

/// Block size used by the compressed digest stream.
const DIGEST_BLOCK_THRESHOLD: u16 = 16_384u16;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct NodeDigest {
    pub(crate) heartbeat: Heartbeat,
    pub(crate) last_gc_version: Version,
    pub(crate) max_version: Version,
}

impl Serializable for NodeDigest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.heartbeat.serialize(buf);
        self.last_gc_version.serialize(buf);
        self.max_version.serialize(buf);
    }

    fn serialized_len(&self) -> usize {
        self.heartbeat.serialized_len()
            + self.last_gc_version.serialized_len()
            + self.max_version.serialized_len()
    }
}

impl Deserializable for NodeDigest {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let heartbeat = Heartbeat::deserialize(buf)?;
        let last_gc_version = Version::deserialize(buf)?;
        let max_version = Version::deserialize(buf)?;
        Ok(NodeDigest {
            heartbeat,
            last_gc_version,
            max_version,
        })
    }
}

/// Decodes one `(ChitchatId, NodeDigest)` entry of the compressed digest stream.
impl Deserializable for (ChitchatId, NodeDigest) {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let chitchat_id = ChitchatId::deserialize(buf)?;
        let node_digest = NodeDigest::deserialize(buf)?;
        Ok((chitchat_id, node_digest))
    }
}

/// A digest represents is a piece of information summarizing
/// the staleness of one peer's data.
///
/// It is equivalent to a map
/// peer -> (heartbeat, max version).
#[derive(Debug, Default, Eq, PartialEq)]
pub struct Digest {
    pub(crate) node_digests: BTreeMap<ChitchatId, NodeDigest>,
}

#[cfg(test)]
impl Digest {
    pub fn add_node(
        &mut self,
        node: ChitchatId,
        heartbeat: Heartbeat,
        last_gc_version: Version,
        max_version: Version,
    ) {
        let node_digest = NodeDigest {
            heartbeat,
            last_gc_version,
            max_version,
        };
        self.node_digests.insert(node, node_digest);
    }

    pub fn sample_for_test(num_nodes: u16) -> Digest {
        let mut digest = Digest::default();
        for i in 0..num_nodes {
            let node = ChitchatId::for_local_test(10_000 + i);
            digest.add_node(
                node,
                Heartbeat(100 + i as u64),
                i as u64 % 7,
                1_000 + i as u64,
            );
        }
        digest
    }
}

impl Digest {
    /// Serializes the digest using the wire format selected by `protocol_version`.
    pub(crate) fn serialize(&self, protocol_version: ProtocolVersion, buf: &mut Vec<u8>) {
        match protocol_version {
            ProtocolVersion::V0 => self.serialize_v0(buf),
            ProtocolVersion::V1 => self.serialize_v1(buf),
        }
    }

    pub(crate) fn serialized_len(&self, protocol_version: ProtocolVersion) -> usize {
        match protocol_version {
            ProtocolVersion::V0 => {
                let mut len = (self.node_digests.len() as u16).serialized_len();
                for (chitchat_id, node_digest) in &self.node_digests {
                    len += chitchat_id.serialized_len();
                    len += node_digest.serialized_len();
                }
                len
            }
            ProtocolVersion::V1 => {
                let mut buf = Vec::new();
                self.serialize_v1(&mut buf);
                buf.len()
            }
        }
    }

    pub(crate) fn deserialize(
        protocol_version: ProtocolVersion,
        buf: &mut &[u8],
    ) -> anyhow::Result<Self> {
        match protocol_version {
            ProtocolVersion::V0 => Self::deserialize_v0(buf),
            ProtocolVersion::V1 => Self::deserialize_v1(buf),
        }
    }

    /// Serializes the digest using the V0 (uncompressed) wire format.
    fn serialize_v0(&self, buf: &mut Vec<u8>) {
        (self.node_digests.len() as u16).serialize(buf);
        for (chitchat_id, node_digest) in &self.node_digests {
            chitchat_id.serialize(buf);
            node_digest.serialize(buf);
        }
    }

    /// Serializes the digest using the V1 (compressed) wire format: a zstd block
    /// stream of `(ChitchatId, NodeDigest)` entries.
    fn serialize_v1(&self, buf: &mut Vec<u8>) {
        let mut stream_writer =
            CompressedStreamWriter::with_block_threshold(DIGEST_BLOCK_THRESHOLD);
        for (chitchat_id, node_digest) in &self.node_digests {
            stream_writer.append(chitchat_id);
            stream_writer.append(node_digest);
        }
        buf.extend_from_slice(&stream_writer.finish());
    }

    fn deserialize_v0(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let num_nodes = u16::deserialize(buf)?;
        let mut node_digests: BTreeMap<ChitchatId, NodeDigest> = Default::default();

        for _ in 0..num_nodes {
            let chitchat_id = ChitchatId::deserialize(buf)?;
            let node_digest = NodeDigest::deserialize(buf)?;
            node_digests.insert(chitchat_id, node_digest);
        }
        Ok(Digest { node_digests })
    }

    fn deserialize_v1(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let node_digests = deserialize_stream::<(ChitchatId, NodeDigest)>(buf)?
            .into_iter()
            .collect();
        Ok(Digest { node_digests })
    }
}

#[cfg(test)]
mod tests {
    use crate::digest::{Digest, NodeDigest};
    use crate::message::ProtocolVersion;
    use crate::serialize::test_serdeser_aux;
    use crate::{ChitchatId, Heartbeat};

    fn digest_serdeser_aux(digest: &Digest, protocol_version: ProtocolVersion) -> Digest {
        let mut buf = Vec::new();
        digest.serialize(protocol_version, &mut buf);
        assert_eq!(buf.len(), digest.serialized_len(protocol_version));
        Digest::deserialize(protocol_version, &mut &buf[..]).unwrap()
    }

    #[test]
    fn test_node_digest_serialization() {
        let node_digest = NodeDigest {
            heartbeat: crate::Heartbeat(100u64),
            last_gc_version: 2,
            max_version: 3,
        };
        test_serdeser_aux(&node_digest, 24);
    }

    #[test]
    fn test_digest_serialization() {
        let mut digest = Digest::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let node2 = ChitchatId::for_local_test(10_002);
        let node3 = ChitchatId::for_local_test(10_002);
        digest.add_node(node1, Heartbeat(101), 1, 11);
        digest.add_node(node2, Heartbeat(102), 20, 12);
        digest.add_node(node3, Heartbeat(103), 0, 13);

        let mut buf = Vec::new();
        digest.serialize(ProtocolVersion::V0, &mut buf);
        assert_eq!(buf.len(), 104);
        assert_eq!(digest_serdeser_aux(&digest, ProtocolVersion::V0), digest);
    }

    #[test]
    fn test_digest_compressed_roundtrip() {
        for num_nodes in [0u16, 1, 3, 500] {
            let digest = Digest::sample_for_test(num_nodes);
            assert_eq!(digest_serdeser_aux(&digest, ProtocolVersion::V1), digest);
        }
    }

    #[test]
    fn test_digest_v0_and_v1_decode_identically() {
        let digest = Digest::sample_for_test(200);
        assert_eq!(
            digest_serdeser_aux(&digest, ProtocolVersion::V0),
            digest_serdeser_aux(&digest, ProtocolVersion::V1),
        );
    }
}
