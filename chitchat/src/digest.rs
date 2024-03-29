use std::collections::BTreeMap;

use crate::serialize::*;
use crate::{ChitchatId, Heartbeat, Version};

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
}

impl Serializable for Digest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.node_digests.len() as u16).serialize(buf);
        for (chitchat_id, node_digest) in &self.node_digests {
            chitchat_id.serialize(buf);
            node_digest.serialize(buf);
        }
    }
    fn serialized_len(&self) -> usize {
        let mut len = (self.node_digests.len() as u16).serialized_len();
        for (chitchat_id, node_digest) in &self.node_digests {
            len += chitchat_id.serialized_len();
            len += node_digest.serialized_len();
        }
        len
    }
}

impl Deserializable for Digest {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let num_nodes = u16::deserialize(buf)?;
        let mut node_digests: BTreeMap<ChitchatId, NodeDigest> = Default::default();

        for _ in 0..num_nodes {
            let chitchat_id = ChitchatId::deserialize(buf)?;
            let node_digest = NodeDigest::deserialize(buf)?;
            node_digests.insert(chitchat_id, node_digest);
        }
        Ok(Digest { node_digests })
    }
}

#[cfg(test)]
mod tests {
    use crate::digest::{Digest, NodeDigest};
    use crate::serialize::test_serdeser_aux;
    use crate::{ChitchatId, Heartbeat};

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
        test_serdeser_aux(&digest, 104);
    }
}
