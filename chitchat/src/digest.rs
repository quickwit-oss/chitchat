use std::collections::BTreeMap;

use crate::serialize::*;
use crate::{NodeId, Version};

/// A digest represents is a piece of information summarizing
/// the staleness of one peer's data.
///
/// It is equivalent to a map
/// peer -> max version.
#[derive(Debug, Default, PartialEq)]
pub struct Digest {
    pub(crate) node_max_version: BTreeMap<NodeId, Version>,
}

impl Digest {
    #[cfg(test)]
    pub fn add_node(&mut self, node: NodeId, max_version: Version) {
        self.node_max_version.insert(node, max_version);
    }
}

impl Serializable for Digest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.node_max_version.len() as u16).serialize(buf);
        for (node_id, version) in &self.node_max_version {
            node_id.serialize(buf);
            version.serialize(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let num_nodes = u16::deserialize(buf)?;
        let mut node_max_version: BTreeMap<NodeId, Version> = Default::default();
        for _ in 0..num_nodes {
            let node_id = NodeId::deserialize(buf)?;
            let version = u64::deserialize(buf)?;
            node_max_version.insert(node_id, version);
        }
        Ok(Digest { node_max_version })
    }

    fn serialized_len(&self) -> usize {
        let mut len = (self.node_max_version.len() as u16).serialized_len();
        for (node_id, version) in &self.node_max_version {
            len += node_id.serialized_len();
            len += version.serialized_len();
        }
        len
    }
}
