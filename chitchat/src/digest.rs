use std::collections::BTreeMap;

use crate::serialize::*;
use crate::{ChitchatId, Version};

/// A digest represents is a piece of information summarizing
/// the staleness of one peer's data.
///
/// It is equivalent to a map
/// peer -> max version.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct Digest {
    pub(crate) node_max_version: BTreeMap<ChitchatId, Version>,
}

impl Digest {
    #[cfg(test)]
    pub fn add_node(&mut self, node: ChitchatId, max_version: Version) {
        self.node_max_version.insert(node, max_version);
    }
}

impl Serializable for Digest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.node_max_version.len() as u16).serialize(buf);
        for (chitchat_id, version) in &self.node_max_version {
            chitchat_id.serialize(buf);
            version.serialize(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let num_nodes = u16::deserialize(buf)?;
        let mut node_max_version: BTreeMap<ChitchatId, Version> = Default::default();
        for _ in 0..num_nodes {
            let chitchat_id = ChitchatId::deserialize(buf)?;
            let version = u64::deserialize(buf)?;
            node_max_version.insert(chitchat_id, version);
        }
        Ok(Digest { node_max_version })
    }

    fn serialized_len(&self) -> usize {
        let mut len = (self.node_max_version.len() as u16).serialized_len();
        for (chitchat_id, version) in &self.node_max_version {
            len += chitchat_id.serialized_len();
            len += version.serialized_len();
        }
        len
    }
}
