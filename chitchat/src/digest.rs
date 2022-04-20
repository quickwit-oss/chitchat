// Copyright (C) 2022 Quickwit, Inc.
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
