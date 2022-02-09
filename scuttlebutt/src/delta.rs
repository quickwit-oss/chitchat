// Copyright (C) 2021 Quickwit, Inc.
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
use std::mem;

use crate::serialize::*;
#[cfg(test)]
use crate::Version;
use crate::VersionedValue;

#[derive(Default, Eq, PartialEq, Debug, Clone)]
pub struct Delta {
    pub(crate) node_deltas: BTreeMap<String, NodeDelta>,
}

impl Serializable for Delta {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.node_deltas.len() as u16).serialize(buf);
        for (node_id, node_delta) in &self.node_deltas {
            node_id.serialize(buf);
            node_delta.serialize(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let mut node_deltas: BTreeMap<String, NodeDelta> = Default::default();
        let num_nodes = u16::deserialize(buf)?;
        for _ in 0..num_nodes {
            let node_id = String::deserialize(buf)?;
            let node_delta = NodeDelta::deserialize(buf)?;
            node_deltas.insert(node_id, node_delta);
        }
        Ok(Delta { node_deltas })
    }

    fn serialized_len(&self) -> usize {
        let mut len = 2;
        for (node_id, node_delta) in &self.node_deltas {
            len += node_id.serialized_len();
            len += node_delta.serialized_len();
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
    pub fn add_node_delta(&mut self, node_id: &str, key: &str, value: &str, version: Version) {
        self.node_deltas
            .entry(node_id.to_string())
            .or_default()
            .key_values
            .insert(
                key.to_string(),
                VersionedValue {
                    value: value.to_string(),
                    version,
                },
            );
    }
}

#[derive(serde::Serialize, Default, Eq, PartialEq, Debug, Clone)]
pub(crate) struct NodeDelta {
    pub key_values: BTreeMap<String, VersionedValue>,
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
    current_node_id: String,
    current_node_delta: NodeDelta,
    reached_capacity: bool,
}

impl DeltaWriter {
    pub fn with_mtu(mtu: usize) -> Self {
        DeltaWriter {
            delta: Delta::default(),
            mtu,
            num_bytes: 2,
            current_node_id: String::new(),
            current_node_delta: NodeDelta::default(),
            reached_capacity: false,
        }
    }
    fn flush(&mut self) {
        let node_id = mem::take(&mut self.current_node_id);
        let node_delta = mem::take(&mut self.current_node_delta);
        if node_id.is_empty() {
            return;
        }
        self.delta.node_deltas.insert(node_id, node_delta);
    }

    pub fn add_node(&mut self, node_id: &str) -> bool {
        assert!(!node_id.is_empty());
        assert!(node_id != self.current_node_id);
        assert!(!self.delta.node_deltas.contains_key(node_id));
        self.flush();
        if !self.attempt_add_bytes(2 + node_id.len() + 2) {
            return false;
        }
        self.current_node_id = node_id.to_string();
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

    pub fn add_kv(&mut self, key: &str, versioned_value: VersionedValue) -> bool {
        assert!(!self.current_node_delta.key_values.contains_key(key));
        if !self.attempt_add_bytes(2 + key.len() + 2 + versioned_value.value.len() + 8) {
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
        for (key, VersionedValue { value, version }) in &self.key_values {
            key.serialize(buf);
            value.serialize(buf);
            version.serialize(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let mut key_values: BTreeMap<String, VersionedValue> = Default::default();
        let num_kvs = u16::deserialize(buf)?;
        for _ in 0..num_kvs {
            let key = String::deserialize(buf)?;
            let value = String::deserialize(buf)?;
            let version = u64::deserialize(buf)?;
            key_values.insert(key, VersionedValue { value, version });
        }
        Ok(NodeDelta { key_values })
    }

    fn serialized_len(&self) -> usize {
        let mut len = 2;
        for (key, VersionedValue { value, version }) in &self.key_values {
            len += key.serialized_len();
            len += value.serialized_len();
            len += version.serialized_len();
        }
        len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_serialization_default() {
        test_serdeser_aux(&Delta::default(), 2);
    }

    #[test]
    fn test_delta_serialization_simple() {
        let mut delta_writer = DeltaWriter::with_mtu(108);
        delta_writer.add_node("node1");
        delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1,
            },
        );
        delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
            },
        );
        delta_writer.add_node("node2");
        delta_writer.add_kv(
            "key21",
            VersionedValue {
                value: "val21".to_string(),
                version: 2,
            },
        );
        delta_writer.add_kv(
            "key22",
            VersionedValue {
                value: "val22".to_string(),
                version: 3,
            },
        );
        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 108);
    }

    #[test]
    fn test_delta_serialization_simple_node() {
        let mut delta_writer = DeltaWriter::with_mtu(64);
        assert!(delta_writer.add_node("node1"));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1
            }
        ));
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2
            }
        ));
        assert!(delta_writer.add_node("node2"));
        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 64);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_node() {
        let mut delta_writer = DeltaWriter::with_mtu(63);
        assert!(delta_writer.add_node("node1"));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1
            }
        ));
        assert!(delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2
            }
        ));
        assert!(!delta_writer.add_node("node2"));
        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 55);
    }

    #[test]
    fn test_delta_serialization_exceed_mtu_on_add_kv() {
        let mut delta_writer = DeltaWriter::with_mtu(54);
        assert!(delta_writer.add_node("node1"));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1
            }
        ));
        assert!(!delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2
            }
        ));
        let delta: Delta = delta_writer.into();
        test_serdeser_aux(&delta, 33);
    }

    #[test]
    #[should_panic]
    fn test_delta_serialization_panic_if_add_after_exceed() {
        let mut delta_writer = DeltaWriter::with_mtu(54);
        assert!(delta_writer.add_node("node1"));
        assert!(delta_writer.add_kv(
            "key11",
            VersionedValue {
                value: "val11".to_string(),
                version: 1
            }
        ));
        assert!(!delta_writer.add_kv(
            "key12",
            VersionedValue {
                value: "val12".to_string(),
                version: 2
            }
        ));
        delta_writer.add_kv(
            "key13",
            VersionedValue {
                value: "val12".to_string(),
                version: 2,
            },
        );
    }
}
