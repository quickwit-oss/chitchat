use std::fmt::Debug;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::serialize::Deserializable;
use crate::Serializable;

/// For the lifetime of a cluster, nodes can go down and come back up multiple times. They may also
/// die permanently. A [`ChitchatId`] is composed of three components:
/// - `node_id`: an identifier unique across the cluster.
/// - `generation_id`: a numeric identifier that distinguishes a node's state between restarts.
/// - `gossip_advertise_addr`: the socket address peers should use to gossip with the node.
///
/// The `generation_id` is used to detect when a node has restarted. It must be monotonically
/// increasing to differentiate the most recent state and must be incremented every time a node
/// leaves and rejoins the cluster. Backends such as Cassandra or Quickwit typically use the node's
/// startup time as the `generation_id`. Applications with stable state across restarts can use a
/// constant `generation_id`, for instance, `0`.
#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ChitchatId {
    /// An identifier unique across the cluster.
    pub node_id: String,
    /// A numeric identifier incremented every time the node leaves and rejoins the cluster.
    pub generation_id: u64,
    /// The socket address peers should use to gossip with the node.
    pub gossip_advertise_addr: SocketAddr,
}

impl Debug for ChitchatId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.node_id.as_str(),
            self.generation_id,
            self.gossip_advertise_addr
        )
    }
}

impl ChitchatId {
    pub fn new(node_id: String, generation_id: u64, gossip_advertise_addr: SocketAddr) -> Self {
        Self {
            node_id,
            generation_id,
            gossip_advertise_addr,
        }
    }
}

#[cfg(any(test, feature = "testsuite"))]
impl ChitchatId {
    /// Returns the gossip advertise port for performing assertions during tests.
    pub fn advertise_port(&self) -> u16 {
        self.gossip_advertise_addr.port()
    }

    /// Creates a new [`ChitchatId`] for local testing.
    pub fn for_local_test(port: u16) -> Self {
        Self::new(format!("node-{port}"), 0, ([127, 0, 0, 1], port).into())
    }
}

/// A versioned key-value pair.
#[derive(Debug, Clone, Eq, Serialize, Deserialize)]
#[serde(
    into = "VersionedValueForSerialization",
    from = "VersionedValueForSerialization"
)]
pub struct VersionedValue {
    pub value: String,
    pub version: Version,
    // The tombstone instant is transient:
    // Only the presence of a tombstone or not is serialized, and used in partial eq eq.
    pub(crate) tombstone: Option<Instant>,
}

impl VersionedValue {
    pub fn new(value: String, version: Version, is_tombstone: bool) -> VersionedValue {
        VersionedValue {
            value,
            version,
            tombstone: if is_tombstone {
                Some(Instant::now())
            } else {
                None
            },
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.tombstone.is_some()
    }

    #[cfg(test)]
    pub fn for_test(value: &str, version: Version) -> Self {
        Self {
            value: value.to_string(),
            version,
            tombstone: None,
        }
    }
}

impl PartialEq for VersionedValue {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
            && self.version.eq(&other.version)
            && self.is_tombstone().eq(&other.is_tombstone())
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub(crate) struct KeyValueMutation {
    pub(crate) key: String,
    pub(crate) value: String,
    pub(crate) version: Version,
    pub(crate) tombstone: bool,
}

impl<'a> From<&'a KeyValueMutation> for KeyValueMutationRef<'a> {
    fn from(mutation: &'a KeyValueMutation) -> KeyValueMutationRef<'a> {
        KeyValueMutationRef {
            key: mutation.key.as_str(),
            value: mutation.value.as_str(),
            version: mutation.version,
            tombstone: mutation.tombstone,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub(crate) struct KeyValueMutationRef<'a> {
    pub(crate) key: &'a str,
    pub(crate) value: &'a str,
    pub(crate) version: Version,
    pub(crate) tombstone: bool,
}

impl<'a> Serializable for KeyValueMutationRef<'a> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        Serializable::serialize(self.key, buf);
        Serializable::serialize(self.value, buf);
        Serializable::serialize(&self.version, buf);
        Serializable::serialize(&self.tombstone, buf);
    }

    fn serialized_len(&self) -> usize {
        Serializable::serialized_len(self.key)
            + Serializable::serialized_len(self.value)
            + Serializable::serialized_len(&self.version)
            + Serializable::serialized_len(&self.tombstone)
    }
}

impl Deserializable for KeyValueMutation {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let key: String = Deserializable::deserialize(buf)?;
        let value: String = Deserializable::deserialize(buf)?;
        let version: u64 = Deserializable::deserialize(buf)?;
        let tombstone: bool = Deserializable::deserialize(buf)?;
        Ok(KeyValueMutation {
            key,
            value,
            version,
            tombstone,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct VersionedValueForSerialization {
    pub value: String,
    pub version: Version,
    pub is_tombstone: bool,
}

impl From<VersionedValueForSerialization> for VersionedValue {
    fn from(versioned_value: VersionedValueForSerialization) -> Self {
        VersionedValue::new(
            versioned_value.value,
            versioned_value.version,
            versioned_value.is_tombstone,
        )
    }
}

impl From<VersionedValue> for VersionedValueForSerialization {
    fn from(versioned_value: VersionedValue) -> Self {
        VersionedValueForSerialization {
            value: versioned_value.value,
            version: versioned_value.version,
            is_tombstone: versioned_value.tombstone.is_some(),
        }
    }
}

/// The current version of a key.
pub type Version = u64;

/// The current heartbeat of a node.
#[derive(
    Debug, Clone, Copy, Default, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Heartbeat(pub(crate) u64);

impl Heartbeat {
    pub(crate) fn inc(&mut self) {
        self.0 = self.0.wrapping_add(1);
    }
}

impl From<Heartbeat> for u64 {
    fn from(heartbeat: Heartbeat) -> Self {
        heartbeat.0
    }
}
