use std::fmt::Debug;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::Serializable;
use crate::serialize::Deserializable;

/// For the lifetime of a cluster, nodes can go down and come back up multiple times. They may also
/// die permanently. A [`ChitchatId`] is composed of three components:
/// - `node_id`: an identifier unique across the cluster.
/// - `generation_id`: a numeric identifier that distinguishes a node's state between restarts.
/// - `gossip_advertise_addr`: the socket address peers should use to gossip with the node.
///
/// The `generation_id` is used to detect when a node has restarted. It must be monotonically
/// increasing to differentiate the most recent state and must be incremented every time a node
/// leaves and rejoins the cluster. Backends such as Cassandra or Quickwit typically use the node's
/// startup time as the `generation_id`.
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

/// The current status of a key-value pair with regard to deletion.
///
/// In both the `Deleted` and `DeleteAfterTtl` status, the `Instant` is NOT the scheduled
/// time of deletion but the time at which the DeletionStatus was created.
#[derive(Clone, Copy, Debug)]
pub enum DeletionStatus {
    Set,
    /// The key-value pair is not visible but will be GCed only at Instant + grace period
    Deleted(Instant),
    /// The key-value pair is still visible and will be GCed at Instant + grace period
    DeleteAfterTtl(Instant),
}

#[cfg(test)]
impl PartialEq for DeletionStatus {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl DeletionStatus {
    pub fn time_of_start_scheduled_for_deletion(&self) -> Option<Instant> {
        match self {
            DeletionStatus::Set => None,
            DeletionStatus::Deleted(time_of_deletion)
            | DeletionStatus::DeleteAfterTtl(time_of_deletion) => Some(*time_of_deletion),
        }
    }
}

/// A versioned key-value pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    into = "VersionedValueForSerialization",
    from = "VersionedValueForSerialization"
)]
pub struct VersionedValue {
    pub value: String,
    pub version: Version,
    // The tombstone instant is transient:
    // Only the presence of a tombstone or not is serialized, and used in partial eq eq.
    pub status: DeletionStatus,
}

impl VersionedValue {
    #[cfg(test)]
    pub fn new(value: String, version: Version, is_tombstone: bool) -> VersionedValue {
        VersionedValue {
            value,
            version,
            status: if is_tombstone {
                DeletionStatus::Deleted(Instant::now())
            } else {
                DeletionStatus::Set
            },
        }
    }

    pub fn is_deleted(&self) -> bool {
        match self.status {
            DeletionStatus::Set => false,
            DeletionStatus::Deleted(_) => true,
            DeletionStatus::DeleteAfterTtl(_) => false,
        }
    }

    #[cfg(test)]
    pub fn for_test(value: &str, version: Version) -> Self {
        Self {
            value: value.to_string(),
            version,
            status: DeletionStatus::Set,
        }
    }
}

/// This `PartialEq` implementation is for test only.
/// It checks for the equality of value, version and delete_status.
/// However, it does NOT check the timestamp associated with the delete_status.
#[cfg(test)]
impl PartialEq for VersionedValue {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.version == other.version && self.status == other.status
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub(crate) struct KeyValueMutation {
    pub(crate) key: String,
    pub(crate) value: String,
    pub(crate) version: Version,
    pub(crate) status: DeletionStatusMutation,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[repr(u8)]
pub enum DeletionStatusMutation {
    Set = 0u8,
    Delete = 1u8,
    DeleteAfterTtl = 2u8,
}

impl DeletionStatusMutation {
    pub fn into_status(self, now: Instant) -> DeletionStatus {
        match self {
            DeletionStatusMutation::Set => DeletionStatus::Set,
            DeletionStatusMutation::DeleteAfterTtl => DeletionStatus::DeleteAfterTtl(now),
            DeletionStatusMutation::Delete => DeletionStatus::Deleted(now),
        }
    }

    pub fn scheduled_for_deletion(&self) -> bool {
        match self {
            DeletionStatusMutation::Set => false,
            DeletionStatusMutation::Delete | DeletionStatusMutation::DeleteAfterTtl => true,
        }
    }
}

impl From<DeletionStatus> for DeletionStatusMutation {
    fn from(deletion_status: DeletionStatus) -> Self {
        match deletion_status {
            DeletionStatus::Set => DeletionStatusMutation::Set,
            DeletionStatus::DeleteAfterTtl(_) => DeletionStatusMutation::DeleteAfterTtl,
            DeletionStatus::Deleted(_) => DeletionStatusMutation::Delete,
        }
    }
}

impl TryFrom<u8> for DeletionStatusMutation {
    type Error = ();

    fn try_from(state_code: u8) -> Result<Self, Self::Error> {
        match state_code {
            0u8 => Ok(DeletionStatusMutation::Set),
            1u8 => Ok(DeletionStatusMutation::Delete),
            2u8 => Ok(DeletionStatusMutation::DeleteAfterTtl),
            _ => Err(()),
        }
    }
}

impl From<DeletionStatusMutation> for u8 {
    fn from(state_mutation: DeletionStatusMutation) -> u8 {
        state_mutation as u8
    }
}

impl Serializable for DeletionStatusMutation {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(u8::from(*self));
    }

    fn serialized_len(&self) -> usize {
        1
    }
}

impl Deserializable for DeletionStatusMutation {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let deletion_status_code = <u8 as Deserializable>::deserialize(buf)?;
        DeletionStatusMutation::try_from(deletion_status_code)
            .map_err(|_| anyhow::anyhow!("Invalid deletion status code {deletion_status_code}"))
    }
}

impl<'a> From<&'a KeyValueMutation> for KeyValueMutationRef<'a> {
    fn from(mutation: &'a KeyValueMutation) -> KeyValueMutationRef<'a> {
        KeyValueMutationRef {
            key: mutation.key.as_str(),
            value: mutation.value.as_str(),
            version: mutation.version,
            state: mutation.status,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub(crate) struct KeyValueMutationRef<'a> {
    pub(crate) key: &'a str,
    pub(crate) value: &'a str,
    pub(crate) version: Version,
    pub(crate) state: DeletionStatusMutation,
}

impl Serializable for KeyValueMutationRef<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        Serializable::serialize(self.key, buf);
        Serializable::serialize(self.value, buf);
        Serializable::serialize(&self.version, buf);
        Serializable::serialize(&self.state, buf);
    }

    fn serialized_len(&self) -> usize {
        Serializable::serialized_len(self.key)
            + Serializable::serialized_len(self.value)
            + Serializable::serialized_len(&self.version)
            + Serializable::serialized_len(&self.state)
    }
}

impl Deserializable for KeyValueMutation {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let key: String = Deserializable::deserialize(buf)?;
        let value: String = Deserializable::deserialize(buf)?;
        let version: u64 = Deserializable::deserialize(buf)?;
        let state: DeletionStatusMutation = Deserializable::deserialize(buf)?;
        Ok(KeyValueMutation {
            key,
            value,
            version,
            status: state,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct VersionedValueForSerialization {
    pub value: String,
    pub version: Version,
    pub status: DeletionStatusMutation, /* TODO fixme. Deserialization could result in incorrect
                                         * ttls. */
}

impl From<VersionedValueForSerialization> for VersionedValue {
    fn from(versioned_value: VersionedValueForSerialization) -> Self {
        VersionedValue {
            value: versioned_value.value,
            version: versioned_value.version,
            status: versioned_value.status.into_status(Instant::now()),
        }
    }
}

impl From<VersionedValue> for VersionedValueForSerialization {
    fn from(versioned_value: VersionedValue) -> Self {
        VersionedValueForSerialization {
            value: versioned_value.value,
            version: versioned_value.version,
            status: DeletionStatusMutation::from(versioned_value.status),
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
        // Heartbeat is increased everytime we gossip or receive a message. In
        // real-world scenarios, this should not happen more than a few hundred
        // times per second so we should never overflow.
        self.0 = self.0.checked_add(1).expect("Heartbeat overflow");
    }
}

impl From<Heartbeat> for u64 {
    fn from(heartbeat: Heartbeat) -> Self {
        heartbeat.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deletion_status_to_u8() {
        let mut count_values = 0;
        for deletion_status_code in 0..=u8::MAX {
            let Ok(deletion_status) = DeletionStatusMutation::try_from(deletion_status_code) else {
                continue;
            };
            let deletion_status_code_deser_ser: u8 = deletion_status.into();
            assert_eq!(deletion_status_code, deletion_status_code_deser_ser);
            count_values += 1;
        }
        assert_eq!(count_values, 3);
    }
}
