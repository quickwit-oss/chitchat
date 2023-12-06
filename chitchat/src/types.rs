use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

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
// This type doesn't implement Eq & co because there are multiple notions of equality depending
// on what you want to do with it. Nodes with the same node_id are the same by definition, so
// sometime checking node_id is enough, but sometime we want to compare ChitchatId for generations,
// in which case node_id+generation_id needs to be compared. Mixing both is easy and can lead to
// bugs. Instead you have to use dedicated methods and/or wrappers depending on what equality means
// for you in this context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChitchatId {
    /// An identifier unique across the cluster.
    pub node_id: String,
    /// A numeric identifier incremented every time the node leaves and rejoins the cluster.
    pub generation_id: u64,
    /// The socket address peers should use to gossip with the node.
    pub gossip_advertise_addr: SocketAddr,
}

impl ChitchatId {
    pub fn new(node_id: String, generation_id: u64, gossip_advertise_addr: SocketAddr) -> Self {
        Self {
            node_id,
            generation_id,
            gossip_advertise_addr,
        }
    }

    pub fn eq_node_id(&self, other: &ChitchatId) -> bool {
        self.node_id == other.node_id
    }

    pub fn eq_generation(&self, other: &ChitchatId) -> bool {
        self.eq_node_id(other) && self.generation_id == other.generation_id
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

#[derive(Debug, Clone)]
pub struct ChitchatIdNodeEq(pub ChitchatId);

impl Eq for ChitchatIdNodeEq {}
impl PartialEq for ChitchatIdNodeEq {
    fn eq(&self, other: &ChitchatIdNodeEq) -> bool {
        self.0.eq_node_id(&other.0)
    }
}
impl Hash for ChitchatIdNodeEq {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.node_id.hash(state);
    }
}
impl Ord for ChitchatIdNodeEq {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.node_id.cmp(&other.0.node_id)
    }
}
impl PartialOrd for ChitchatIdNodeEq {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub struct ChitchatIdGenerationEq(pub ChitchatId);

impl Eq for ChitchatIdGenerationEq {}
impl PartialEq for ChitchatIdGenerationEq {
    fn eq(&self, other: &ChitchatIdGenerationEq) -> bool {
        self.0.eq_generation(&other.0)
    }
}
impl Hash for ChitchatIdGenerationEq {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.node_id.hash(state);
        self.0.generation_id.hash(state);
    }
}
impl Ord for ChitchatIdGenerationEq {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .node_id
            .cmp(&other.0.node_id)
            .then(self.0.generation_id.cmp(&other.0.generation_id))
    }
}
impl PartialOrd for ChitchatIdGenerationEq {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A versioned key-value pair.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct VersionedValue {
    pub value: String,
    pub version: Version,
    pub tombstone: Option<u64>,
}

#[cfg(test)]
impl VersionedValue {
    pub fn for_test(value: &str, version: Version) -> Self {
        Self {
            value: value.to_string(),
            version,
            tombstone: None,
        }
    }
}

/// The current version of a key.
pub type Version = u64;

/// The highest version of a key in a node's state.
pub type MaxVersion = u64;

/// The current heartbeat of a node.
#[derive(
    Debug, Clone, Copy, Default, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Heartbeat(pub(crate) u64);

impl Heartbeat {
    pub(crate) fn inc(&mut self) {
        self.0 += 1;
    }
}

impl From<Heartbeat> for u64 {
    fn from(heartbeat: Heartbeat) -> Self {
        heartbeat.0
    }
}
