use crate::delta::Delta;
use crate::digest::Digest;

/// Chitchat message.
///
/// Each variant represents a step of the gossip "handshake"
/// between node A and node B.
/// The names {SYN, SYN-ACK, ACK} of the different steps are borrowed from
/// TCP handshake.
///
/// The message is a pure data model. The protocol version used to encode it on
/// the wire is not part of the model: it is a (de)serialization parameter (see
/// `message_serialize`), stamped in the message header so peers can decode the
/// digest regardless of the sender's wire format.
#[derive(Debug, Eq, PartialEq)]
pub enum ChitchatMessage {
    /// Scuttlebutt SYN: node A initiates a handshake and sends its digest.
    Syn { cluster_id: String, digest: Digest },
    /// Scuttlebutt SYN-ACK: node B returns a partial update as described in the Scuttlebutt
    /// reconciliation algorithm and its own digest.
    SynAck { digest: Digest, delta: Delta },
    /// Scuttlebutt ACK: node A returns a partial update for B.
    Ack { delta: Delta },
    /// Node B rejects the SYN message because node A and B belong to different clusters.
    BadCluster,
    /// A message used by tests to trigger a panic
    #[cfg(test)]
    PanicForTest,
}

/// A [`ChitchatMessage`] paired with the protocol version it is encoded
/// with. The version is not part of the message model (see
/// [`ChitchatMessage`]'s doc); this struct is the unit that actually travels
/// on the wire, see `message_serialize`.
#[derive(Debug, Eq, PartialEq)]
pub struct ChitchatEnvelope {
    pub version: ProtocolVersion,
    pub message: ChitchatMessage,
}

impl ChitchatEnvelope {
    #[cfg(test)]
    pub(crate) fn new_syn_v0(cluster_id: String, digest: Digest) -> Self {
        Self {
            version: ProtocolVersion::V0,
            message: ChitchatMessage::Syn { cluster_id, digest },
        }
    }

    #[cfg(test)]
    pub(crate) fn new_syn_v1(cluster_id: String, digest: Digest) -> Self {
        Self {
            version: ProtocolVersion::V1,
            message: ChitchatMessage::Syn { cluster_id, digest },
        }
    }

    #[cfg(test)]
    pub(crate) fn new_syn_ack_v0(digest: Digest, delta: Delta) -> Self {
        Self {
            version: ProtocolVersion::V0,
            message: ChitchatMessage::SynAck { digest, delta },
        }
    }

    #[cfg(test)]
    pub(crate) fn new_ack_v0(delta: Delta) -> Self {
        Self {
            version: ProtocolVersion::V0,
            message: ChitchatMessage::Ack { delta },
        }
    }

    #[cfg(test)]
    pub(crate) fn new_bad_cluster_v0() -> Self {
        Self {
            version: ProtocolVersion::V0,
            message: ChitchatMessage::BadCluster,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_panic_for_test_v0() -> Self {
        Self {
            version: ProtocolVersion::V0,
            message: ChitchatMessage::PanicForTest,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u8)]
pub enum ProtocolVersion {
    /// The digest is serialized using the V0 (uncompressed) wire format.
    V0 = 0,
    /// The digest is serialized using the V1 (compressed) wire format.
    V1 = 1,
}

impl ProtocolVersion {
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::V0),
            1 => Some(Self::V1),
            _ => None,
        }
    }

    pub fn to_code(self) -> u8 {
        self as u8
    }
}
