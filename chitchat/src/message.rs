use std::io::BufRead;

use anyhow::Context;

use crate::delta::Delta;
use crate::digest::Digest;
use crate::serialize::Serializable;

/// Chitchat message.
///
/// Each variant represents a step of the gossip "handshake"
/// between node A and node B.
/// The names {Syn, SynAck, Ack} of the different steps are borrowed from
/// TCP Handshake.
#[derive(Debug, PartialEq)]
pub enum ChitchatMessage {
    /// Node A initiates handshakes.
    Syn { cluster_id: String, digest: Digest },
    /// Node B returns a partial update as described
    /// in the scuttlebutt reconcialiation algorithm,
    /// and returns its own checksum.
    SynAck { digest: Digest, delta: Delta },
    /// Node A returns a partial update for B.
    Ack { delta: Delta },
    /// Node B rejects the Syn message because of a
    /// cluster name mismatch between the peers.
    BadCluster,
}

#[derive(Copy, Clone)]
#[repr(u8)]
enum MessageType {
    Syn = 0,
    SynAck = 1u8,
    Ack = 2u8,
    BadCluster = 3u8,
}

impl MessageType {
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Syn),
            1 => Some(Self::SynAck),
            2 => Some(Self::Ack),
            3 => Some(Self::BadCluster),
            _ => None,
        }
    }
    pub fn to_code(self) -> u8 {
        self as u8
    }
}

impl Serializable for ChitchatMessage {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            ChitchatMessage::Syn { cluster_id, digest } => {
                buf.push(MessageType::Syn.to_code());
                digest.serialize(buf);
                cluster_id.serialize(buf);
            }
            ChitchatMessage::SynAck { digest, delta } => {
                buf.push(MessageType::SynAck.to_code());
                digest.serialize(buf);
                delta.serialize(buf);
            }
            ChitchatMessage::Ack { delta } => {
                buf.push(MessageType::Ack.to_code());
                delta.serialize(buf);
            }
            ChitchatMessage::BadCluster => {
                buf.push(MessageType::BadCluster.to_code());
            }
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let code = buf
            .first()
            .cloned()
            .and_then(MessageType::from_code)
            .context("Invalid message type")?;
        buf.consume(1);
        match code {
            MessageType::Syn => {
                let digest = Digest::deserialize(buf)?;
                let cluster_id = String::deserialize(buf)?;
                Ok(Self::Syn { cluster_id, digest })
            }
            MessageType::SynAck => {
                let digest = Digest::deserialize(buf)?;
                let delta = Delta::deserialize(buf)?;
                Ok(Self::SynAck { digest, delta })
            }
            MessageType::Ack => {
                let delta = Delta::deserialize(buf)?;
                Ok(Self::Ack { delta })
            }
            MessageType::BadCluster => Ok(Self::BadCluster),
        }
    }

    fn serialized_len(&self) -> usize {
        match self {
            ChitchatMessage::Syn { cluster_id, digest } => {
                1 + cluster_id.serialized_len() + digest.serialized_len()
            }
            ChitchatMessage::SynAck { digest, delta } => syn_ack_serialized_len(digest, delta),
            ChitchatMessage::Ack { delta } => 1 + delta.serialized_len(),
            ChitchatMessage::BadCluster => 1,
        }
    }
}

pub(crate) fn syn_ack_serialized_len(digest: &Digest, delta: &Delta) -> usize {
    1 + digest.serialized_len() + delta.serialized_len()
}

#[cfg(test)]
mod tests {
    use crate::serialize::test_serdeser_aux;
    use crate::{ChitchatMessage, Digest, NodeId};

    #[test]
    fn test_syn() {
        let mut digest = Digest::default();
        digest.add_node(NodeId::for_test_localhost(10_001), 1);
        digest.add_node(NodeId::for_test_localhost(10_002), 2);
        let syn = ChitchatMessage::Syn {
            cluster_id: "cluster-a".to_string(),
            digest,
        };
        test_serdeser_aux(&syn, 68);
    }

    #[test]
    fn test_bad_cluster() {
        test_serdeser_aux(&ChitchatMessage::BadCluster, 1);
    }
}
