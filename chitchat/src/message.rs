use std::io::BufRead;

use anyhow::{bail, Context};

use crate::delta::Delta;
use crate::digest::Digest;
use crate::serialize::{Deserializable, Serializable};

const MAGIC_NUMBER: u16 = 45_139;

/// Chitchat message.
///
/// Each variant represents a step of the gossip "handshake"
/// between node A and node B.
/// The names {SYN, SYN-ACK, ACK} of the different steps are borrowed from
/// TCP handshake.
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
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum ProtocolVersion {
    V0 = 0,
}

impl ProtocolVersion {
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::V0),
            _ => None,
        }
    }

    pub fn to_code(self) -> u8 {
        self as u8
    }
}

#[derive(Clone, Copy)]
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
        buf.extend(MAGIC_NUMBER.to_le_bytes());
        ProtocolVersion::V0.to_code().serialize(buf);

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

    fn serialized_len(&self) -> usize {
        2 + 1
            + match self {
                ChitchatMessage::Syn { cluster_id, digest } => {
                    1 + cluster_id.serialized_len() + digest.serialized_len()
                }
                ChitchatMessage::SynAck { digest, delta } => {
                    1 + digest.serialized_len() + delta.serialized_len()
                }
                ChitchatMessage::Ack { delta } => 1 + delta.serialized_len(),
                ChitchatMessage::BadCluster => 1,
            }
    }
}

impl Deserializable for ChitchatMessage {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        if buf.len() < 3 {
            bail!("buffer too small to store the magic number and the protocol version");
        }
        let magic_number = u16::from_le_bytes(buf[0..2].try_into().unwrap());
        if magic_number != MAGIC_NUMBER {
            bail!("invalid chitchat magic number");
        }
        let protocol_version =
            ProtocolVersion::from_code(buf[2]).context("invalid protocol version")?;

        if protocol_version != ProtocolVersion::V0 {
            bail!(
                "unsupported protocol version `{}`",
                protocol_version.to_code()
            )
        }
        buf.consume(3);

        let message_type = buf
            .first()
            .copied()
            .and_then(MessageType::from_code)
            .context("invalid message type")?;
        buf.consume(1);

        match message_type {
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
}

#[cfg(test)]
mod tests {
    use crate::serialize::test_serdeser_aux;
    use crate::{ChitchatId, ChitchatMessage, Delta, Digest, Heartbeat};

    #[test]
    fn test_syn() {
        {
            let syn = ChitchatMessage::Syn {
                cluster_id: "cluster-a".to_string(),
                digest: Digest::default(),
            };
            test_serdeser_aux(&syn, 17);
        }
        {
            let mut digest = Digest::default();
            let node = ChitchatId::for_local_test(10_001);
            digest.add_node(node, Heartbeat(0), 0, 0);

            let syn = ChitchatMessage::Syn {
                cluster_id: "cluster-a".to_string(),
                digest,
            };
            test_serdeser_aux(&syn, 68);
        }
    }

    #[test]
    fn test_syn_ack() {
        {
            let syn_ack = ChitchatMessage::SynAck {
                digest: Digest::default(),
                delta: Delta::default(),
            };
            // 2 (magic number) + 1 (protocol version) + 1 (message tag) + 2 (digest len) + 1 (delta
            // end op)
            test_serdeser_aux(&syn_ack, 7);
        }
        {
            // 2 bytes.
            let mut digest = Digest::default();
            let node = ChitchatId::for_local_test(10_001);
            // +43 bytes = 27 bytes (ChitchatId) + 8 (hearbeat) + 8 (max_version).
            digest.add_node(node, Heartbeat(0), 0, 0);

            // 4 bytes
            let mut delta = Delta::default();
            let node = ChitchatId::for_local_test(10_001);
            // +27 bytes (ChitchatId)
            // + 2 bytes (node delta len)
            // + 8 bytes (last_gc_version)
            // + 8 bytes (from_version).
            delta.add_node(node.clone(), 0u64, 0u64);
            // +29 bytes.
            delta.add_kv(&node, "key", "value", 0, true);
            // That's compression kicking in.
            delta.set_serialized_len(60);

            let syn_ack = ChitchatMessage::SynAck { digest, delta };
            // 1 byte (protocol version) + 1 byte (message tag) + 53 bytes (digest) + 60 bytes
            // (delta).
            test_serdeser_aux(&syn_ack, 2 + 1 + 1 + 53 + 60);
        }
    }

    #[test]
    fn test_ack() {
        {
            let delta = Delta::default();
            let ack = ChitchatMessage::Ack { delta };
            test_serdeser_aux(&ack, 5);
        }
        {
            // 4 bytes.
            let mut delta = Delta::default();
            let node = ChitchatId::for_local_test(10_001);
            // +37 bytes = 27 bytes (ChitchatId) + 2 bytes (node delta len) + 8 bytes (heartbeat).
            delta.add_node(node.clone(), 0u64, 0u64);
            // +29 bytes.
            delta.add_kv(&node, "key", "value", 0, true);
            delta.set_serialized_len(60);
            let ack = ChitchatMessage::Ack { delta };
            test_serdeser_aux(&ack, 2 + 1 + 1 + 60);
        }
    }

    #[test]
    fn test_bad_cluster() {
        test_serdeser_aux(&ChitchatMessage::BadCluster, 4);
    }
}
