//! Wire (de)serialization for [`ChitchatMessage`], via [`ChitchatEnvelope`].
//!
//! This module is deliberately kept separate from the message model
//! (`message.rs`) so that the on-wire format can grow across protocol
//! versions without cluttering the data types.
//!
//! The protocol version is not carried by [`ChitchatMessage`]: it is stamped
//! in [`ChitchatEnvelope`], which owns the wire format below, and it
//! drives the digest encoding (mirroring [`Digest`], whose encoding it
//! selects).

use std::io::BufRead;

use anyhow::{Context, bail};

use crate::delta::Delta;
use crate::digest::Digest;
use crate::message::{ChitchatEnvelope, ChitchatMessage, ProtocolVersion};
use crate::serialize::{Deserializable, Serializable};

const MAGIC_NUMBER: u16 = 45_139;

#[derive(Clone, Copy)]
#[repr(u8)]
enum MessageType {
    Syn = 0,
    SynAck = 1u8,
    Ack = 2u8,
    BadCluster = 3u8,
    #[cfg(test)]
    PanicForTest = 255u8,
}

impl MessageType {
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Syn),
            1 => Some(Self::SynAck),
            2 => Some(Self::Ack),
            3 => Some(Self::BadCluster),
            #[cfg(test)]
            255 => Some(Self::PanicForTest),
            _ => None,
        }
    }

    pub fn to_code(self) -> u8 {
        self as u8
    }
}

impl ChitchatEnvelope {
    /// Bytes written before the message body: magic number (2) + protocol
    /// version (1) + message type (1).
    pub(crate) const HEADER_LEN: usize = 2 + 1 + 1;

    /// Serializes the message. The protocol version is stamped in the header
    /// and drives the digest encoding.
    pub(crate) fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend(MAGIC_NUMBER.to_le_bytes());
        self.version.to_code().serialize(buf);

        match &self.message {
            ChitchatMessage::Syn { cluster_id, digest } => {
                buf.push(MessageType::Syn.to_code());
                digest.serialize(self.version, buf);
                cluster_id.serialize(buf);
            }
            ChitchatMessage::SynAck { digest, delta } => {
                buf.push(MessageType::SynAck.to_code());
                digest.serialize(self.version, buf);
                delta.serialize(buf);
            }
            ChitchatMessage::Ack { delta } => {
                buf.push(MessageType::Ack.to_code());
                delta.serialize(buf);
            }
            ChitchatMessage::BadCluster => {
                buf.push(MessageType::BadCluster.to_code());
            }
            #[cfg(test)]
            ChitchatMessage::PanicForTest => {
                buf.push(MessageType::PanicForTest.to_code());
            }
        }
    }

    pub(crate) fn serialize_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.serialize(&mut buf);
        buf
    }

    #[cfg(test)]
    pub(crate) fn serialized_len(&self) -> usize {
        Self::HEADER_LEN
            + match &self.message {
                ChitchatMessage::Syn { cluster_id, digest } => {
                    cluster_id.serialized_len() + digest.serialized_len(self.version)
                }
                ChitchatMessage::SynAck { digest, delta } => {
                    digest.serialized_len(self.version) + delta.serialized_len()
                }
                ChitchatMessage::Ack { delta } => delta.serialized_len(),
                ChitchatMessage::BadCluster => 0,
                #[cfg(test)]
                ChitchatMessage::PanicForTest => 0,
            }
    }

    /// Deserializes a message, reading the protocol version from the header.
    /// The version is needed both to decode the digest and by the caller: a
    /// responder echoes it so its reply is encoded with a format the
    /// initiator is guaranteed to understand.
    pub(crate) fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        if buf.len() < 3 {
            bail!("buffer too small to store the magic number and the protocol version");
        }
        let magic_number = u16::from_le_bytes(buf[0..2].try_into().unwrap());
        if magic_number != MAGIC_NUMBER {
            bail!("invalid chitchat magic number");
        }
        let version = ProtocolVersion::from_code(buf[2]).context("invalid protocol version")?;
        buf.consume(3);

        let message_type = buf
            .first()
            .copied()
            .and_then(MessageType::from_code)
            .context("invalid message type")?;
        buf.consume(1);

        let message = match message_type {
            MessageType::Syn => {
                let digest = Digest::deserialize(version, buf)?;
                let cluster_id = String::deserialize(buf)?;
                ChitchatMessage::Syn { cluster_id, digest }
            }
            MessageType::SynAck => {
                let digest = Digest::deserialize(version, buf)?;
                let delta = Delta::deserialize(buf)?;
                ChitchatMessage::SynAck { digest, delta }
            }
            MessageType::Ack => {
                let delta = Delta::deserialize(buf)?;
                ChitchatMessage::Ack { delta }
            }
            MessageType::BadCluster => ChitchatMessage::BadCluster,
            #[cfg(test)]
            MessageType::PanicForTest => ChitchatMessage::PanicForTest,
        };
        Ok(ChitchatEnvelope { version, message })
    }
}

#[cfg(test)]
mod tests {
    use crate::message::ChitchatEnvelope;
    use crate::{ChitchatId, Delta, Digest, Heartbeat};

    /// Checks the reported length, then round-trips and asserts both the
    /// message and the version survive.
    fn test_envelope_serdeser_aux(envelope: ChitchatEnvelope, expected_len: usize) {
        let buf = envelope.serialize_to_vec();
        assert_eq!(buf.len(), expected_len);
        assert_eq!(buf.len(), envelope.serialized_len());
        let deserialized = ChitchatEnvelope::deserialize(&mut &buf[..]).unwrap();
        assert_eq!(envelope, deserialized);
    }

    #[test]
    fn test_syn() {
        {
            let envelope = ChitchatEnvelope::new_syn_v0("cluster-a".to_string(), Digest::default());
            test_envelope_serdeser_aux(envelope, 17);
        }
        {
            let mut digest = Digest::default();
            let node = ChitchatId::for_local_test(10_001);
            digest.add_node(node, Heartbeat(0), 0, 0);

            let envelope = ChitchatEnvelope::new_syn_v0("cluster-a".to_string(), digest);
            test_envelope_serdeser_aux(envelope, 68);
        }
    }

    #[test]
    fn test_syn_compressed_digest_roundtrips() {
        let mut digest = Digest::default();
        for i in 0..100 {
            let node = ChitchatId::for_local_test(10_000 + i);
            digest.add_node(node, Heartbeat(i as u64), 0, i as u64);
        }
        // Emit the compressed wire format.
        let envelope = ChitchatEnvelope::new_syn_v1("cluster-a".to_string(), digest);
        let buf = envelope.serialize_to_vec();
        // A peer decodes it transparently, regardless of the encoding used.
        let deserialized = ChitchatEnvelope::deserialize(&mut &buf[..]).unwrap();
        assert_eq!(envelope, deserialized);
    }

    #[test]
    fn test_syn_ack() {
        {
            let envelope = ChitchatEnvelope::new_syn_ack_v0(Digest::default(), Delta::default());
            // 2 (magic number) + 1 (protocol version) + 1 (message tag) + 2 (digest len) + 1 (delta
            // end op)
            test_envelope_serdeser_aux(envelope, 7);
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
            // +22 bytes.
            delta.add_kv(&node, "key", "value", 1, true);
            // That's compression kicking in.
            delta.set_serialized_len(70);

            let envelope = ChitchatEnvelope::new_syn_ack_v0(digest, delta);
            // 1 byte (protocol version) + 1 byte (message tag) + 53 bytes (digest) + 60 bytes
            // (delta).
            test_envelope_serdeser_aux(envelope, 2 + 1 + 1 + 53 + 70);
        }
    }

    #[test]
    fn test_ack() {
        {
            let envelope = ChitchatEnvelope::new_ack_v0(Delta::default());
            test_envelope_serdeser_aux(envelope, 5);
        }
        {
            // 4 bytes.
            let mut delta = Delta::default();
            let node = ChitchatId::for_local_test(10_001);
            // +37 bytes = 27 bytes (ChitchatId) + 2 bytes (node delta len) + 8 bytes (heartbeat).
            delta.add_node(node.clone(), 0u64, 0u64);
            // +29 bytes.
            delta.add_kv(&node, "key", "value", 1, true);
            delta.set_serialized_len(70);
            let envelope = ChitchatEnvelope::new_ack_v0(delta);
            test_envelope_serdeser_aux(envelope, 2 + 1 + 1 + 70);
        }
    }

    #[test]
    fn test_bad_cluster() {
        test_envelope_serdeser_aux(ChitchatEnvelope::new_bad_cluster_v0(), 4);
    }
}
