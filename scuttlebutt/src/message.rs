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

use std::io::BufRead;

use anyhow::Context;

use crate::delta::Delta;
use crate::digest::Digest;
use crate::serialize::Serializable;

/// ScuttleButt message.
///
/// Each variant represents a step of the gossip "handshake"
/// between node A and node B.
/// The names {Syn, SynAck, Ack} of the different steps are borrowed from
/// TCP Handshake.
#[derive(Debug, PartialEq)]
pub enum ScuttleButtMessage {
    /// Node A initiates handshakes.
    Syn {
        cluster_id: String,
        digest: Digest,
    },
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

impl Serializable for ScuttleButtMessage {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            ScuttleButtMessage::Syn {
                cluster_id,
                digest,
            } => {
                buf.push(MessageType::Syn.to_code());
                digest.serialize(buf);
                cluster_id.serialize(buf);
            }
            ScuttleButtMessage::SynAck { digest, delta } => {
                buf.push(MessageType::SynAck.to_code());
                digest.serialize(buf);
                delta.serialize(buf);
            }
            ScuttleButtMessage::Ack { delta } => {
                buf.push(MessageType::Ack.to_code());
                delta.serialize(buf);
            }
            ScuttleButtMessage::BadCluster => {
                buf.push(MessageType::BadCluster.to_code());
            }
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let code = buf
            .get(0)
            .cloned()
            .and_then(MessageType::from_code)
            .context("Invalid message type")?;
        buf.consume(1);
        match code {
            MessageType::Syn => {
                let digest = Digest::deserialize(buf)?;
                let cluster_id = String::deserialize(buf)?;
                Ok(Self::Syn {
                    cluster_id,
                    digest,
                })
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
            ScuttleButtMessage::Syn {
                cluster_id,
                digest,
            } => 1 + cluster_id.serialized_len() + digest.serialized_len(),
            ScuttleButtMessage::SynAck { digest, delta } => {
                1 + digest.serialized_len() + delta.serialized_len()
            }
            ScuttleButtMessage::Ack { delta } => 1 + delta.serialized_len(),
            ScuttleButtMessage::BadCluster => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::serialize::test_serdeser_aux;
    use crate::{Digest, ScuttleButtMessage};

    #[test]
    fn test_syn() {
        let mut digest = Digest::default();
        digest.add_node("node1".into(), 1);
        digest.add_node("node2".into(), 2);
        let syn = ScuttleButtMessage::Syn {
            cluster_id: "cluster-a".to_string(),
            digest,
        };
        test_serdeser_aux(&syn, 58);
    }

    #[test]
    fn test_bad_cluster() {
        test_serdeser_aux(&ScuttleButtMessage::BadCluster, 1);
    }
}
