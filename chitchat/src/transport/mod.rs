use std::net::SocketAddr;

use async_trait::async_trait;

use crate::message::ChitchatEnvelope;

mod channel;
mod udp;
mod utils;

pub use channel::{ChannelTransport, Statistics};
pub use udp::{UdpSocket, UdpTransport};
pub use utils::TransportExt;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SendOutcome {
    /// Number of bytes sent to the transport layer by Chitchat.
    pub num_bytes_sent: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub struct RecvOutcome {
    /// Address of the peer that sent the message.
    pub from_addr: SocketAddr,
    /// Decoded Chitchat envelope.
    pub envelope: ChitchatEnvelope,
    /// Number of bytes received from the transport layer by Chitchat.
    pub num_bytes_received: usize,
}

#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>>;
}

#[async_trait]
pub trait Socket: Send + Sync + 'static {
    /// Returns the address this socket is bound to.
    fn local_addr(&self) -> anyhow::Result<SocketAddr>;

    // The envelope carries the protocol version selecting the wire format it
    // is encoded with (see `message_serialize`).
    //
    // Returns the number of bytes sent to the transport layer by Chitchat. Only returns an
    // error if the transport is broken and may not emit messages in the future.
    async fn send(
        &mut self,
        to: SocketAddr,
        envelope: ChitchatEnvelope,
    ) -> anyhow::Result<SendOutcome>;

    // Returns the envelope containing the message and its protocol version, the peer address, and
    // the number of bytes received from the transport layer by Chitchat. A responder can echo the
    // version (see `message_serialize`).
    //
    // Only returns an error if the transport is broken and may not receive message
    // in the future.
    async fn recv(&mut self) -> anyhow::Result<RecvOutcome>;
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use tokio::net::UdpSocket;
    use tokio::time::timeout;

    use super::Transport;
    use crate::MAX_UDP_DATAGRAM_PAYLOAD_SIZE;
    use crate::digest::Digest;
    use crate::message::{ChitchatEnvelope, ChitchatMessage, ProtocolVersion};
    use crate::transport::{ChannelTransport, RecvOutcome, UdpTransport};

    fn sample_syn_envelope() -> ChitchatEnvelope {
        ChitchatEnvelope {
            version: ProtocolVersion::V0,
            message: ChitchatMessage::Syn {
                cluster_id: "cluster_id".to_string(),
                digest: Digest::default(),
            },
        }
    }

    fn sample_compressed_syn_envelope() -> ChitchatEnvelope {
        ChitchatEnvelope {
            version: ProtocolVersion::V1,
            message: ChitchatMessage::Syn {
                cluster_id: "cluster_id".to_string(),
                digest: Digest::sample_for_test(100),
            },
        }
    }

    #[tokio::test]
    async fn test_udp_transport_ignore_invalid_payload() {
        let bind_addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let send_udp_socket = UdpSocket::bind(bind_addr).await.unwrap();
        let send_addr = send_udp_socket.local_addr().unwrap();
        let mut recv_socket = UdpTransport.open(bind_addr).await.unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();
        let invalid_payload = b"junk";
        send_udp_socket
            .send_to(&invalid_payload[..], recv_addr)
            .await
            .unwrap();
        let valid_envelope = sample_syn_envelope();
        let mut valid_payload: Vec<u8> = Vec::new();
        valid_envelope.serialize(&mut valid_payload);
        send_udp_socket
            .send_to(&valid_payload[..], recv_addr)
            .await
            .unwrap();
        let RecvOutcome {
            from_addr: send_addr2,
            envelope: received_envelope,
            num_bytes_received,
        } = recv_socket.recv().await.unwrap();
        assert_eq!(send_addr, send_addr2);
        assert_eq!(received_envelope, valid_envelope);
        assert_eq!(num_bytes_received, valid_payload.len());
    }

    async fn test_transport_cannot_open_twice_aux(transport: &dyn Transport) {
        let bind_addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let socket = transport.open(bind_addr).await.unwrap();
        let addr = socket.local_addr().unwrap();
        assert!(transport.open(addr).await.is_err());
    }

    async fn test_transport_recv_waits_for_envelope(transport: &dyn Transport) {
        let bind_addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let mut socket1 = transport.open(bind_addr).await.unwrap();
        let mut socket2 = transport.open(bind_addr).await.unwrap();
        let addr1 = socket1.local_addr().unwrap();
        let addr2 = socket2.local_addr().unwrap();
        assert!(
            timeout(Duration::from_millis(200), socket2.recv())
                .await
                .is_err()
        );
        let syn_envelope = sample_syn_envelope();
        let socket_recv_fut = tokio::task::spawn(async move { socket2.recv().await.unwrap() });
        tokio::time::sleep(Duration::from_millis(100)).await;
        socket1.send(addr2, syn_envelope).await.unwrap();
        let RecvOutcome {
            from_addr: exp1, ..
        } = socket_recv_fut.await.unwrap();
        assert_eq!(addr1, exp1);
    }

    async fn test_transport_socket_released_on_drop(transport: &dyn Transport) {
        let bind_addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let socket = transport.open(bind_addr).await.unwrap();
        let addr = socket.local_addr().unwrap();
        std::mem::drop(socket);
        let _new_socket = transport.open(addr).await.unwrap();
    }

    async fn test_transport_sending_to_unbound_addr_is_ok(transport: &dyn Transport) {
        let bind_addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let mut socket = transport.open(bind_addr).await.unwrap();
        let mut unbound_addr = socket.local_addr().unwrap();
        unbound_addr.set_ip([127, 0, 0, 2].into());
        socket
            .send(unbound_addr, sample_syn_envelope())
            .await
            .unwrap();
    }

    async fn test_transport_reports_num_bytes(transport: &dyn Transport) {
        let bind_addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let mut socket1 = transport.open(bind_addr).await.unwrap();
        let mut socket2 = transport.open(bind_addr).await.unwrap();
        let addr1 = socket1.local_addr().unwrap();
        let addr2 = socket2.local_addr().unwrap();
        assert_ne!(addr1, addr2);

        let envelope = sample_compressed_syn_envelope();
        let expected_num_bytes = envelope.serialize_to_vec().len();
        let uncompressed_num_bytes = ChitchatEnvelope {
            version: ProtocolVersion::V0,
            message: ChitchatMessage::Syn {
                cluster_id: "cluster_id".to_string(),
                digest: Digest::sample_for_test(100),
            },
        }
        .serialize_to_vec()
        .len();
        assert!(expected_num_bytes < uncompressed_num_bytes);

        let send_outcome = socket1.send(addr2, envelope).await.unwrap();
        let recv_outcome = socket2.recv().await.unwrap();

        assert_eq!(send_outcome.num_bytes_sent, expected_num_bytes);
        assert_eq!(recv_outcome.num_bytes_received, expected_num_bytes);
    }

    async fn test_transport_suite(transport: &dyn Transport) {
        test_transport_cannot_open_twice_aux(transport).await;
        test_transport_socket_released_on_drop(transport).await;
        test_transport_recv_waits_for_envelope(transport).await;
        test_transport_sending_to_unbound_addr_is_ok(transport).await;
        test_transport_reports_num_bytes(transport).await;
    }

    #[tokio::test]
    async fn test_transport_udp() {
        test_transport_suite(&UdpTransport).await;
    }

    #[tokio::test]
    async fn test_transport_in_mem() {
        test_transport_suite(&ChannelTransport::with_mtu(MAX_UDP_DATAGRAM_PAYLOAD_SIZE)).await;
    }
}
