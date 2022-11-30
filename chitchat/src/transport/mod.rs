use std::io;
use std::net::SocketAddr;

use async_trait::async_trait;

use crate::message::ChitchatMessage;

mod channel;
mod udp;
mod utils;

pub use channel::{ChannelTransport, Statistics};
pub use udp::UdpTransport;
pub use utils::TransportExt;

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("Io Error: {0}")]
    IoError(#[from] io::Error),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn open(&self, listen_addr: SocketAddr) -> Result<Box<dyn Socket>, TransportError>;
}

#[async_trait]
pub trait Socket: Send + Sync + 'static {
    // Only returns an error if the transport is broken and may not emit message
    // in the future.
    async fn send(&mut self, to: SocketAddr, msg: ChitchatMessage) -> Result<(), TransportError>;
    // Only returns an error if the transport is broken and may not receive message
    // in the future.
    async fn recv(&mut self) -> Result<(SocketAddr, ChitchatMessage), TransportError>;
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use tokio::net::UdpSocket;
    use tokio::time::timeout;

    use super::Transport;
    use crate::digest::Digest;
    use crate::message::ChitchatMessage;
    use crate::serialize::Serializable;
    use crate::transport::{ChannelTransport, UdpTransport};

    fn sample_syn_msg() -> ChitchatMessage {
        ChitchatMessage::Syn {
            cluster_id: "cluster_id".to_string(),
            digest: Digest::default(),
        }
    }

    #[tokio::test]
    async fn test_udp_transport_ignore_invalid_payload() {
        let recv_addr: SocketAddr = ([127, 0, 0, 1], 30_000u16).into();
        let send_addr: SocketAddr = ([127, 0, 0, 1], 30_001u16).into();
        let send_udp_socket: UdpSocket = UdpSocket::bind(send_addr).await.unwrap();
        let mut recv_socket = UdpTransport.open(recv_addr).await.unwrap();
        let invalid_payload = b"junk";
        send_udp_socket
            .send_to(&invalid_payload[..], recv_addr)
            .await
            .unwrap();
        let valid_message = sample_syn_msg();
        let mut valid_payload: Vec<u8> = Vec::new();
        valid_message.serialize(&mut valid_payload);
        send_udp_socket
            .send_to(&valid_payload[..], recv_addr)
            .await
            .unwrap();
        let (send_addr2, received_message) = recv_socket.recv().await.unwrap();
        assert_eq!(send_addr, send_addr2);
        assert_eq!(received_message, valid_message);
    }

    async fn test_transport_cannot_open_twice_aux(transport: &dyn Transport) {
        let addr: SocketAddr = ([127, 0, 0, 1], 10_000u16).into();
        let _socket = transport.open(addr).await.unwrap();
        assert!(transport.open(addr).await.is_err());
    }

    async fn test_transport_recv_waits_for_message(transport: &dyn Transport) {
        let addr1: SocketAddr = ([127, 0, 0, 1], 20_001u16).into();
        let addr2: SocketAddr = ([127, 0, 0, 1], 20_002u16).into();
        let mut socket1 = transport.open(addr1).await.unwrap();
        let mut socket2 = transport.open(addr2).await.unwrap();
        assert!(timeout(Duration::from_millis(200), socket2.recv())
            .await
            .is_err());
        let syn_message = sample_syn_msg();
        let socket_recv_fut = tokio::task::spawn(async move { socket2.recv().await.unwrap() });
        tokio::time::sleep(Duration::from_millis(100)).await;
        socket1.send(addr2, syn_message).await.unwrap();
        let (exp1, _received_msg) = socket_recv_fut.await.unwrap();
        assert_eq!(addr1, exp1);
    }

    async fn test_transport_socket_released_on_drop(transport: &dyn Transport) {
        let addr: SocketAddr = ([127, 0, 0, 1], 10_000u16).into();
        let socket = transport.open(addr).await.unwrap();
        std::mem::drop(socket);
        let _new_socket = transport.open(addr).await.unwrap();
    }

    async fn test_transport_sending_to_unbound_addr_is_ok(transport: &dyn Transport) {
        let addr: SocketAddr = ([127, 0, 0, 1], 40_000u16).into();
        let unbound_addr: SocketAddr = ([127, 0, 0, 1], 40_001u16).into();
        let mut socket = transport.open(addr).await.unwrap();
        socket.send(unbound_addr, sample_syn_msg()).await.unwrap()
    }

    async fn test_transport_suite(transport: &dyn Transport) {
        test_transport_cannot_open_twice_aux(transport).await;
        test_transport_socket_released_on_drop(transport).await;
        test_transport_recv_waits_for_message(transport).await;
        test_transport_sending_to_unbound_addr_is_ok(transport).await;
    }

    #[tokio::test]
    async fn test_transport_udp() {
        test_transport_suite(&UdpTransport).await;
    }

    #[tokio::test]
    async fn test_transport_in_mem() {
        test_transport_suite(&ChannelTransport::default()).await;
    }
}
