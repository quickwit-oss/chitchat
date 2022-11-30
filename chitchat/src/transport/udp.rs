use std::net::SocketAddr;

use async_trait::async_trait;
use tracing::warn;

use crate::serialize::Serializable;
use crate::transport::{Socket, Transport, TransportError};
use crate::{ChitchatMessage, MTU};

pub struct UdpTransport;

#[async_trait]
impl Transport for UdpTransport {
    async fn open(&self, bind_addr: SocketAddr) -> Result<Box<dyn Socket>, TransportError> {
        let socket = tokio::net::UdpSocket::bind(bind_addr).await?;
        Ok(Box::new(UdpSocket {
            buf_send: Vec::with_capacity(MTU),
            buf_recv: Box::new([0u8; MTU]),
            socket,
        }))
    }
}

struct UdpSocket {
    buf_send: Vec<u8>,
    buf_recv: Box<[u8; MTU]>,
    socket: tokio::net::UdpSocket,
}

#[async_trait]
impl Socket for UdpSocket {
    async fn send(
        &mut self,
        to_addr: SocketAddr,
        message: ChitchatMessage,
    ) -> Result<(), TransportError> {
        self.buf_send.clear();
        message.serialize(&mut self.buf_send);
        self.send_bytes(to_addr, &self.buf_send).await?;
        Ok(())
    }

    /// Recv needs to be cancellable.
    async fn recv(&mut self) -> Result<(SocketAddr, ChitchatMessage), TransportError> {
        loop {
            if let Some(message) = self.receive_one().await? {
                return Ok(message);
            }
        }
    }
}

impl UdpSocket {
    async fn receive_one(
        &mut self,
    ) -> Result<Option<(SocketAddr, ChitchatMessage)>, TransportError> {
        let (len, from_addr) = self.socket.recv_from(&mut self.buf_recv[..]).await?;
        let mut buf = &self.buf_recv[..len];
        match ChitchatMessage::deserialize(&mut buf) {
            Ok(msg) => Ok(Some((from_addr, msg))),
            Err(err) => {
                warn!(payload_len=len, from=%from_addr, err=%err, "invalid-chitchat-payload");
                Ok(None)
            }
        }
    }

    pub(crate) async fn send_bytes(
        &self,
        to_addr: SocketAddr,
        payload: &[u8],
    ) -> Result<(), TransportError> {
        self.socket.send_to(payload, to_addr).await?;
        Ok(())
    }
}
