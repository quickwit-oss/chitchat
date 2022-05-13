use std::net::SocketAddr;

use anyhow::Context;
use tokio::net::UdpSocket;
use tracing::warn;

use crate::message::ChitchatMessage;
use crate::serialize::Serializable;
use async_trait::async_trait;

#[async_trait]
pub trait Transport: Send {
    // Only returns an error if the transport is broken and may not emit message
    // in the future.
    async fn send(&mut self, to: SocketAddr, msg: ChitchatMessage) -> anyhow::Result<()>;
    // Only returns an error if the transport is broken and may not receive message
    // in the future.
    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, ChitchatMessage)>;
}

const UDP_MTU: usize = 65_507;

pub struct UdpTransport {
    buf_send: Vec<u8>,
    buf_recv: Box<[u8; UDP_MTU]>,
    socket: UdpSocket,
}

impl UdpTransport {
    pub async fn open(bind_addr: SocketAddr) -> anyhow::Result<UdpTransport> {
        let socket = UdpSocket::bind(bind_addr).await
            .with_context(|| format!("Failed to bind to {bind_addr}/UDP for gossip."))?;
        Ok(UdpTransport { buf_send:  Vec::with_capacity(65_000), buf_recv: Box::new([0u8; UDP_MTU]), socket })
    }
}

#[async_trait]
impl Transport for UdpTransport {
    async fn send(&mut self, to: SocketAddr, message: ChitchatMessage) -> anyhow::Result<()> {
        self.buf_send.clear();
        message.serialize(&mut self.buf_send);
        self.socket.send_to(&self.buf_send, to).await
            .context("Failed to send chitchat message to target")?;
        // TODO should we inore errors
        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, ChitchatMessage)> {
        loop {
            if let Some(message) = self.receive_one().await? {
                return Ok(message);
            }
        }
    }
}

impl UdpTransport {
    async fn receive_one(&mut self) -> anyhow::Result<Option<(SocketAddr, ChitchatMessage)>> {
        let (len, from_addr) = self
            .socket
            .recv_from(&mut self.buf_recv[..])
            .await
            .context("Error while receiving UDP message")?;
        let mut buf = &self.buf_recv[..len];
        match ChitchatMessage::deserialize(&mut buf) {
            Ok(msg) => Ok(Some((from_addr, msg))),
            Err(err) => {
                warn!(payload_len=len, from=%from_addr, err=%err, "invalid-chitchat-payload");
                return Ok(None);
            }
        }
    }
}
