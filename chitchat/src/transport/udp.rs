use std::net::SocketAddr;

use anyhow::{bail, Context};
use async_trait::async_trait;
use tracing::warn;

use crate::serialize::Serializable;
use crate::transport::{Socket, Transport};
use crate::ChitchatMessage;

/// Maximum payload size (in bytes) for UDP.
const UDP_MTU: usize = 1_400;

pub struct UdpTransport;

#[async_trait]
impl Transport for UdpTransport {
    async fn open(&self, bind_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {
        let socket = tokio::net::UdpSocket::bind(bind_addr)
            .await
            .with_context(|| format!("Failed to bind to {bind_addr}/UDP for gossip."))?;
        Ok(Box::new(UdpSocket {
            buf_send: Vec::with_capacity(UDP_MTU),
            buf_recv: Box::new([0u8; UDP_MTU]),
            socket,
        }))
    }
}

struct UdpSocket {
    buf_send: Vec<u8>,
    buf_recv: Box<[u8; UDP_MTU]>,
    socket: tokio::net::UdpSocket,
}

#[async_trait]
impl Socket for UdpSocket {
    async fn send(&mut self, to_addr: SocketAddr, message: ChitchatMessage) -> anyhow::Result<()> {
        self.buf_send.clear();
        message.serialize(&mut self.buf_send);
        let buf_send_len = self.buf_send.len();
        if buf_send_len > UDP_MTU {
            bail!("Message larger ({buf_send_len} bytes) than mtu ({UDP_MTU} bytes)");
        }
        self.send_bytes(to_addr, &self.buf_send).await?;
        Ok(())
    }

    /// Recv needs to be cancellable.
    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, ChitchatMessage)> {
        loop {
            if let Some(message) = self.receive_one().await? {
                return Ok(message);
            }
        }
    }
}

impl UdpSocket {
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
                Ok(None)
            }
        }
    }

    pub(crate) async fn send_bytes(
        &self,
        to_addr: SocketAddr,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        self.socket
            .send_to(payload, to_addr)
            .await
            .context("Failed to send chitchat message to target")?;
        Ok(())
    }
}
