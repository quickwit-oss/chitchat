use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context};
use async_trait::async_trait;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info};

use crate::serialize::Serializable;
use crate::transport::{Socket, Transport};
use crate::ChitchatMessage;

const MAX_MESSAGE_PER_CHANNEL: usize = 100;

#[derive(Debug, Clone, Copy, Default)]
pub struct Statistics {
    pub num_bytes_total: u64,
    pub num_messages_total: u64,
}

impl Statistics {
    pub fn record_message_len(&mut self, message_num_bytes: usize) {
        self.num_bytes_total += message_num_bytes as u64;
        self.num_messages_total += 1;
    }
}

#[derive(Default)]
struct ChannelTransportInner {
    send_channels: HashMap<SocketAddr, Sender<(SocketAddr, ChitchatMessage)>>,
    statistics: Statistics,
    pub removed_links: HashMap<SocketAddr, HashSet<SocketAddr>>,
}

#[derive(Clone, Default)]
pub struct ChannelTransport {
    inner: Arc<Mutex<ChannelTransportInner>>,
    mtu_opt: Option<usize>,
}

#[async_trait]
impl Transport for ChannelTransport {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {
        let mut inner_lock = self.inner.lock().unwrap();
        let (message_tx, message_rx) = tokio::sync::mpsc::channel(MAX_MESSAGE_PER_CHANNEL);
        if inner_lock.send_channels.contains_key(&listen_addr) {
            bail!("Address not available `{listen_addr}`");
        }
        inner_lock.send_channels.insert(listen_addr, message_tx);
        Ok(Box::new(InProcessSocket {
            listen_addr,
            broker: self.clone(),
            message_rx,
        }))
    }
}

impl ChannelTransport {
    pub fn with_mtu(mtu: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ChannelTransportInner::default())),
            mtu_opt: Some(mtu),
        }
    }

    pub fn statistics(&self) -> Statistics {
        self.inner.lock().unwrap().statistics
    }

    pub async fn add_link(&self, from_addr: SocketAddr, to_addr: SocketAddr) {
        let mut inner_lock = self.inner.lock().unwrap();
        if let Some(from_addr_entry) = inner_lock.removed_links.get_mut(&from_addr) {
            from_addr_entry.remove(&to_addr);
        }
        if let Some(to_addr_entry) = inner_lock.removed_links.get_mut(&to_addr) {
            to_addr_entry.remove(&from_addr);
        }
    }

    pub async fn remove_link(&self, from_addr: SocketAddr, to_addr: SocketAddr) {
        let mut inner_lock = self.inner.lock().unwrap();
        let from_addr_entry = inner_lock
            .removed_links
            .entry(from_addr)
            .or_insert_with(HashSet::new);
        from_addr_entry.insert(to_addr);
        let to_addr_entry = inner_lock
            .removed_links
            .entry(to_addr)
            .or_insert_with(HashSet::new);
        to_addr_entry.insert(from_addr);
    }

    async fn send(
        &self,
        from_addr: SocketAddr,
        to_addr: SocketAddr,
        message: ChitchatMessage,
    ) -> anyhow::Result<()> {
        let num_bytes = message.serialized_len();
        if let Some(mtu) = self.mtu_opt {
            if num_bytes > mtu {
                bail!("Serialized message size exceeds MTU.");
            }
        }
        debug!(num_bytes = num_bytes, "send");
        let mut inner_lock = self.inner.lock().unwrap();
        inner_lock.statistics.record_message_len(num_bytes);
        if let Some(to_addrs) = inner_lock.removed_links.get(&from_addr) {
            if to_addrs.contains(&to_addr) {
                return Ok(());
            }
        }
        if let Some(message_tx) = inner_lock.send_channels.get(&to_addr) {
            // if the channel is saturated, we start dropping messages.
            let _ = message_tx.try_send((from_addr, message));
        }
        Ok(())
    }

    fn close(&self, addr: SocketAddr) {
        info!(addr=%addr, "close");
        let mut inner_lock = self.inner.lock().unwrap();
        inner_lock.send_channels.remove(&addr);
    }
}

struct InProcessSocket {
    listen_addr: SocketAddr,
    broker: ChannelTransport,
    message_rx: Receiver<(SocketAddr, ChitchatMessage)>,
}

#[async_trait]
impl Socket for InProcessSocket {
    async fn send(&mut self, to_addr: SocketAddr, message: ChitchatMessage) -> anyhow::Result<()> {
        self.broker.send(self.listen_addr, to_addr, message).await?;
        Ok(())
    }

    /// Recv needs to be cancellable.
    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, ChitchatMessage)> {
        let (from_addr, message) = self.message_rx.recv().await.context("Channel closed")?;
        Ok((from_addr, message))
    }
}

impl Drop for InProcessSocket {
    fn drop(&mut self) {
        self.broker.close(self.listen_addr);
    }
}
