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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rand::prelude::*;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time;

use crate::failure_detector::FailureDetectorConfig;
use crate::message::ScuttleButtMessage;
use crate::serialize::Serializable;
use crate::ScuttleButt;

/// Maximum payload size (in bytes) for UDP.
const UDP_MTU: usize = 65_507;

/// Interval between random gossip.
const GOSSIP_INTERVAL: Duration = Duration::from_secs(1);

/// Number of nodes picked for random gossip.
const GOSSIP_COUNT: usize = 3;

/// UDP ScuttleButt server.
pub struct ScuttleServer {
    channel: UnboundedSender<ChannelMessage>,
    scuttlebutt: Arc<Mutex<ScuttleButt>>,
    join_handle: JoinHandle<Result<(), anyhow::Error>>,
}

impl ScuttleServer {
    /// Launch a new server.
    ///
    /// This will start the ScuttleButt server as a new Tokio background task.
    pub fn spawn(
        address: impl Into<String>,
        seed_nodes: &[String],
        failure_detector_config: FailureDetectorConfig,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        let scuttlebutt = ScuttleButt::with_node_id_and_seeds(
            address.into(),
            seed_nodes.to_vec(),
            failure_detector_config,
        );
        let scuttlebutt_arc = Arc::new(Mutex::new(scuttlebutt));
        let scuttlebutt_server = scuttlebutt_arc.clone();

        let join_handle = tokio::spawn(async move {
            let mut server = UdpServer::new(rx, scuttlebutt_server).await?;
            server.run().await
        });

        Self {
            channel: tx,
            scuttlebutt: scuttlebutt_arc,
            join_handle,
        }
    }

    pub fn scuttlebutt(&self) -> Arc<Mutex<ScuttleButt>> {
        self.scuttlebutt.clone()
    }

    /// Call a function with mutable access to the [`ScuttleButt`].
    pub async fn with_scuttlebutt<F, T>(&self, mut fun: F) -> T
    where F: FnMut(&mut ScuttleButt) -> T {
        let mut scuttlebutt = self.scuttlebutt.lock().await;
        fun(&mut scuttlebutt)
    }

    /// Shut the server down.
    pub async fn shutdown(self) -> Result<(), anyhow::Error> {
        let _ = self.channel.send(ChannelMessage::Shutdown);
        self.join_handle.await?
    }

    /// Perform a ScuttleButt "handshake" with another UDP server.
    pub fn gossip(&self, addr: impl Into<String>) -> Result<(), anyhow::Error> {
        self.channel.send(ChannelMessage::Gossip(addr.into()))?;
        Ok(())
    }
}

/// UDP server for ScuttleButt communication.
struct UdpServer {
    channel: UnboundedReceiver<ChannelMessage>,
    scuttlebutt: Arc<Mutex<ScuttleButt>>,
    socket: Arc<UdpSocket>,
}

impl UdpServer {
    async fn new(
        channel: UnboundedReceiver<ChannelMessage>,
        scuttlebutt: Arc<Mutex<ScuttleButt>>,
    ) -> anyhow::Result<Self> {
        let socket = {
            let address = &scuttlebutt.lock().await.self_node_id;
            Arc::new(UdpSocket::bind(address).await?)
        };

        Ok(Self {
            scuttlebutt,
            channel,
            socket,
        })
    }

    /// Listen for new ScuttleButt messages.
    async fn run(&mut self) -> anyhow::Result<()> {
        let mut gossip_interval = time::interval(GOSSIP_INTERVAL);
        let mut rng = SmallRng::from_entropy();

        let mut buf = [0; UDP_MTU];
        loop {
            tokio::select! {
                result = self.socket.recv_from(&mut buf) => match result {
                    Ok((len, addr)) => {
                        let _ = self.process_package(addr, &buf[..len]).await;
                    }
                    Err(err) => return Err(err.into()),
                },
                _ = gossip_interval.tick() => self.gossip_multiple(&mut rng).await,
                message = self.channel.recv() => match message {
                    Some(ChannelMessage::Gossip(addr)) => {
                        let _ = self.gossip(addr).await;
                    },
                    Some(ChannelMessage::Shutdown) | None => break,
                },
            }
        }
        Ok(())
    }

    /// Process a single UDP packet.
    async fn process_package(&self, addr: SocketAddr, mut data: &[u8]) -> anyhow::Result<()> {
        // Handle gossip from other servers.
        let message = ScuttleButtMessage::deserialize(&mut data)?;
        let response = self.scuttlebutt.lock().await.process_message(message);

        // Send reply if necessary.
        if let Some(message) = response {
            let mut buf = Vec::new();
            message.serialize(&mut buf);
            self.socket.send_to(&buf[..], addr).await?;
        }

        Ok(())
    }

    /// Gossip to multiple randomly chosen nodes.
    async fn gossip_multiple(&self, rng: &mut SmallRng) {
        // TODO: Gossip with live nodes & randomly include dead node
        let scuttlebutt_guard = self.scuttlebutt.lock().await;
        let self_node_id = &scuttlebutt_guard.self_node_id;
        const EMPTY_STRING: String = String::new();
        let mut rand_nodes = [EMPTY_STRING; GOSSIP_COUNT];

        // Select up to [`GOSSIP_COUNT`] node IDs at random.
        let nodes = scuttlebutt_guard.cluster_state.nodes();
        let count = nodes
            .filter(|node_id| node_id != self_node_id)
            .map(ToString::to_string)
            .choose_multiple_fill(rng, &mut rand_nodes);

        // Drop lock to prevent deadlock in [`UdpSocket::gossip`].
        drop(scuttlebutt_guard);
        for node in &rand_nodes[..count] {
            let _ = self.gossip(node).await;
        }

        // Update nodes liveliness
        let mut scuttlebutt_guard = self.scuttlebutt.lock().await;
        scuttlebutt_guard.update_nodes_liveliness();
    }

    /// Gossip to one other UDP server.
    async fn gossip(&self, addr: impl Into<String>) -> anyhow::Result<()> {
        let syn = self.scuttlebutt.lock().await.create_syn_message();
        let mut buf = Vec::new();
        syn.serialize(&mut buf);
        let _ = self.socket.send_to(&buf, addr.into()).await?;
        Ok(())
    }
}

#[derive(Debug)]
enum ChannelMessage {
    Gossip(String),
    Shutdown,
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::time::Duration;

    use super::*;
    use crate::failure_detector::FailureDetectorConfig;
    use crate::message::ScuttleButtMessage;
    use crate::HEARTBEAT_KEY;

    async fn timeout<O>(future: impl Future<Output = O>) -> O {
        tokio::time::timeout(Duration::from_millis(100), future)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn syn() {
        let test_addr = "0.0.0.0:1111";
        let socket = UdpSocket::bind(test_addr).await.unwrap();

        let server = ScuttleServer::spawn("0.0.0.0:1112", &[], FailureDetectorConfig::default());
        server.gossip(test_addr).unwrap();

        let mut buf = [0; UDP_MTU];
        let (len, _addr) = timeout(socket.recv_from(&mut buf)).await.unwrap();

        let msg = ScuttleButtMessage::deserialize(&mut &buf[..len]).unwrap();
        match msg {
            ScuttleButtMessage::Syn { .. } => (),
            message => panic!("unexpected message: {:?}", message),
        }

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn syn_ack() {
        let server_addr = "0.0.0.0:2221";
        let socket = UdpSocket::bind("0.0.0.0:2222").await.unwrap();
        let mut scuttlebutt = ScuttleButt::with_node_id_and_seeds(
            "offline".into(),
            Vec::new(),
            FailureDetectorConfig::default(),
        );

        let server = ScuttleServer::spawn(server_addr, &[], FailureDetectorConfig::default());

        let mut buf = Vec::new();
        let syn = scuttlebutt.create_syn_message();
        syn.serialize(&mut buf);
        socket.send_to(&buf[..], server_addr).await.unwrap();

        let mut buf = [0; super::UDP_MTU];
        let (len, _addr) = timeout(socket.recv_from(&mut buf)).await.unwrap();

        let msg = ScuttleButtMessage::deserialize(&mut &buf[..len]).unwrap();
        match msg {
            ScuttleButtMessage::SynAck { .. } => (),
            message => panic!("unexpected message: {:?}", message),
        }

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn ignore_broken_payload() {
        let server_addr = "0.0.0.0:3331";
        let server = ScuttleServer::spawn(server_addr, &[], FailureDetectorConfig::default());
        let socket = UdpSocket::bind("0.0.0.0:3332").await.unwrap();
        let mut scuttlebutt = ScuttleButt::with_node_id_and_seeds(
            "offline".into(),
            Vec::new(),
            FailureDetectorConfig::default(),
        );

        // Send broken payload.
        socket.send_to(b"broken", server_addr).await.unwrap();

        // Confirm nothing broke using a regular payload.
        let syn = scuttlebutt.create_syn_message();
        let message = syn.serialize_to_vec();
        socket.send_to(&message, server_addr).await.unwrap();

        let mut buf = [0; UDP_MTU];
        let (len, _addr) = timeout(socket.recv_from(&mut buf)).await.unwrap();

        let msg = ScuttleButtMessage::deserialize(&mut &buf[..len]).unwrap();
        match msg {
            ScuttleButtMessage::SynAck { .. } => (),
            message => panic!("unexpected message: {:?}", message),
        }

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn ignore_oversized_payload() {
        let server_addr = "0.0.0.0:4441";
        let server = ScuttleServer::spawn(server_addr, &[], FailureDetectorConfig::default());
        let socket = UdpSocket::bind("0.0.0.0:4442").await.unwrap();
        let mut scuttlebutt = ScuttleButt::with_node_id_and_seeds(
            "offline".into(),
            Vec::new(),
            FailureDetectorConfig::default(),
        );

        // Send broken payload.
        socket.send_to(&[0; UDP_MTU], server_addr).await.unwrap();

        // Confirm nothing broke using a regular payload.
        let syn = scuttlebutt.create_syn_message();
        let buf = syn.serialize_to_vec();
        socket.send_to(&buf[..], server_addr).await.unwrap();

        let mut buf = [0; UDP_MTU];
        let (len, _addr) = timeout(socket.recv_from(&mut buf)).await.unwrap();

        match ScuttleButtMessage::deserialize(&mut &buf[..len]).unwrap() {
            ScuttleButtMessage::SynAck { .. } => (),
            message => panic!("unexpected message: {:?}", message),
        }

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn seeding() {
        let server_addr = "0.0.0.0:5551";
        let socket = UdpSocket::bind(server_addr).await.unwrap();

        let server = ScuttleServer::spawn(
            "0.0.0.0:5552",
            &[server_addr.into()],
            FailureDetectorConfig::default(),
        );

        let mut buf = [0; UDP_MTU];
        let (len, _addr) = timeout(socket.recv_from(&mut buf)).await.unwrap();

        match ScuttleButtMessage::deserialize(&mut &buf[..len]).unwrap() {
            ScuttleButtMessage::Syn { .. } => (),
            message => panic!("unexpected message: {:?}", message),
        }

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn heartbeat() {
        let test_addr = "0.0.0.0:6661";
        let mut scuttlebutt = ScuttleButt::with_node_id_and_seeds(
            test_addr.into(),
            Vec::new(),
            FailureDetectorConfig::default(),
        );
        let socket = UdpSocket::bind(test_addr).await.unwrap();

        let server_addr = "0.0.0.0:6662";
        let server = ScuttleServer::spawn(server_addr, &[], FailureDetectorConfig::default());

        // Add our test socket to the server's nodes.
        server
            .with_scuttlebutt(|server_scuttlebutt| {
                let syn = server_scuttlebutt.create_syn_message();
                let syn_ack = scuttlebutt.process_message(syn).unwrap();
                server_scuttlebutt.process_message(syn_ack);
            })
            .await;

        // Wait for heartbeat.
        let mut buf: Box<[u8]> = vec![0; UDP_MTU].into_boxed_slice();
        let (len, _addr) = timeout(socket.recv_from(&mut buf[..])).await.unwrap();

        let message = match ScuttleButtMessage::deserialize(&mut &buf[..len]).unwrap() {
            message @ ScuttleButtMessage::Syn { .. } => message,
            message => panic!("unexpected message: {:?}", message),
        };

        // Reply.
        let syn_ack = scuttlebutt.process_message(message).unwrap();
        let message = syn_ack.serialize_to_vec();
        socket.send_to(&message[..], server_addr).await.unwrap();

        // Wait for delta to ensure heartbeat key was incremented.
        let (len, _addr) = timeout(socket.recv_from(&mut buf)).await.unwrap();
        let delta = match ScuttleButtMessage::deserialize(&mut &buf[..len]).unwrap() {
            ScuttleButtMessage::Ack { delta } => delta,
            message => panic!("unexpected message: {:?}", message),
        };

        let node_delta = &delta.node_deltas.get(server_addr).unwrap().key_values;
        let heartbeat = &node_delta.get(HEARTBEAT_KEY).unwrap().value;
        assert_eq!(heartbeat, "2");

        server.shutdown().await.unwrap();
    }
}
