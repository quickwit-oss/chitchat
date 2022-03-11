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

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rand::prelude::*;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::error;

use crate::failure_detector::FailureDetectorConfig;
use crate::message::ScuttleButtMessage;
use crate::serialize::Serializable;
use crate::{NodeId, ScuttleButt};

/// Maximum payload size (in bytes) for UDP.
const UDP_MTU: usize = 65_507;

/// Interval between random gossip.
const GOSSIP_INTERVAL: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(1)
};

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
        node_id: NodeId,
        seed_nodes: &[String],
        address: impl Into<String>,
        failure_detector_config: FailureDetectorConfig,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        let scuttlebutt = ScuttleButt::with_node_id_and_seeds(
            node_id,
            seed_nodes.iter().cloned().collect(),
            address.into(),
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
            let bind_address = &scuttlebutt.lock().await.address;
            Arc::new(UdpSocket::bind(bind_address).await?)
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
        // Gossip with live nodes & probabilistically include a random dead node
        let scuttlebutt_guard = self.scuttlebutt.lock().await;
        let cluster_state = scuttlebutt_guard.cluster_state();

        let peer_nodes = cluster_state
            .nodes()
            .filter(|node_id| *node_id != scuttlebutt_guard.self_node_id())
            .map(|node_id| node_id.gossip_public_address.as_str())
            .collect::<HashSet<_>>();
        let live_nodes = scuttlebutt_guard
            .live_nodes()
            .map(|node_id| node_id.gossip_public_address.as_str())
            .collect::<HashSet<_>>();
        let dead_nodes = scuttlebutt_guard
            .dead_nodes()
            .map(|node_id| node_id.gossip_public_address.as_str())
            .collect::<HashSet<_>>();
        let seed_nodes = scuttlebutt_guard.seed_nodes().collect::<HashSet<_>>();
        let (selected_nodes, random_dead_node_opt, random_seed_node_opt) =
            select_nodes_for_gossip(rng, peer_nodes, live_nodes, dead_nodes, seed_nodes);

        // Drop lock to prevent deadlock in [`UdpSocket::gossip`].
        drop(scuttlebutt_guard);

        for node in selected_nodes {
            let result = self.gossip(&node).await;
            if result.is_err() {
                error!(node = ?node, "Gossip error with a live node.");
            }
        }

        if let Some(random_dead_node) = random_dead_node_opt {
            let result = self.gossip(&random_dead_node).await;
            if result.is_err() {
                error!(node = ?random_dead_node, "Gossip error with a dead node.")
            }
        }

        if let Some(random_seed_node) = random_seed_node_opt {
            let result = self.gossip(&random_seed_node).await;
            if result.is_err() {
                error!(node = ?random_seed_node, "Gossip error with a seed node.")
            }
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

fn select_nodes_for_gossip<R>(
    rng: &mut R,
    peer_nodes: HashSet<&str>,
    live_nodes: HashSet<&str>,
    dead_nodes: HashSet<&str>,
    seed_nodes: HashSet<&str>,
) -> (Vec<String>, Option<String>, Option<String>)
where
    R: Rng + ?Sized,
{
    let live_nodes_count = live_nodes.len();
    let dead_nodes_count = dead_nodes.len();
    const EMPTY_STRING: String = String::new();
    let mut rand_nodes = [EMPTY_STRING; GOSSIP_COUNT];

    // Select `GOSSIP_COUNT` number of live nodes.
    // On startup, select from cluster nodes since we don't know any live node yet.
    let count = if live_nodes_count == 0 {
        peer_nodes
            .iter()
            .map(ToString::to_string)
            .choose_multiple_fill(rng, &mut rand_nodes)
    } else {
        live_nodes
            .iter()
            .map(ToString::to_string)
            .choose_multiple_fill(rng, &mut rand_nodes)
    };
    let mut nodes = Vec::new();
    let mut has_gossiped_with_a_seed_node = false;
    for node_id in &rand_nodes[..count] {
        has_gossiped_with_a_seed_node =
            has_gossiped_with_a_seed_node || seed_nodes.contains(node_id.as_str());
        nodes.push((*node_id).clone());
    }

    // Select a dead node for potential gossip.
    let random_dead_node_opt =
        select_dead_node_to_gossip_with(rng, &dead_nodes, live_nodes_count, dead_nodes_count);

    // Select a seed node for potential gossip.
    // It prevents network partition caused by the number of seeds.
    // See https://issues.apache.org/jira/browse/CASSANDRA-150
    let random_seed_node_opt =
        if !has_gossiped_with_a_seed_node || live_nodes_count < seed_nodes.len() {
            select_seed_node_to_gossip_with(rng, &seed_nodes, live_nodes_count, dead_nodes_count)
        } else {
            None
        };

    (nodes, random_dead_node_opt, random_seed_node_opt)
}

/// Selects a dead node to gossip with, with some probability.
fn select_dead_node_to_gossip_with<R>(
    rng: &mut R,
    dead_nodes: &HashSet<&str>,
    live_nodes_count: usize,
    dead_nodes_count: usize,
) -> Option<String>
where
    R: Rng + ?Sized,
{
    let selection_probability = dead_nodes_count as f64 / (live_nodes_count + 1) as f64;
    if selection_probability > rng.gen::<f64>() {
        return dead_nodes.iter().choose(rng).map(ToString::to_string);
    }
    None
}

/// Selects a seed node to gossip with, with some probability.
fn select_seed_node_to_gossip_with<R>(
    rng: &mut R,
    seed_nodes: &HashSet<&str>,
    live_nodes_count: usize,
    dead_nodes_count: usize,
) -> Option<String>
where
    R: Rng + ?Sized,
{
    let random_seed_node = seed_nodes.iter().choose(rng).map(ToString::to_string);
    if live_nodes_count == 0 {
        return random_seed_node;
    }

    let selection_probability =
        seed_nodes.len() as f64 / (live_nodes_count + dead_nodes_count) as f64;
    if selection_probability >= rng.gen::<f64>() {
        return random_seed_node;
    }
    None
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::time::Duration;

    use tokio_stream::StreamExt;

    use super::*;
    use crate::failure_detector::FailureDetectorConfig;
    use crate::message::ScuttleButtMessage;
    use crate::HEARTBEAT_KEY;

    #[derive(Debug, Default)]
    struct RngForTest {
        value: u32,
    }

    impl RngCore for RngForTest {
        fn next_u32(&mut self) -> u32 {
            self.value += 1;
            self.value - 1
        }

        fn next_u64(&mut self) -> u64 {
            self.value += 1;
            (self.value - 1) as u64
        }

        fn fill_bytes(&mut self, _dest: &mut [u8]) {
            unimplemented!();
        }

        fn try_fill_bytes(&mut self, _dest: &mut [u8]) -> Result<(), rand::Error> {
            unimplemented!();
        }
    }

    fn to_hash_set<'a>(node_ids: &[&'a str]) -> std::collections::HashSet<&'a str> {
        node_ids.iter().copied().collect()
    }

    async fn timeout<O>(future: impl Future<Output = O>) -> O {
        tokio::time::timeout(Duration::from_millis(100), future)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn syn() {
        let test_addr = "0.0.0.0:1111";
        let socket = UdpSocket::bind(test_addr).await.unwrap();

        let server = ScuttleServer::spawn(
            "0.0.0.0:1112".into(),
            &[],
            "0.0.0.0:1112",
            FailureDetectorConfig::default(),
        );
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
            HashSet::new(),
            "offline".to_string(),
            FailureDetectorConfig::default(),
        );

        let server = ScuttleServer::spawn(
            server_addr.into(),
            &[],
            server_addr,
            FailureDetectorConfig::default(),
        );

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
        let server = ScuttleServer::spawn(
            server_addr.into(),
            &[],
            server_addr,
            FailureDetectorConfig::default(),
        );
        let socket = UdpSocket::bind("0.0.0.0:3332").await.unwrap();
        let mut scuttlebutt = ScuttleButt::with_node_id_and_seeds(
            "offline".into(),
            HashSet::new(),
            "offline".to_string(),
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
        let server = ScuttleServer::spawn(
            server_addr.into(),
            &[],
            server_addr,
            FailureDetectorConfig::default(),
        );
        let socket = UdpSocket::bind("0.0.0.0:4442").await.unwrap();
        let mut scuttlebutt = ScuttleButt::with_node_id_and_seeds(
            "offline".into(),
            HashSet::new(),
            "offline".to_string(),
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
            "0.0.0.0:5552".into(),
            &[server_addr.into()],
            "0.0.0.0:5552",
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
            HashSet::new(),
            test_addr.to_string(),
            FailureDetectorConfig::default(),
        );
        let socket = UdpSocket::bind(test_addr).await.unwrap();

        let server_addr = "0.0.0.0:6662";
        let server_node_id = NodeId::from(server_addr);
        let server = ScuttleServer::spawn(
            NodeId::from(server_addr),
            &[],
            server_addr,
            FailureDetectorConfig::default(),
        );

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

        let node_delta = &delta.node_deltas.get(&server_node_id).unwrap().key_values;
        let heartbeat = &node_delta.get(HEARTBEAT_KEY).unwrap().value;
        assert_eq!(heartbeat, "2");

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_member_change_event_is_broadcasted() {
        let node1 = ScuttleServer::spawn(
            NodeId::from("0.0.0.0:6663"),
            &[],
            "0.0.0.0:6663",
            FailureDetectorConfig::default(),
        );
        let node2 = ScuttleServer::spawn(
            NodeId::from("0.0.0.0:6664"),
            &["0.0.0.0:6663".to_string()],
            "0.0.0.0:6664",
            FailureDetectorConfig::default(),
        );
        let mut live_nodes_watcher = node1
            .scuttlebutt()
            .lock()
            .await
            .live_nodes_watcher()
            .skip_while(|live_nodes| live_nodes.is_empty());

        tokio::time::timeout(Duration::from_secs(3), async move {
            let live_nodes = live_nodes_watcher.next().await.unwrap();
            assert_eq!(live_nodes.len(), 1);
            assert!(live_nodes.contains(&NodeId::from("0.0.0.0:6664")));
        })
        .await
        .unwrap();

        node1.shutdown().await.unwrap();
        node2.shutdown().await.unwrap();
    }

    #[test]
    fn test_select_nodes_for_gossip() {
        {
            let mut rng = RngForTest::default();
            let (nodes, dead_node, seed_node) = select_nodes_for_gossip(
                &mut rng,
                to_hash_set(&["node-1", "node-2", "node-3"]),
                to_hash_set(&["node-1", "node-2"]),
                to_hash_set(&["node-3"]),
                to_hash_set(&["node-2"]),
            );
            assert_eq!(nodes.len(), 2);
            assert_eq!(dead_node, Some("node-3".to_string()));
            assert_eq!(
                seed_node, None,
                "Should have already gossiped with a seed node."
            );
        }

        {
            let mut rng = RngForTest::default();
            let (nodes, dead_node, seed_node) = select_nodes_for_gossip(
                &mut rng,
                to_hash_set(&["node-1", "node-2", "node-3", "node-4", "node-5"]),
                to_hash_set(&["node-1", "node-2", "node-3", "node-4", "node-5"]),
                to_hash_set(&[]),
                to_hash_set(&[]),
            );
            assert_eq!(nodes.len(), 3);
            assert_eq!(dead_node, None);
            assert_eq!(seed_node, None);
        }

        {
            let mut rng = RngForTest::default();
            let (nodes, dead_node, seed_node) = select_nodes_for_gossip(
                &mut rng,
                to_hash_set(&["node-1", "node-2", "node-3", "node-4", "node-5"]),
                to_hash_set(&["node-1"]),
                to_hash_set(&["node-2", "node-3", "node-4", "node-5"]),
                to_hash_set(&["node-4", "node-5"]),
            );
            assert_eq!(nodes, ["node-1".to_string()]);
            assert!(dead_node.is_some());
            assert!(seed_node.is_some());
        }
    }
}
