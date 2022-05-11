use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rand::prelude::*;
use tokio::net::{lookup_host, ToSocketAddrs, UdpSocket};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::message::ChitchatMessage;
use crate::serialize::Serializable;
use crate::{Chitchat, ChitchatConfig, NodeId};

/// Maximum payload size (in bytes) for UDP.
const UDP_MTU: usize = 65_507;

/// Number of nodes picked for random gossip.
const GOSSIP_COUNT: usize = 3;

/// UDP Chitchat server.
pub struct ChitchatServer {
    node_id: NodeId,
    command_tx: UnboundedSender<Command>,
    chitchat: Arc<Mutex<Chitchat>>,
    join_handle: JoinHandle<Result<(), anyhow::Error>>,
}

const DNS_POLLING_DURATION: Duration = Duration::from_secs(60);

async fn dns_refresh_loop(
    seed_hosts_requiring_dns: HashSet<String>,
    seed_addrs_not_requiring_resolution: HashSet<SocketAddr>,
    seed_addrs_tx: watch::Sender<HashSet<SocketAddr>>,
) {
    let mut interval = time::interval(DNS_POLLING_DURATION);
    // We actually do not want to run the polling loop right away,
    // hence this tick.
    interval.tick().await;
    while seed_addrs_tx.receiver_count() > 0 {
        interval.tick().await;
        let mut seed_addrs = seed_addrs_not_requiring_resolution.clone();
        for seed_host in &seed_hosts_requiring_dns {
            resolve_seed_host(seed_host, &mut seed_addrs).await;
        }
        if seed_addrs_tx.send(seed_addrs).is_err() {
            return;
        }
    }
}

async fn resolve_seed_host(seed_host: &str, seed_addrs: &mut HashSet<SocketAddr>) {
    if let Ok(resolved_seed_addrs) = lookup_host(seed_host).await {
        for seed_addr in resolved_seed_addrs {
            if seed_addrs.contains(&seed_addr) {
                continue;
            }
            debug!(seed_host=seed_host, seed_addr=%seed_addr, "seed-addr-from_dns");
            seed_addrs.insert(seed_addr);
        }
    } else {
        warn!(seed_host=%seed_host, "Failed to lookup host");
    }
}

// The seed nodes address can be string representing a
// socket addr directly, or hostnames:port.
//
// The latter is especially important when relying on
// a headless service in k8s or when using DNS in general.
//
// In that case, we do not want to perform the resolution
// once and forall.
// We want to periodically retry DNS resolution,
// in order to avoid having a split cluster.
//
// The newcomers are supposed to chime in too,
// so there is not need to refresh it too often,
// especially if it is not empty.
async fn spawn_dns_refresh_loop(seeds: &[String]) -> watch::Receiver<HashSet<SocketAddr>> {
    let mut seed_addrs_not_requiring_resolution: HashSet<SocketAddr> = Default::default();
    let mut first_round_seed_resolution: HashSet<SocketAddr> = Default::default();
    let mut seed_requiring_dns: HashSet<String> = Default::default();
    for seed in seeds {
        if let Ok(seed_addr) = seed.parse() {
            seed_addrs_not_requiring_resolution.insert(seed_addr);
        } else {
            seed_requiring_dns.insert(seed.clone());
            // We run DNS resolution for the first iteration too.
            // It will be run in the DNS polling loop too, but running
            // it for the first iteration makes sure our first gossip
            // round will not be for nothing.
            resolve_seed_host(seed, &mut first_round_seed_resolution).await;
        }
    }

    let initial_seed_addrs: HashSet<SocketAddr> = seed_addrs_not_requiring_resolution
        .union(&first_round_seed_resolution)
        .cloned()
        .collect();

    info!(initial_seed_addrs=?initial_seed_addrs);

    let (seed_addrs_tx, seed_addrs_rx) = watch::channel(initial_seed_addrs);
    if !seed_requiring_dns.is_empty() {
        tokio::task::spawn(dns_refresh_loop(
            seed_requiring_dns,
            seed_addrs_not_requiring_resolution,
            seed_addrs_tx,
        ));
    }
    seed_addrs_rx
}

impl ChitchatServer {
    /// Launch a new server.
    ///
    /// This will start the Chitchat server as a new Tokio background task.
    pub async fn spawn(config: ChitchatConfig, initial_key_values: Vec<(String, String)>) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let seed_addrs: watch::Receiver<HashSet<SocketAddr>> =
            spawn_dns_refresh_loop(&config.seed_nodes).await;

        let node_id = config.node_id.clone();

        let chitchat = Chitchat::with_node_id_and_seeds(config, seed_addrs, initial_key_values);
        let chitchat_arc = Arc::new(Mutex::new(chitchat));
        let chitchat_server = chitchat_arc.clone();

        let join_handle = tokio::spawn(async move {
            let mut server = UdpServer::new(command_rx, chitchat_server).await?;
            server.run().await
        });

        Self {
            node_id,
            command_tx,
            chitchat: chitchat_arc,
            join_handle,
        }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn chitchat(&self) -> Arc<Mutex<Chitchat>> {
        self.chitchat.clone()
    }

    /// Call a function with mutable access to the [`Chitchat`].
    pub async fn with_chitchat<F, T>(&self, mut fun: F) -> T
    where F: FnMut(&mut Chitchat) -> T {
        let mut chitchat = self.chitchat.lock().await;
        fun(&mut chitchat)
    }

    /// Shut the server down.
    pub async fn shutdown(self) -> Result<(), anyhow::Error> {
        let _ = self.command_tx.send(Command::Shutdown);
        self.join_handle.await?
    }

    /// Perform a Chitchat "handshake" with another UDP server.
    pub fn gossip(&self, addr: impl Into<String>) -> Result<(), anyhow::Error> {
        self.command_tx.send(Command::Gossip(addr.into()))?;
        Ok(())
    }
}

/// UDP server for Chitchat communication.
struct UdpServer {
    command_rx: UnboundedReceiver<Command>,
    chitchat: Arc<Mutex<Chitchat>>,
    socket: UdpSocket,
}

impl UdpServer {
    async fn new(
        command_rx: UnboundedReceiver<Command>,
        chitchat: Arc<Mutex<Chitchat>>,
    ) -> anyhow::Result<Self> {
        let bind_address = chitchat.lock().await.config.listen_addr;
        let socket = UdpSocket::bind(bind_address).await?;
        Ok(Self {
            chitchat,
            command_rx,
            socket,
        })
    }

    /// Listen for new Chitchat messages.
    async fn run(&mut self) -> anyhow::Result<()> {
        let gossip_interval = self.chitchat.lock().await.config.gossip_interval;
        let mut gossip_interval = time::interval(gossip_interval);
        let mut rng = SmallRng::from_rng(thread_rng()).expect("Failed to seed random generator");
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
                message = self.command_rx.recv() => match message {
                    Some(Command::Gossip(addr)) => {
                        let _ = self.gossip(addr).await;
                    },
                    Some(Command::Shutdown) | None => break,
                },
            }
        }
        Ok(())
    }

    /// Process a single UDP packet.
    async fn process_package(&self, addr: SocketAddr, mut data: &[u8]) -> anyhow::Result<()> {
        // Handle gossip from other servers.
        let message = ChitchatMessage::deserialize(&mut data)?;
        let response = self.chitchat.lock().await.process_message(message);

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
        let chitchat_guard = self.chitchat.lock().await;
        let cluster_state = chitchat_guard.cluster_state();

        let peer_nodes = cluster_state
            .nodes()
            .filter(|node_id| *node_id != chitchat_guard.self_node_id())
            .map(|node_id| node_id.gossip_public_address)
            .collect::<HashSet<_>>();
        let live_nodes = chitchat_guard
            .live_nodes()
            .map(|node_id| node_id.gossip_public_address)
            .collect::<HashSet<_>>();
        let dead_nodes = chitchat_guard
            .dead_nodes()
            .map(|node_id| node_id.gossip_public_address)
            .collect::<HashSet<_>>();
        let seed_nodes: HashSet<SocketAddr> = chitchat_guard.seed_nodes();
        let (selected_nodes, random_dead_node_opt, random_seed_node_opt) =
            select_nodes_for_gossip(rng, peer_nodes, live_nodes, dead_nodes, seed_nodes);

        // Drop lock to prevent deadlock in [`UdpSocket::gossip`].
        drop(chitchat_guard);

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
        let mut chitchat_guard = self.chitchat.lock().await;
        chitchat_guard.update_nodes_liveliness();
    }

    /// Gossip to one other UDP server.
    async fn gossip(&self, addr: impl ToSocketAddrs) -> anyhow::Result<()> {
        let syn = self.chitchat.lock().await.create_syn_message();
        let mut buf = Vec::new();
        syn.serialize(&mut buf);
        let _ = self.socket.send_to(&buf, addr).await?;
        Ok(())
    }
}

#[derive(Debug)]
enum Command {
    Gossip(String),
    Shutdown,
}

fn select_nodes_for_gossip<R>(
    rng: &mut R,
    peer_nodes: HashSet<SocketAddr>,
    live_nodes: HashSet<SocketAddr>,
    dead_nodes: HashSet<SocketAddr>,
    seed_nodes: HashSet<SocketAddr>,
) -> (Vec<SocketAddr>, Option<SocketAddr>, Option<SocketAddr>)
where
    R: Rng + ?Sized,
{
    let live_nodes_count = live_nodes.len();
    let dead_nodes_count = dead_nodes.len();

    // Select `GOSSIP_COUNT` number of live nodes.
    // On startup, select from cluster nodes since we don't know any live node yet.
    let nodes = if live_nodes_count == 0 {
        peer_nodes
    } else {
        live_nodes
    }
    .iter()
    .cloned()
    .choose_multiple(rng, GOSSIP_COUNT);

    let mut has_gossiped_with_a_seed_node = false;
    for node_id in &nodes {
        if seed_nodes.contains(node_id) {
            has_gossiped_with_a_seed_node = true;
            break;
        }
    }

    // Select a dead node for potential gossip.
    let random_dead_node_opt: Option<SocketAddr> =
        select_dead_node_to_gossip_with(rng, &dead_nodes, live_nodes_count, dead_nodes_count);

    // Select a seed node for potential gossip.
    // It prevents network partition caused by the number of seeds.
    // See https://issues.apache.org/jira/browse/CASSANDRA-150
    let random_seed_node_opt: Option<SocketAddr> =
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
    dead_nodes: &HashSet<SocketAddr>,
    live_nodes_count: usize,
    dead_nodes_count: usize,
) -> Option<SocketAddr>
where
    R: Rng + ?Sized,
{
    let selection_probability = dead_nodes_count as f64 / (live_nodes_count + 1) as f64;
    if selection_probability > rng.gen::<f64>() {
        return dead_nodes.iter().choose(rng).cloned();
    }
    None
}

/// Selects a seed node to gossip with, with some probability.
fn select_seed_node_to_gossip_with<R>(
    rng: &mut R,
    seed_nodes: &HashSet<SocketAddr>,
    live_nodes_count: usize,
    dead_nodes_count: usize,
) -> Option<SocketAddr>
where
    R: Rng + ?Sized,
{
    let selection_probability =
        seed_nodes.len() as f64 / (live_nodes_count + dead_nodes_count) as f64;
    if live_nodes_count == 0 || rng.gen::<f64>() <= selection_probability {
        return seed_nodes.iter().choose(rng).cloned();
    }
    None
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::time::Duration;

    use tokio_stream::StreamExt;

    use super::*;
    use crate::message::ChitchatMessage;
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

    fn to_hash_set<T: Eq + std::hash::Hash>(node_ids: Vec<T>) -> std::collections::HashSet<T> {
        node_ids.into_iter().collect()
    }

    async fn timeout<O>(future: impl Future<Output = O>) -> O {
        tokio::time::timeout(Duration::from_millis(100), future)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_syn() {
        let test_addr = "0.0.0.0:1111";
        let socket = UdpSocket::bind(test_addr).await.unwrap();
        let config = ChitchatConfig::for_test(1112);
        let server = ChitchatServer::spawn(config, Vec::new()).await;
        server.gossip(test_addr).unwrap();

        let mut buf = [0; UDP_MTU];
        let (len, _addr) = timeout(socket.recv_from(&mut buf)).await.unwrap();

        let msg = ChitchatMessage::deserialize(&mut &buf[..len]).unwrap();
        match msg {
            ChitchatMessage::Syn { cluster_id, digest } => {
                assert_eq!(cluster_id, "default-cluster");
                assert_eq!(digest.node_max_version.len(), 1);
            }
            message => panic!("unexpected message: {:?}", message),
        }

        server.shutdown().await.unwrap();
    }

    #[cfg(test)]
    fn empty_seeds() -> watch::Receiver<HashSet<SocketAddr>> {
        watch::channel(Default::default()).1
    }

    #[tokio::test]
    async fn test_syn_ack() {
        let node_2 = NodeId::for_test_localhost(2221);
        let config1 = ChitchatConfig::for_test(2222);
        let socket = UdpSocket::bind(config1.listen_addr).await.unwrap();
        let mut chitchat = Chitchat::with_node_id_and_seeds(
            ChitchatConfig::for_test(2222),
            empty_seeds(),
            Vec::new(),
        );
        let server = ChitchatServer::spawn(ChitchatConfig::for_test(2221), Vec::new()).await;

        let mut buf = Vec::new();
        let syn = chitchat.create_syn_message();
        syn.serialize(&mut buf);
        socket
            .send_to(&buf[..], node_2.gossip_public_address)
            .await
            .unwrap();

        let mut buf = [0; super::UDP_MTU];
        let (len, _addr) = timeout(socket.recv_from(&mut buf)).await.unwrap();

        let msg = ChitchatMessage::deserialize(&mut &buf[..len]).unwrap();
        match msg {
            ChitchatMessage::SynAck { .. } => (),
            message => panic!("unexpected message: {:?}", message),
        }

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_syn_bad_cluster() {
        let mut outsider_config = ChitchatConfig::for_test(2224);
        let socket = UdpSocket::bind(outsider_config.listen_addr).await.unwrap();
        outsider_config.cluster_id = "another-cluster".to_string();
        let mut outsider =
            Chitchat::with_node_id_and_seeds(outsider_config, empty_seeds(), Vec::new());

        let server_config = ChitchatConfig::for_test(2223);
        let server_addr = server_config.node_id.gossip_public_address;
        let server = ChitchatServer::spawn(server_config, Vec::new()).await;

        let mut buf = Vec::new();
        let syn = outsider.create_syn_message();
        syn.serialize(&mut buf);
        socket.send_to(&buf[..], server_addr).await.unwrap();

        let mut buf = [0; super::UDP_MTU];
        let (len, _addr) = timeout(socket.recv_from(&mut buf)).await.unwrap();

        let msg = ChitchatMessage::deserialize(&mut &buf[..len]).unwrap();
        match msg {
            ChitchatMessage::BadCluster => (),
            message => panic!("unexpected message: {:?}", message),
        }

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_ignore_broken_payload() {
        let server_config = ChitchatConfig::for_test(3331);
        let server_addr = server_config.node_id.gossip_public_address;
        let server = ChitchatServer::spawn(server_config, Vec::new()).await;
        let client_config = ChitchatConfig::for_test(3332);
        let socket = UdpSocket::bind(client_config.listen_addr).await.unwrap();
        let mut client_chitchat =
            Chitchat::with_node_id_and_seeds(client_config, empty_seeds(), Vec::new());
        // Send broken payload.
        socket.send_to(b"broken", server_addr).await.unwrap();

        // Confirm nothing broke using a regular payload.
        let syn = client_chitchat.create_syn_message();
        let message = syn.serialize_to_vec();
        socket.send_to(&message, server_addr).await.unwrap();

        let mut buf = [0; UDP_MTU];
        let (len, _addr) = timeout(socket.recv_from(&mut buf)).await.unwrap();

        let msg = ChitchatMessage::deserialize(&mut &buf[..len]).unwrap();
        match msg {
            ChitchatMessage::SynAck { .. } => (),
            message => panic!("unexpected message: {:?}", message),
        }

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_seeding() {
        let seed_config = ChitchatConfig::for_test(5551);
        let seed_addr = seed_config.node_id.gossip_public_address;
        let socket = UdpSocket::bind(seed_addr).await.unwrap();

        let mut client_config = ChitchatConfig::for_test(5552);
        client_config.seed_nodes = vec![seed_addr.to_string()];
        let client = ChitchatServer::spawn(client_config, Vec::new()).await;

        let mut buf = [0; UDP_MTU];
        let (len, _addr) = timeout(socket.recv_from(&mut buf)).await.unwrap();

        match ChitchatMessage::deserialize(&mut &buf[..len]).unwrap() {
            ChitchatMessage::Syn { .. } => (),
            message => panic!("unexpected message: {:?}", message),
        }

        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let test_config = ChitchatConfig::for_test(6661);
        let test_addr = test_config.listen_addr;
        let mut chitchat = Chitchat::with_node_id_and_seeds(test_config, empty_seeds(), Vec::new());
        let socket = UdpSocket::bind(test_addr).await.unwrap();

        let server_config = ChitchatConfig::for_test(6662);
        let server_id = server_config.node_id.clone();
        let server_addr = server_config.node_id.gossip_public_address;
        let server = ChitchatServer::spawn(server_config, Vec::new()).await;

        // Add our test socket to the server's nodes.
        server
            .with_chitchat(|server_chitchat| {
                let syn = server_chitchat.create_syn_message();
                let syn_ack = chitchat.process_message(syn).unwrap();
                server_chitchat.process_message(syn_ack);
            })
            .await;

        // Wait for heartbeat.
        let mut buf: Box<[u8]> = vec![0; UDP_MTU].into_boxed_slice();
        let (len, _addr) = timeout(socket.recv_from(&mut buf[..])).await.unwrap();

        let message = match ChitchatMessage::deserialize(&mut &buf[..len]).unwrap() {
            message @ ChitchatMessage::Syn { .. } => message,
            message => panic!("unexpected message: {:?}", message),
        };

        // Reply.
        let syn_ack = chitchat.process_message(message).unwrap();
        let message = syn_ack.serialize_to_vec();
        socket.send_to(&message[..], server_addr).await.unwrap();

        // Wait for delta to ensure heartbeat key was incremented.
        let (len, _addr) = timeout(socket.recv_from(&mut buf)).await.unwrap();
        let delta = match ChitchatMessage::deserialize(&mut &buf[..len]).unwrap() {
            ChitchatMessage::Ack { delta } => delta,
            message => panic!("unexpected message: {:?}", message),
        };

        let node_delta = &delta.node_deltas.get(&server_id).unwrap().key_values;
        let heartbeat = &node_delta.get(HEARTBEAT_KEY).unwrap().value;
        assert_eq!(heartbeat, "2");

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_member_change_event_is_broadcasted() {
        let node1_config = ChitchatConfig::for_test(6663);
        let node1_addr = node1_config.node_id.gossip_public_address;
        let node1 = ChitchatServer::spawn(node1_config, Vec::new()).await;

        let mut node2_config = ChitchatConfig::for_test(6664);
        node2_config.seed_nodes = vec![node1_addr.to_string()];
        let node2_id = node2_config.node_id.clone();
        let node2 = ChitchatServer::spawn(node2_config, Vec::new()).await;
        let mut live_nodes_watcher = node1
            .chitchat()
            .lock()
            .await
            .live_nodes_watcher()
            .skip_while(|live_nodes| live_nodes.is_empty());

        tokio::time::timeout(Duration::from_secs(3), async move {
            let live_nodes = live_nodes_watcher.next().await.unwrap();
            assert_eq!(live_nodes.len(), 1);
            assert!(live_nodes.contains(&node2_id));
        })
        .await
        .unwrap();

        node1.shutdown().await.unwrap();
        node2.shutdown().await.unwrap();
    }

    #[test]
    fn test_select_nodes_for_gossip() {
        let node1 = NodeId::for_test_localhost(10_001);
        let node2 = NodeId::for_test_localhost(10_002);
        let node3 = NodeId::for_test_localhost(10_003);
        let mut rng = RngForTest::default();
        let (nodes, dead_node, seed_node) = select_nodes_for_gossip(
            &mut rng,
            to_hash_set(vec![
                node1.gossip_public_address,
                node2.gossip_public_address,
                node3.gossip_public_address,
            ]),
            to_hash_set(vec![
                node1.gossip_public_address,
                node2.gossip_public_address,
            ]),
            to_hash_set(vec![node3.gossip_public_address]),
            to_hash_set(vec![node2.gossip_public_address]),
        );
        assert_eq!(nodes.len(), 2);
        assert_eq!(dead_node, Some(node3.gossip_public_address));
        assert_eq!(
            seed_node, None,
            "Should have already gossiped with a seed node."
        );
    }

    #[test]
    fn test_gossip_no_dead_node_no_seed_nodes() {
        let nodes: HashSet<SocketAddr> = (10_001..=10_005)
            .map(NodeId::for_test_localhost)
            .map(|node_id| node_id.gossip_public_address)
            .collect();
        let mut rng = RngForTest::default();
        let (nodes, dead_node, seed_node) = select_nodes_for_gossip(
            &mut rng,
            nodes.clone(),
            nodes,
            to_hash_set(vec![]),
            to_hash_set(vec![]),
        );
        assert_eq!(nodes.len(), 3);
        assert_eq!(dead_node, None);
        assert_eq!(seed_node, None);
    }

    #[test]
    fn test_gossip_dead_and_seed_node() {
        let nodes: Vec<SocketAddr> = (10_001..=10_005)
            .map(NodeId::for_test_localhost)
            .map(|node_id| node_id.gossip_public_address)
            .collect();
        let seeds: HashSet<SocketAddr> = nodes[3..5].iter().cloned().collect();
        let mut rng = RngForTest::default();
        let (gossip_nodes, gossip_dead_node, gossip_seed_node) = select_nodes_for_gossip(
            &mut rng,
            to_hash_set(nodes.clone()),
            to_hash_set(vec![nodes[0]]),
            nodes[1..].iter().cloned().collect(),
            seeds,
        );
        assert_eq!(gossip_nodes, &[nodes[0]]);
        assert!(gossip_dead_node.is_some());
        assert!(gossip_seed_node.is_some());
    }
}
