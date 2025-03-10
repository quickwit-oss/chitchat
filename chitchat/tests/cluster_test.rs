use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, TcpListener};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyhow::anyhow;
use chitchat::transport::ChannelTransport;
use chitchat::{
    spawn_chitchat, Chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig,
    NodeState,
};
use itertools::Itertools;
use rand::prelude::IndexedRandom;
use rand::{rng, Rng};
use tracing::{debug, error, info};

#[derive(Debug)]
enum Operation {
    InsertKeysValues {
        chitchat_id: ChitchatId,
        keys_values: Vec<(String, String)>,
    },
    DeleteKey {
        chitchat_id: ChitchatId,
        key: String,
    },
    DeleteKeyAfterTtl {
        chitchat_id: ChitchatId,
        key: String,
    },
    AddNode {
        chitchat_id: ChitchatId,
        peer_seeds: Option<Vec<ChitchatId>>,
    },
    RemoveNetworkLink(ChitchatId, ChitchatId),
    AddNetworkLink(ChitchatId, ChitchatId),
    Wait(Duration),
    NodeStatusAssert {
        server_chitchat_id: ChitchatId,
        chitchat_id: ChitchatId,
        expected_status: NodeStatusPredicate,
        timeout_opt: Option<Duration>,
    },
    NodeStateAssert {
        server_chitchat_id: ChitchatId,
        chitchat_id: ChitchatId,
        predicate: NodeStatePredicate,
        timeout_opt: Option<Duration>,
    },
}

#[derive(Debug)]
enum NodeStatePredicate {
    EqualKeyValue(String, String),  // key, value
    KeyPresent(String, bool),       // key, present
    KeyMarkedDeleted(String, bool), // key, marked
}

impl NodeStatePredicate {
    fn check(&self, node_state: &NodeState) -> bool {
        match self {
            NodeStatePredicate::EqualKeyValue(key, expected_value) => {
                let versioned_value = node_state
                    .get_versioned(key)
                    .expect("Key is expected to be present");
                &versioned_value.value == expected_value
            }
            NodeStatePredicate::KeyPresent(key, present) => {
                debug!(key=%key, present=present, "assert-key-present");
                &node_state.get_versioned(key).is_some() == present
            }
            NodeStatePredicate::KeyMarkedDeleted(key, marked) => {
                debug!(key=%key, marked=marked, "assert-key-marked-for-deletion");
                node_state.get_versioned(key).unwrap().is_deleted() == *marked
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum NodeStatusPredicate {
    Live,
    Dead,
    Absent,
}

impl NodeStatusPredicate {
    fn check(&self, chitchat: &Chitchat, chitchat_id: &ChitchatId) -> bool {
        match self {
            NodeStatusPredicate::Live => {
                if chitchat.live_nodes().contains(chitchat_id) {
                    assert!(chitchat.node_state(chitchat_id).is_some());
                    return true;
                }
            }
            NodeStatusPredicate::Dead => {
                if chitchat.dead_nodes().contains(chitchat_id) {
                    assert!(chitchat.node_state(chitchat_id).is_some());
                    return true;
                }
            }
            NodeStatusPredicate::Absent => {
                if chitchat.node_state(chitchat_id).is_none() {
                    assert!(!chitchat.live_nodes().contains(chitchat_id));
                    assert!(!chitchat.dead_nodes().contains(chitchat_id));
                    return true;
                }
            }
        }
        false
    }
}

struct Simulator {
    transport: ChannelTransport,
    node_handles: HashMap<ChitchatId, ChitchatHandle>,
    gossip_interval: Duration,
    marked_for_deletion_key_grace_period: Duration,
    dead_node_grace_period: Duration,
}

impl Simulator {
    pub fn new(gossip_interval: Duration, marked_for_deletion_key_grace_period: Duration) -> Self {
        Self {
            transport: ChannelTransport::with_mtu(65_507),
            node_handles: HashMap::new(),
            gossip_interval,
            marked_for_deletion_key_grace_period,
            dead_node_grace_period: Duration::from_secs(3600),
        }
    }

    pub fn new_with_dead_node_grace_period(
        gossip_interval: Duration,
        marked_for_deletion_key_grace_period: Duration,
        dead_node_grace_period: Duration,
    ) -> Self {
        Self {
            transport: ChannelTransport::with_mtu(65_507),
            node_handles: HashMap::new(),
            gossip_interval,
            marked_for_deletion_key_grace_period,
            dead_node_grace_period,
        }
    }

    pub async fn execute(&mut self, operations: Vec<Operation>) {
        for operation in operations {
            info!("Execute operation {operation:?}");
            match operation {
                Operation::AddNode {
                    chitchat_id,
                    peer_seeds,
                } => {
                    self.spawn_node(chitchat_id, peer_seeds).await;
                }
                Operation::InsertKeysValues {
                    chitchat_id,
                    keys_values,
                } => {
                    self.insert_keys_values(chitchat_id, keys_values).await;
                }
                Operation::DeleteKey { chitchat_id, key } => {
                    self.delete(chitchat_id, &key).await;
                }
                Operation::DeleteKeyAfterTtl { chitchat_id, key } => {
                    self.delete_after_ttl(chitchat_id, &key).await;
                }
                Operation::Wait(duration) => {
                    tokio::time::sleep(duration).await;
                }
                Operation::RemoveNetworkLink(node_1, node_2) => {
                    debug!(node_l=%node_1.node_id, node_r=%node_2.node_id, "remove-link");
                    self.transport
                        .remove_link(node_1.gossip_advertise_addr, node_2.gossip_advertise_addr)
                        .await;
                }
                Operation::AddNetworkLink(node_1, node_2) => {
                    debug!(node_l=%node_1.node_id, node_r=%node_2.node_id, "add-link");
                    self.transport
                        .add_link(node_1.gossip_advertise_addr, node_2.gossip_advertise_addr)
                        .await;
                }
                Operation::NodeStatusAssert {
                    server_chitchat_id,
                    chitchat_id,
                    expected_status,
                    timeout_opt,
                } => {
                    debug!(server_node_id=%server_chitchat_id.node_id, node_id=%chitchat_id.node_id, "node-presence-assert");
                    let chitchat = self
                        .node_handles
                        .get(&server_chitchat_id)
                        .unwrap()
                        .chitchat();
                    if let Some(timeout) = timeout_opt {
                        let chitchat_id_clone = chitchat_id.clone();
                        tokio::time::timeout(timeout, async move {
                            let mut wait_printed = false;
                            loop {
                                let chitchat_guard = chitchat.lock().await;
                                if expected_status.check(&chitchat_guard, &chitchat_id_clone) {
                                    break;
                                }
                                if !wait_printed {
                                    info!(node_id=%chitchat_id_clone.node_id, state_snapshot=?chitchat_guard.state_snapshot(), "Waiting for node status to be {:?}", expected_status);
                                    wait_printed = true;
                                }
                                drop(chitchat_guard);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }).await.map_err(|_| {
                            anyhow!("Node {:?} assert timeout on chitchat_id={}", expected_status, chitchat_id.node_id)
                        }).unwrap();
                    } else {
                        let chitchat_guard = chitchat.lock().await;
                        assert!(
                            expected_status.check(&chitchat_guard, &chitchat_id),
                            "Node {:?} assertion failed",
                            expected_status,
                        );
                    }
                }
                Operation::NodeStateAssert {
                    server_chitchat_id,
                    chitchat_id,
                    predicate,
                    timeout_opt,
                } => {
                    debug!(server_node_id=%server_chitchat_id.node_id, node_id=%chitchat_id.node_id, "node-state-assert");
                    let chitchat = self
                        .node_handles
                        .get(&server_chitchat_id)
                        .unwrap()
                        .chitchat();
                    // Wait for node_state & predicate.
                    if let Some(timeout) = timeout_opt {
                        let chitchat_clone = chitchat.clone();
                        let chitchat_id_clone = chitchat_id.clone();
                        tokio::time::timeout(timeout, async move {
                            loop {
                                let chitchat_guard = chitchat_clone.lock().await;
                                if let Some(node_state) = chitchat_guard.node_state(&chitchat_id_clone) {
                                    if predicate.check(node_state) {
                                        break;
                                    } else {
                                        info!(node_id=%chitchat_id_clone.node_id, "Waiting for predicate to be true.");
                                    }
                                } else {
                                    info!(node_id=%chitchat_id_clone.node_id, state_snapshot=?chitchat_guard.state_snapshot(), "Waiting for node state to be present.");
                                }
                                drop(chitchat_guard);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }).await.map_err(|_| {
                            anyhow!("Predicate timeout on chitchat_id={}", chitchat_id.node_id)
                        }).unwrap();
                    } else {
                        let chitchat_guard = chitchat.lock().await;
                        if let Some(node_state) = chitchat_guard.node_state(&chitchat_id) {
                            let predicate_value = predicate.check(node_state);
                            if !predicate_value {
                                info!(node_id=%chitchat_id.node_id, predicate=?predicate, state_snapshot=?chitchat_guard.state_snapshot(), "Predicate false.");
                            }
                            assert!(predicate_value);
                        } else {
                            error!(node_id=%chitchat_id.node_id, state_snapshot=?chitchat_guard.state_snapshot(), "Node state missing.");
                            panic!("Node state missing");
                        }
                    }
                }
            }
        }
    }

    pub async fn insert_keys_values(
        &mut self,
        chitchat_id: ChitchatId,
        keys_values: Vec<(String, String)>,
    ) {
        debug!(node_id=%chitchat_id.node_id, num_keys_values=?keys_values.len(), "insert-keys-values");
        let chitchat = self.node_handles.get(&chitchat_id).unwrap().chitchat();
        let mut chitchat_guard = chitchat.lock().await;
        for (key, value) in keys_values.into_iter() {
            chitchat_guard.self_node_state().set(key.clone(), value);
        }
    }

    pub async fn delete(&mut self, chitchat_id: ChitchatId, key: &str) {
        let chitchat = self.node_handles.get(&chitchat_id).unwrap().chitchat();
        let mut chitchat_guard = chitchat.lock().await;
        chitchat_guard.self_node_state().delete(key);
        let hearbeat = chitchat_guard.self_node_state().heartbeat();
        debug!(node_id=%chitchat_id.node_id, key=%key, hearbeat=?hearbeat, "Deleted key.");
    }

    pub async fn delete_after_ttl(&mut self, chitchat_id: ChitchatId, key: &str) {
        let chitchat = self.node_handles.get(&chitchat_id).unwrap().chitchat();
        let mut chitchat_guard = chitchat.lock().await;
        chitchat_guard.self_node_state().delete_after_ttl(key);
        let hearbeat = chitchat_guard.self_node_state().heartbeat();
        debug!(node_id=%chitchat_id.node_id, key=%key, hearbeat=?hearbeat, "Scheduled key for deletion after grace period");
    }

    pub async fn spawn_node(
        &mut self,
        chitchat_id: ChitchatId,
        peer_seeds: Option<Vec<ChitchatId>>,
    ) {
        debug!(node_id=%chitchat_id.node_id, "spawn");
        let seed_nodes: Vec<_> = peer_seeds
            .unwrap_or_else(|| {
                self.node_handles
                    .keys()
                    .cloned()
                    .collect::<Vec<ChitchatId>>()
            })
            .iter()
            .map(|chitchat_id| chitchat_id.gossip_advertise_addr.to_string())
            .collect();
        let config = ChitchatConfig {
            chitchat_id: chitchat_id.clone(),
            cluster_id: "default-cluster".to_string(),
            gossip_interval: self.gossip_interval,
            listen_addr: chitchat_id.gossip_advertise_addr,
            seed_nodes,
            failure_detector_config: FailureDetectorConfig {
                initial_interval: self.gossip_interval * 10,
                dead_node_grace_period: self.dead_node_grace_period,
                ..Default::default()
            },
            marked_for_deletion_grace_period: self.marked_for_deletion_key_grace_period,
            catchup_callback: None,
            extra_liveness_predicate: None,
        };
        let handle = spawn_chitchat(config, Vec::new(), &self.transport)
            .await
            .unwrap();
        self.node_handles.insert(chitchat_id, handle);
    }
}

pub fn create_chitchat_id(id: &str) -> ChitchatId {
    let port = find_available_tcp_port().unwrap();
    ChitchatId {
        node_id: id.to_string(),
        generation_id: 0,
        gossip_advertise_addr: ([127, 0, 0, 1], port).into(),
    }
}

pub fn test_chitchat_id(port: u16) -> ChitchatId {
    ChitchatId {
        node_id: format!("node_{port}"),
        generation_id: 0,
        gossip_advertise_addr: ([127, 0, 0, 1], port).into(),
    }
}

/// Finds a random available TCP port.
///
/// We keep a process-wide memory of used ports, otherwise multiple calls to
/// this function might return the same port, introducing an annoying race
/// condition. This works as long as this function is kept private and all tests
/// in this file are executed in the same process (this is the case with `cargo
/// test` but not `nextest`).
fn find_available_tcp_port() -> anyhow::Result<u16> {
    static USED_PORTS: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();
    let socket: SocketAddr = ([127, 0, 0, 1], 0u16).into();
    let listener = TcpListener::bind(socket)?;
    let port = listener.local_addr()?.port();
    {
        let mut used_ports_guard = USED_PORTS
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .unwrap();
        if !used_ports_guard.contains(&port) {
            used_ports_guard.insert(port);
            return Ok(port);
        }
    }
    info!(port=%port, "port already in use, look for another one");
    find_available_tcp_port()
}

#[tokio::test]
async fn test_simple_simulation_insert() {
    // let _ = tracing_subscriber::fmt::try_init();
    let mut simulator = Simulator::new(Duration::from_millis(100), Duration::from_secs(1));
    let chitchat_id_1 = create_chitchat_id("node-1");
    let chitchat_id_2 = create_chitchat_id("node-2");
    let operations = vec![
        Operation::AddNode {
            chitchat_id: chitchat_id_1.clone(),
            peer_seeds: None,
        },
        Operation::AddNode {
            chitchat_id: chitchat_id_2.clone(),
            peer_seeds: None,
        },
        Operation::InsertKeysValues {
            chitchat_id: chitchat_id_1.clone(),
            keys_values: vec![("key_a".to_string(), "0".to_string())],
        },
        Operation::InsertKeysValues {
            chitchat_id: chitchat_id_2.clone(),
            keys_values: vec![("key_b".to_string(), "1".to_string())],
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::EqualKeyValue("key_a".to_string(), "0".to_string()),
            timeout_opt: Some(Duration::from_millis(200)),
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_1.clone(),
            chitchat_id: chitchat_id_2.clone(),
            predicate: NodeStatePredicate::EqualKeyValue("key_b".to_string(), "1".to_string()),
            timeout_opt: None,
        },
    ];
    simulator.execute(operations).await;
}

#[tokio::test]
async fn test_simple_simulation_delete_after_ttl() {
    let mut simulator = Simulator::new(Duration::from_millis(100), Duration::from_secs(1));
    let chitchat_id_1 = create_chitchat_id("node-1");
    let chitchat_id_2 = create_chitchat_id("node-2");
    let operations = vec![
        Operation::AddNode {
            chitchat_id: chitchat_id_1.clone(),
            peer_seeds: None,
        },
        Operation::AddNode {
            chitchat_id: chitchat_id_2.clone(),
            peer_seeds: None,
        },
        Operation::InsertKeysValues {
            chitchat_id: chitchat_id_1.clone(),
            keys_values: vec![("key_a".to_string(), "0".to_string())],
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::EqualKeyValue("key_a".to_string(), "0".to_string()),
            timeout_opt: Some(Duration::from_millis(200)),
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_1.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::EqualKeyValue("key_a".to_string(), "0".to_string()),
            timeout_opt: None,
        },
        Operation::DeleteKeyAfterTtl {
            chitchat_id: chitchat_id_1.clone(),
            key: "key_a".to_string(),
        },
        Operation::Wait(Duration::from_millis(300)),
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::EqualKeyValue("key_a".to_string(), "0".to_string()),
            timeout_opt: None,
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_1.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::EqualKeyValue("key_a".to_string(), "0".to_string()),
            timeout_opt: None,
        },
        Operation::Wait(Duration::from_secs(3)),
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_1.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), false),
            timeout_opt: None,
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), false),
            timeout_opt: None,
        },
    ];
    simulator.execute(operations).await;
}

#[tokio::test]
async fn test_simple_simulation_with_network_partition() {
    // let _ = tracing_subscriber::fmt::try_init();
    let mut simulator = Simulator::new(Duration::from_millis(100), Duration::from_secs(1));
    let chitchat_id_1 = create_chitchat_id("node-1");
    let chitchat_id_2 = create_chitchat_id("node-2");
    let operations = vec![
        Operation::AddNode {
            chitchat_id: chitchat_id_1.clone(),
            peer_seeds: None,
        },
        Operation::AddNode {
            chitchat_id: chitchat_id_2.clone(),
            peer_seeds: None,
        },
        Operation::InsertKeysValues {
            chitchat_id: chitchat_id_1.clone(),
            keys_values: vec![("key_a".to_string(), "0".to_string())],
        },
        // Wait propagation of states.
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::EqualKeyValue("key_a".to_string(), "0".to_string()),
            timeout_opt: Some(Duration::from_millis(500)),
        },
        Operation::RemoveNetworkLink(chitchat_id_1.clone(), chitchat_id_2.clone()),
        Operation::InsertKeysValues {
            chitchat_id: chitchat_id_2.clone(),
            keys_values: vec![("key_b".to_string(), "1".to_string())],
        },
        // Wait propagation of states.
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_1.clone(),
            chitchat_id: chitchat_id_2.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_b".to_string(), false),
            timeout_opt: Some(Duration::from_millis(500)),
        },
    ];
    simulator.execute(operations).await;
}

#[tokio::test]
async fn test_marked_for_deletion_gc_with_network_partition_2_nodes() {
    // let _ = tracing_subscriber::fmt::try_init();
    const TIMEOUT: Duration = Duration::from_millis(500);
    let mut simulator = Simulator::new(Duration::from_millis(100), Duration::from_secs(1));
    let chitchat_id_1 = test_chitchat_id(1);
    let chitchat_id_2 = test_chitchat_id(2);
    let peer_seeds = vec![chitchat_id_1.clone(), chitchat_id_2.clone()];
    let operations = vec![
        Operation::AddNode {
            chitchat_id: chitchat_id_1.clone(),
            peer_seeds: Some(peer_seeds.clone()),
        },
        Operation::AddNode {
            chitchat_id: chitchat_id_2.clone(),
            peer_seeds: Some(peer_seeds.clone()),
        },
        Operation::InsertKeysValues {
            chitchat_id: chitchat_id_1.clone(),
            keys_values: vec![("key_a".to_string(), "0".to_string())],
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), true),
            timeout_opt: Some(TIMEOUT),
        },
        // Isolate node 2.
        Operation::RemoveNetworkLink(chitchat_id_1.clone(), chitchat_id_2.clone()),
        Operation::Wait(Duration::from_secs(5)),
        // Mark for deletion key.
        Operation::DeleteKey {
            chitchat_id: chitchat_id_1.clone(),
            key: "key_a".to_string(),
        },
        // Check marked for deletion is not propagated to node 2.
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyMarkedDeleted("key_a".to_string(), false),
            timeout_opt: None,
        },
        // Wait for garbage collection: grace period * heartbeat ~ 1 second + margin of 1 second.
        Operation::Wait(Duration::from_secs(2)),
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_1.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), false),
            timeout_opt: None,
        },
        Operation::Wait(TIMEOUT),
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), true),
            timeout_opt: None,
        },
        // Relink node 2
        Operation::AddNetworkLink(chitchat_id_1.clone(), chitchat_id_2.clone()),
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            // The key should be deleted.
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), false),
            timeout_opt: Some(TIMEOUT),
        },
    ];
    simulator.execute(operations).await;
}

#[tokio::test]
async fn test_marked_for_deletion_gc_with_network_partition_4_nodes() {
    let _ = tracing_subscriber::fmt::try_init();
    const TIMEOUT: Duration = Duration::from_millis(500);
    let mut simulator = Simulator::new(Duration::from_millis(100), Duration::from_secs(1));
    let chitchat_id_1 = test_chitchat_id(1);
    let chitchat_id_2 = test_chitchat_id(2);
    let chitchat_id_3 = test_chitchat_id(3);
    let chitchat_id_4 = test_chitchat_id(4);
    let peer_seeds = vec![
        chitchat_id_1.clone(),
        chitchat_id_2.clone(),
        chitchat_id_3.clone(),
    ];
    let operations = vec![
        Operation::AddNode {
            chitchat_id: chitchat_id_1.clone(),
            peer_seeds: Some(peer_seeds.clone()),
        },
        Operation::AddNode {
            chitchat_id: chitchat_id_2.clone(),
            peer_seeds: Some(peer_seeds.clone()),
        },
        Operation::AddNode {
            chitchat_id: chitchat_id_3.clone(),
            peer_seeds: Some(peer_seeds.clone()),
        },
        Operation::InsertKeysValues {
            chitchat_id: chitchat_id_1.clone(),
            keys_values: vec![("key_a".to_string(), "0".to_string())],
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), true),
            timeout_opt: Some(TIMEOUT),
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_3.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), true),
            timeout_opt: Some(TIMEOUT),
        },
        // Isolate node 3.
        Operation::RemoveNetworkLink(chitchat_id_1.clone(), chitchat_id_3.clone()),
        Operation::RemoveNetworkLink(chitchat_id_2.clone(), chitchat_id_3.clone()),
        // Mark for deletion key.
        Operation::DeleteKey {
            chitchat_id: chitchat_id_1.clone(),
            key: "key_a".to_string(),
        },
        // Check marked for deletion is propagated to node 2.
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyMarkedDeleted("key_a".to_string(), true),
            timeout_opt: Some(TIMEOUT),
        },
        // Check marked for deletion is not propagated to node 3.
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_3.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyMarkedDeleted("key_a".to_string(), false),
            timeout_opt: None,
        },
        // Wait for garbage collection: grace period + margin of 1 second.
        Operation::Wait(Duration::from_secs(2)),
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), false),
            timeout_opt: Some(TIMEOUT),
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_1.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), false),
            timeout_opt: None,
        },
        // Add node 4 which communicates only with node 3.
        Operation::RemoveNetworkLink(chitchat_id_1.clone(), chitchat_id_4.clone()),
        Operation::RemoveNetworkLink(chitchat_id_2.clone(), chitchat_id_4.clone()),
        Operation::AddNode {
            chitchat_id: chitchat_id_4.clone(),
            peer_seeds: Some(vec![chitchat_id_3.clone()]),
        },
        // Wait for propagation
        // We need to wait longer... because node 4 is just starting?
        Operation::Wait(TIMEOUT),
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_3.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), true),
            timeout_opt: None,
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_4.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), true),
            timeout_opt: Some(TIMEOUT),
        },
        // Relink node 3
        Operation::AddNetworkLink(chitchat_id_1.clone(), chitchat_id_3.clone()),
        Operation::AddNetworkLink(chitchat_id_2.clone(), chitchat_id_3.clone()),
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_3.clone(),
            chitchat_id: chitchat_id_1.clone(),
            // The key should be deleted.
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), false),
            timeout_opt: Some(TIMEOUT),
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_4.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), false),
            timeout_opt: Some(TIMEOUT * 10),
        },
    ];
    simulator.execute(operations).await;
}

// Playground.
// This is a stress test. If you want to debug it, reduce first the number
// of nodes to 3 and keys to 1 or 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_simple_simulation_heavy_insert_delete() {
    // let _ = tracing_subscriber::fmt::try_init();
    let mut rng = rng();
    let mut simulator = Simulator::new(Duration::from_millis(100), Duration::from_secs(5));
    let mut chitchat_ids = Vec::new();
    for i in 0..20 {
        chitchat_ids.push(create_chitchat_id(&format!("node-{}", i)));
    }
    let seeds = vec![
        chitchat_ids[0].clone(),
        chitchat_ids[1].clone(),
        chitchat_ids[2].clone(),
    ];

    let add_node_operations: Vec<_> = chitchat_ids
        .iter()
        .map(|chitchat_id| Operation::AddNode {
            chitchat_id: chitchat_id.clone(),
            peer_seeds: Some(seeds.clone()),
        })
        .collect();
    simulator.execute(add_node_operations).await;

    let key_names: Vec<_> = (0..200).map(|idx| format!("key_{}", idx)).collect();
    let mut keys_values_inserted_per_chitchat_id: HashMap<ChitchatId, HashSet<String>> =
        HashMap::new();
    for chitchat_id in chitchat_ids.iter() {
        let mut keys_values = Vec::new();
        for key in key_names.iter() {
            let value: u64 = rng.random();
            keys_values.push((key.to_string(), value.to_string()));
            let keys_entry = keys_values_inserted_per_chitchat_id
                .entry(chitchat_id.clone())
                .or_default();
            keys_entry.insert(key.to_string());
        }
        simulator
            .execute(vec![Operation::InsertKeysValues {
                chitchat_id: chitchat_id.clone(),
                keys_values,
            }])
            .await;
    }

    tokio::time::sleep(Duration::from_secs(10)).await;
    info!("Checking keys are present...");
    for (chitchat_id, keys) in keys_values_inserted_per_chitchat_id.clone().into_iter() {
        debug!(node_id=%chitchat_id.node_id, keys=?keys, "check");
        for key in keys {
            let server_chitchat_id = chitchat_ids.choose(&mut rng).unwrap().clone();
            let check_operation = Operation::NodeStateAssert {
                server_chitchat_id,
                chitchat_id: chitchat_id.clone(),
                predicate: NodeStatePredicate::KeyPresent(key.to_string(), true),
                timeout_opt: None,
            };
            simulator.execute(vec![check_operation]).await;
        }
    }

    // Marked all keys for deletion.
    for (chitchat_id, keys) in keys_values_inserted_per_chitchat_id.clone().into_iter() {
        for key in keys {
            let check_operation = Operation::DeleteKey {
                chitchat_id: chitchat_id.clone(),
                key,
            };
            simulator.execute(vec![check_operation]).await;
        }
    }

    // Wait for garbage collection to kick in.
    // Time to wait is 10s = grace_period(5s) + margin of 5s
    tokio::time::sleep(Duration::from_secs(10)).await;
    info!("Checking keys are deleted...");
    for (chitchat_id, keys) in keys_values_inserted_per_chitchat_id.clone().into_iter() {
        for key in keys {
            let server_chitchat_id = chitchat_ids.choose(&mut rng).unwrap().clone();
            let check_operation = Operation::NodeStateAssert {
                server_chitchat_id,
                chitchat_id: chitchat_id.clone(),
                predicate: NodeStatePredicate::KeyPresent(key.to_string(), false),
                timeout_opt: None,
            };
            simulator.execute(vec![check_operation]).await;
        }
    }
}

#[tokio::test]
async fn test_bouncing_deletion_status() {
    let _ = tracing_subscriber::fmt::try_init();
    const PROPAGATION_TIMEOUT: Duration = Duration::from_millis(500);
    const NODE_DELETION_GRACE_PERIOD: Duration = Duration::from_secs(4);
    let mut simulator = Simulator::new_with_dead_node_grace_period(
        Duration::from_millis(100),
        Duration::from_secs(1),
        NODE_DELETION_GRACE_PERIOD,
    );
    let chitchat_id_1 = test_chitchat_id(1);
    let chitchat_id_2 = test_chitchat_id(2);
    let late_joiner_chitchat_ids = (3..10).map(test_chitchat_id).collect::<Vec<_>>();
    let peer_seeds = vec![chitchat_id_1.clone()];
    let mut operations = vec![
        Operation::AddNode {
            chitchat_id: chitchat_id_1.clone(),
            peer_seeds: Some(peer_seeds.clone()),
        },
        Operation::AddNode {
            chitchat_id: chitchat_id_2.clone(),
            peer_seeds: Some(peer_seeds.clone()),
        },
        Operation::NodeStatusAssert {
            server_chitchat_id: chitchat_id_1.clone(),
            chitchat_id: chitchat_id_2.clone(),
            expected_status: NodeStatusPredicate::Live,
            timeout_opt: Some(PROPAGATION_TIMEOUT),
        },
        Operation::InsertKeysValues {
            chitchat_id: chitchat_id_2.clone(),
            keys_values: vec![("key_a".to_string(), "0".to_string())],
        },
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_1.clone(),
            chitchat_id: chitchat_id_2.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), true),
            timeout_opt: Some(PROPAGATION_TIMEOUT),
        },
        Operation::RemoveNetworkLink(chitchat_id_1.clone(), chitchat_id_2.clone()),
    ];
    // Isolate node 2 from all nodes, including future ones
    // Equivalent to deleting it from the perspective of the cluster
    for late_joiner_chitchat_id in &late_joiner_chitchat_ids {
        operations.push(Operation::RemoveNetworkLink(
            late_joiner_chitchat_id.clone(),
            chitchat_id_2.clone(),
        ));
    }
    operations.push(Operation::NodeStatusAssert {
        server_chitchat_id: chitchat_id_1.clone(),
        chitchat_id: chitchat_id_2.clone(),
        expected_status: NodeStatusPredicate::Dead,
        timeout_opt: Some(Duration::from_secs(5)),
    });
    // Nodes are joining at different times, which means they will record
    // different times of death for node 2. This means that after node 1
    // definitively removes node 2 (garbage collect), it will receive it
    // back from the late joiners.
    for late_joiner_chitchat_id in &late_joiner_chitchat_ids {
        operations.push(Operation::Wait(Duration::from_secs(1)));
        operations.push(Operation::AddNode {
            chitchat_id: late_joiner_chitchat_id.clone(),
            peer_seeds: Some(peer_seeds.clone()),
        });
    }
    operations.append(&mut vec![
        // We receive the state of node 2 from late joiners,
        // but we don't record it.
        Operation::NodeStatusAssert {
            server_chitchat_id: chitchat_id_1.clone(),
            chitchat_id: chitchat_id_2.clone(),
            expected_status: NodeStatusPredicate::Absent,
            timeout_opt: None,
        },
        // Now if node 2 really comes back, we want node 1 to eventually record it
        Operation::AddNetworkLink(chitchat_id_1.clone(), chitchat_id_2.clone()),
        Operation::NodeStatusAssert {
            server_chitchat_id: chitchat_id_1.clone(),
            chitchat_id: chitchat_id_2.clone(),
            expected_status: NodeStatusPredicate::Live,
            timeout_opt: Some(PROPAGATION_TIMEOUT),
        },
    ]);

    simulator.execute(operations).await;
}
