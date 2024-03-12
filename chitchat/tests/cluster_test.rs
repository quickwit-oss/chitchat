use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use anyhow::anyhow;
use chitchat::transport::ChannelTransport;
use chitchat::{
    spawn_chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, NodeState,
};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
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
    NodeStateAssert {
        server_chitchat_id: ChitchatId,
        chitchat_id: ChitchatId,
        predicate: NodeStatePredicate,
        timeout_opt: Option<Duration>,
    },
}

#[derive(Debug)]
enum NodeStatePredicate {
    EqualKeyValue(String, String), // key, value
    KeyPresent(String, bool),      // key, present
    Deleted(String, bool),         // key, marked
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
            NodeStatePredicate::Deleted(key, marked) => {
                debug!(key=%key, marked=marked, "assert-key-marked-for-deletion");
                node_state.get_versioned(key).unwrap().is_deleted() == *marked
            }
        }
    }
}

struct Simulator {
    transport: ChannelTransport,
    node_handles: HashMap<ChitchatId, ChitchatHandle>,
    gossip_interval: Duration,
    marked_for_deletion_key_grace_period: Duration,
}

impl Simulator {
    pub fn new(gossip_interval: Duration, marked_for_deletion_key_grace_period: Duration) -> Self {
        Self {
            transport: ChannelTransport::with_mtu(65_507),
            node_handles: HashMap::new(),
            gossip_interval,
            marked_for_deletion_key_grace_period,
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

/// Copy-pasted from Quickwit repo.
/// Finds a random available TCP port.
///
/// This function induces a race condition, use it only in unit tests.
pub fn find_available_tcp_port() -> anyhow::Result<u16> {
    let socket: SocketAddr = ([127, 0, 0, 1], 0u16).into();
    let listener = TcpListener::bind(socket)?;
    let port = listener.local_addr()?.port();
    Ok(port)
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
        // Check marked for deletion is not propagated to node 3.
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_2.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::Deleted("key_a".to_string(), false),
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
            predicate: NodeStatePredicate::Deleted("key_a".to_string(), true),
            timeout_opt: Some(TIMEOUT),
        },
        // Check marked for deletion is not propagated to node 3.
        Operation::NodeStateAssert {
            server_chitchat_id: chitchat_id_3.clone(),
            chitchat_id: chitchat_id_1.clone(),
            predicate: NodeStatePredicate::Deleted("key_a".to_string(), false),
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
    let mut rng = thread_rng();
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
            let value: u64 = rng.gen();
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
