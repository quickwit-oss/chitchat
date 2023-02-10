use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use anyhow::anyhow;
use chitchat::transport::ChannelTransport;
use chitchat::{
    spawn_chitchat, ChitchatConfig, ChitchatHandle, FailureDetectorConfig, NodeId, NodeState,
};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use tracing::{debug, info};

enum Operation {
    InsertKeysValues {
        node_id: NodeId,
        keys_values: Vec<(String, String)>,
    },
    MarkKeyForDeletion {
        node_id: NodeId,
        key: String,
    },
    AddNode {
        node_id: NodeId,
        peer_seeds: Option<Vec<NodeId>>,
    },
    RemoveNetworkLink(NodeId, NodeId),
    AddNetworkLink(NodeId, NodeId),
    Wait(Duration),
    NodeStateAssert {
        server_node_id: NodeId,
        node_id: NodeId,
        predicate: NodeStatePredicate,
        timeout_opt: Option<Duration>,
    },
}

enum NodeStatePredicate {
    EqualKeyValue(String, String),   // key, value
    KeyPresent(String, bool),        // key, present
    MarkedForDeletion(String, bool), // key, marked
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
                info!(key=%key, present=present, "assert-key-present");
                &node_state.get_versioned(key).is_some() == present
            }
            NodeStatePredicate::MarkedForDeletion(key, marked) => {
                info!(key=%key, marked=marked, "assert-key-marked-for-deletion");
                &node_state.get_versioned(key).unwrap().marked_for_deletion == marked
            }
        }
    }
}

struct Simulator {
    transport: ChannelTransport,
    node_handles: HashMap<NodeId, ChitchatHandle>,
    gossip_interval: Duration,
    marked_for_deletion_key_grace_period: usize,
}

impl Simulator {
    pub fn new(gossip_interval: Duration) -> Self {
        Self {
            transport: ChannelTransport::default(),
            node_handles: HashMap::new(),
            gossip_interval,
            marked_for_deletion_key_grace_period: 5,
        }
    }

    pub async fn execute(&mut self, operations: Vec<Operation>) {
        for operation in operations.into_iter() {
            match operation {
                Operation::AddNode {
                    node_id,
                    peer_seeds,
                } => {
                    self.spawn_node(node_id, peer_seeds).await;
                }
                Operation::InsertKeysValues {
                    node_id,
                    keys_values,
                } => {
                    self.insert_keys_values(node_id, keys_values).await;
                }
                Operation::MarkKeyForDeletion { node_id, key } => {
                    self.mark_for_deletion(node_id, key).await;
                }
                Operation::Wait(duration) => {
                    tokio::time::sleep(duration).await;
                }
                Operation::RemoveNetworkLink(node_1, node_2) => {
                    info!(node_l=%node_1.id, node_r=%node_2.id, "remove-link");
                    self.transport
                        .remove_link(node_1.gossip_public_address, node_2.gossip_public_address)
                        .await;
                }
                Operation::AddNetworkLink(node_1, node_2) => {
                    debug!(node_l=%node_1.id, node_r=%node_2.id, "add-link");
                    self.transport
                        .add_link(node_1.gossip_public_address, node_2.gossip_public_address)
                        .await;
                }
                Operation::NodeStateAssert {
                    server_node_id,
                    node_id,
                    predicate,
                    timeout_opt,
                } => {
                    info!(server_node_id=%server_node_id.id, node_id=%node_id.id, "node-state-assert");
                    let chitchat = self.node_handles.get(&server_node_id).unwrap().chitchat();
                    // Wait for node_state & predicate.
                    if let Some(timeout) = timeout_opt {
                        let chitchat_clone = chitchat.clone();
                        let node_id_clone = node_id.clone();
                        tokio::time::timeout(timeout, async move {
                            loop {
                                let chitchat_guard = chitchat_clone.lock().await;
                                if let Some(node_state) = chitchat_guard.node_state(&node_id_clone) {
                                    if predicate.check(node_state) {
                                        break;
                                    } else {
                                        info!(node_id=%node_id_clone.id, "Waiting for predicate to be true.");
                                    }
                                } else {
                                    info!(node_id=%node_id_clone.id, "Waiting for node state to be present.");
                                }
                                drop(chitchat_guard);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }).await.map_err(|_| {
                            anyhow!("Predicate timeout on node_id={}", node_id.id)
                        }).unwrap();
                    } else {
                        let chitchat_guard = chitchat.lock().await;
                        if let Some(node_state) = chitchat_guard.node_state(&node_id) {
                            let predicate_value = predicate.check(node_state);
                            if !predicate_value {
                                info!(node_id=?node_id.id, state_snapshot=?chitchat_guard.state_snapshot(), "Predicate false.");
                            }
                            assert!(predicate_value);
                        } else {
                            info!(node_id=?node_id.id, state_snapshot=?chitchat_guard.state_snapshot(), "Node state missing.");
                            panic!("Node state missing");
                        }
                    }
                }
            }
        }
    }

    pub async fn insert_keys_values(
        &mut self,
        node_id: NodeId,
        keys_values: Vec<(String, String)>,
    ) {
        info!(node_id=%node_id.id, num_keys_values=?keys_values.len(), "insert-keys-values");
        let chitchat = self.node_handles.get(&node_id).unwrap().chitchat();
        let mut chitchat_guard = chitchat.lock().await;
        for (key, value) in keys_values.into_iter() {
            chitchat_guard.self_node_state().set(key.clone(), value);
        }
    }

    pub async fn mark_for_deletion(&mut self, node_id: NodeId, key: String) {
        info!(node_id=%node_id.id, key=%key, "mark-for-deletion");
        let chitchat = self.node_handles.get(&node_id).unwrap().chitchat();
        let mut chitchat_guard = chitchat.lock().await;
        chitchat_guard.self_node_state().mark_for_deletion(&key);
        let version = chitchat_guard
            .self_node_state()
            .get_versioned(&key)
            .unwrap()
            .version;
        info!(key=%key, version=version, "marked-for-deletion");
    }

    pub async fn spawn_node(&mut self, node_id: NodeId, peer_seeds: Option<Vec<NodeId>>) {
        info!(node_id=%node_id.id, "spawn");
        let seed_nodes: Vec<_> = peer_seeds
            .unwrap_or_else(|| self.node_handles.keys().cloned().collect::<Vec<NodeId>>())
            .iter()
            .map(|node_id| node_id.gossip_public_address.to_string())
            .collect();
        let config = ChitchatConfig {
            node_id: node_id.clone(),
            cluster_id: "default-cluster".to_string(),
            gossip_interval: self.gossip_interval,
            listen_addr: node_id.gossip_public_address,
            seed_nodes,
            failure_detector_config: FailureDetectorConfig {
                initial_interval: self.gossip_interval * 10,
                ..Default::default()
            },
            is_ready_predicate: None,
            marked_for_deletion_grace_period: self.marked_for_deletion_key_grace_period,
        };
        let handle = spawn_chitchat(config, Vec::new(), &self.transport)
            .await
            .unwrap();
        self.node_handles.insert(node_id, handle);
    }
}

pub fn create_node_id(id: &str) -> NodeId {
    let port = find_available_tcp_port().unwrap();
    NodeId {
        id: id.to_string(),
        gossip_public_address: ([127, 0, 0, 1], port).into(),
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
    let _ = tracing_subscriber::fmt::try_init();
    let mut simulator = Simulator::new(Duration::from_millis(50));
    let node_id_1 = create_node_id("node-1");
    let node_id_2 = create_node_id("node-2");
    let operations = vec![
        Operation::AddNode {
            node_id: node_id_1.clone(),
            peer_seeds: None,
        },
        Operation::AddNode {
            node_id: node_id_2.clone(),
            peer_seeds: None,
        },
        Operation::InsertKeysValues {
            node_id: node_id_1.clone(),
            keys_values: vec![("key_a".to_string(), "0".to_string())],
        },
        Operation::InsertKeysValues {
            node_id: node_id_2.clone(),
            keys_values: vec![("key_b".to_string(), "1".to_string())],
        },
        Operation::NodeStateAssert {
            server_node_id: node_id_2.clone(),
            node_id: node_id_1.clone(),
            predicate: NodeStatePredicate::EqualKeyValue("key_a".to_string(), "0".to_string()),
            timeout_opt: Some(Duration::from_millis(200)),
        },
        Operation::NodeStateAssert {
            server_node_id: node_id_1.clone(),
            node_id: node_id_2.clone(),
            predicate: NodeStatePredicate::EqualKeyValue("key_b".to_string(), "1".to_string()),
            timeout_opt: None,
        },
    ];
    simulator.execute(operations).await;
}

#[tokio::test]
async fn test_simple_simulation_with_network_partition() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut simulator = Simulator::new(Duration::from_millis(50));
    let node_id_1 = create_node_id("node-1");
    let node_id_2 = create_node_id("node-2");
    let operations = vec![
        Operation::AddNode {
            node_id: node_id_1.clone(),
            peer_seeds: None,
        },
        Operation::AddNode {
            node_id: node_id_2.clone(),
            peer_seeds: None,
        },
        Operation::InsertKeysValues {
            node_id: node_id_1.clone(),
            keys_values: vec![("key_a".to_string(), "0".to_string())],
        },
        // Wait propagation of states.
        Operation::NodeStateAssert {
            server_node_id: node_id_2.clone(),
            node_id: node_id_1.clone(),
            predicate: NodeStatePredicate::EqualKeyValue("key_a".to_string(), "0".to_string()),
            timeout_opt: Some(Duration::from_millis(500)),
        },
        Operation::RemoveNetworkLink(node_id_1.clone(), node_id_2.clone()),
        Operation::InsertKeysValues {
            node_id: node_id_2.clone(),
            keys_values: vec![("key_b".to_string(), "1".to_string())],
        },
        // Wait propagation of states.
        Operation::NodeStateAssert {
            server_node_id: node_id_1.clone(),
            node_id: node_id_2.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_b".to_string(), false),
            timeout_opt: Some(Duration::from_millis(500)),
        },
    ];
    simulator.execute(operations).await;
}

#[tokio::test]
async fn test_marked_for_deletion_gc_with_network_partition() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut simulator = Simulator::new(Duration::from_millis(50));
    let node_id_1 = create_node_id("node-1");
    let node_id_2 = create_node_id("node-2");
    let node_id_3 = create_node_id("node-3");
    let node_id_4 = create_node_id("node-4");
    let peer_seeds = vec![node_id_1.clone(), node_id_2.clone(), node_id_3.clone()];
    let operations = vec![
        Operation::AddNode {
            node_id: node_id_1.clone(),
            peer_seeds: Some(peer_seeds.clone()),
        },
        Operation::AddNode {
            node_id: node_id_2.clone(),
            peer_seeds: Some(peer_seeds.clone()),
        },
        Operation::AddNode {
            node_id: node_id_3.clone(),
            peer_seeds: Some(peer_seeds.clone()),
        },
        Operation::InsertKeysValues {
            node_id: node_id_1.clone(),
            keys_values: vec![("key_a".to_string(), "0".to_string())],
        },
        Operation::NodeStateAssert {
            server_node_id: node_id_2.clone(),
            node_id: node_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), true),
            timeout_opt: Some(Duration::from_millis(300)),
        },
        // Isolate node 3.
        Operation::RemoveNetworkLink(node_id_1.clone(), node_id_3.clone()),
        Operation::RemoveNetworkLink(node_id_2.clone(), node_id_3.clone()),
        // Mark for deletion key.
        Operation::MarkKeyForDeletion {
            node_id: node_id_1.clone(),
            key: "key_a".to_string(),
        },
        // Check marked for deletion is propagated to node 2.
        Operation::NodeStateAssert {
            server_node_id: node_id_2.clone(),
            node_id: node_id_1.clone(),
            predicate: NodeStatePredicate::MarkedForDeletion("key_a".to_string(), true),
            timeout_opt: Some(Duration::from_millis(300)),
        },
        // Wait for garbage collection
        Operation::Wait(Duration::from_millis(500)),
        Operation::NodeStateAssert {
            server_node_id: node_id_2.clone(),
            node_id: node_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), false),
            timeout_opt: Some(Duration::from_millis(300)),
        },
        Operation::NodeStateAssert {
            server_node_id: node_id_1.clone(),
            node_id: node_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), false),
            timeout_opt: None,
        },
        Operation::NodeStateAssert {
            server_node_id: node_id_3.clone(),
            node_id: node_id_1.clone(),
            predicate: NodeStatePredicate::MarkedForDeletion("key_a".to_string(), false),
            timeout_opt: None,
        },
        // Add node 4 which communicates only with node 3.
        Operation::RemoveNetworkLink(node_id_1.clone(), node_id_4.clone()),
        Operation::RemoveNetworkLink(node_id_2.clone(), node_id_4.clone()),
        Operation::AddNode {
            node_id: node_id_4.clone(),
            peer_seeds: Some(vec![node_id_3.clone()]),
        },
        // Wait for propagation
        // We need to wait longer... because node 4 is just starting?
        Operation::Wait(Duration::from_millis(1000)),
        Operation::NodeStateAssert {
            server_node_id: node_id_3.clone(),
            node_id: node_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), true),
            timeout_opt: Some(Duration::from_millis(500)),
        },
        Operation::NodeStateAssert {
            server_node_id: node_id_4.clone(),
            node_id: node_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), true),
            timeout_opt: Some(Duration::from_millis(500)),
        },
        // Relink node 3
        Operation::AddNetworkLink(node_id_1.clone(), node_id_3.clone()),
        Operation::AddNetworkLink(node_id_1.clone(), node_id_2.clone()),
        Operation::NodeStateAssert {
            server_node_id: node_id_3.clone(),
            node_id: node_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), false),
            timeout_opt: Some(Duration::from_millis(500)),
        },
        Operation::NodeStateAssert {
            server_node_id: node_id_4.clone(),
            node_id: node_id_1.clone(),
            predicate: NodeStatePredicate::KeyPresent("key_a".to_string(), false),
            timeout_opt: Some(Duration::from_millis(500)),
        },
    ];
    simulator.execute(operations).await;
}

// Playground.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_simple_simulation_heavy_insert_delete() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut rng = thread_rng();
    let mut simulator = Simulator::new(Duration::from_millis(1000));
    let mut node_ids = Vec::new();
    for i in 0..50 {
        node_ids.push(create_node_id(&format!("node-{}", i)));
    }
    let seeds = vec![
        node_ids[0].clone(),
        node_ids[1].clone(),
        node_ids[2].clone(),
    ];

    let add_node_operations: Vec<_> = node_ids
        .iter()
        .map(|node_id| Operation::AddNode {
            node_id: node_id.clone(),
            peer_seeds: Some(seeds.clone()),
        })
        .collect();
    simulator.execute(add_node_operations).await;

    let key_names: Vec<_> = (0..50).map(|idx| format!("key_{}", idx)).collect();
    let mut keys_values_inserted_per_node_id: HashMap<NodeId, HashSet<String>> = HashMap::new();
    for node_id in node_ids.iter() {
        let mut keys_values = Vec::new();
        for key in key_names.iter() {
            let value: u64 = rng.gen();
            keys_values.push((key.to_string(), value.to_string()));
            let keys_entry = keys_values_inserted_per_node_id
                .entry(node_id.clone())
                .or_insert_with(HashSet::new);
            keys_entry.insert(key.to_string());
        }
        simulator
            .execute(vec![Operation::InsertKeysValues {
                node_id: node_id.clone(),
                keys_values,
            }])
            .await;
    }

    tokio::time::sleep(Duration::from_millis(5000)).await;
    for (node_id, keys) in keys_values_inserted_per_node_id.clone().into_iter() {
        info!(node_id=?node_id.id, keys=?keys, "check");
        for key in keys {
            let server_node_id = node_ids.choose(&mut rng).unwrap().clone();
            let check_operation = Operation::NodeStateAssert {
                server_node_id,
                node_id: node_id.clone(),
                predicate: NodeStatePredicate::KeyPresent(key.to_string(), true),
                timeout_opt: None,
            };
            simulator.execute(vec![check_operation]).await;
        }
    }

    // Marked all keys for deletion.
    for (node_id, keys) in keys_values_inserted_per_node_id.clone().into_iter() {
        for key in keys {
            let check_operation = Operation::MarkKeyForDeletion {
                node_id: node_id.clone(),
                key,
            };
            simulator.execute(vec![check_operation]).await;
        }
    }

    // Wait for garbage collection to kick in.
    tokio::time::sleep(Duration::from_millis(10000)).await;
    for (node_id, keys) in keys_values_inserted_per_node_id.clone().into_iter() {
        for key in keys {
            let server_node_id = node_ids.choose(&mut rng).unwrap().clone();
            let check_operation = Operation::NodeStateAssert {
                server_node_id,
                node_id: node_id.clone(),
                predicate: NodeStatePredicate::KeyPresent(key.to_string(), false),
                timeout_opt: None,
            };
            simulator.execute(vec![check_operation]).await;
        }
    }
}
