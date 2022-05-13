#![allow(clippy::bool_assert_comparison)]

mod helpers;

use std::process::Child;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{panic, thread};

use chitchat_test::{ApiResponse, SetKeyValueResponse};
use helpers::spawn_command;
use once_cell::sync::Lazy;

type PeersList = Arc<Mutex<Vec<(Child, String)>>>;

static PEERS: Lazy<PeersList> = Lazy::new(|| Arc::new(Mutex::new(Vec::<(Child, String)>::new())));

fn setup_nodes(size: usize, wait_stabilization_secs: u64) {
    let mut peers_guard = PEERS.lock().unwrap();
    let seed_port = 10000;
    let seed_node = spawn_command(format!("-h localhost:{}", seed_port).as_str()).unwrap();
    peers_guard.push((seed_node, format!("http://localhost:{}", seed_port)));
    for i in 1..size {
        let node_port = seed_port + i;
        let node = spawn_command(
            format!("-h localhost:{} --seed localhost:{}", node_port, seed_port).as_str(),
        )
        .unwrap();
        peers_guard.push((node, format!("http://localhost:{}", node_port)))
    }
    thread::sleep(Duration::from_secs(wait_stabilization_secs));
}

fn shutdown_nodes() {
    let mut peers_guard = PEERS.lock().unwrap();
    for (node, _) in peers_guard.iter_mut() {
        let _ = node.kill();
    }
    peers_guard.clear();
}

fn get_node_info(node_api_endpoint: &str) -> anyhow::Result<ApiResponse> {
    let response = reqwest::blocking::get(node_api_endpoint)?.json::<ApiResponse>()?;
    Ok(response)
}

fn set_kv(node_api_endpoint: &str, key: &str, value: &str) -> anyhow::Result<SetKeyValueResponse> {
    let simple_set_kv = format!("{}?key={}&value={}", node_api_endpoint, key, value);
    let response = reqwest::blocking::get(simple_set_kv)?.json::<SetKeyValueResponse>()?;
    Ok(response)
}

#[test]
fn test_multiple_nodes() -> anyhow::Result<()> {
    panic::set_hook(Box::new(|error| {
        println!("{}", error);
        shutdown_nodes();
    }));
    setup_nodes(5, 3);

    // Assert that we can set a key.
    let set_kv_response = set_kv("http://localhost:10001/set_kv", "some_key", "some_value")?;
    assert_eq!(set_kv_response.status, true);

    // Check node states through api.
    let info = get_node_info("http://localhost:10001")?;
    assert!(info
        .cluster_state
        .node_states
        .get("localhost:10003")
        .is_some());
    assert_eq!(info.cluster_id, "testing");
    assert_eq!(info.live_nodes.len(), 4);
    assert_eq!(info.dead_nodes.len(), 0);

    // Check that "some_key" we set on this local node (localhost:10001) is
    // indeed set to be "some_value"
    let ns = info
        .cluster_state
        .node_states
        .get("localhost:10001")
        .unwrap();
    let v = ns.get_versioned("some_key").unwrap();
    assert_eq!(v.value, "some_value");

    shutdown_nodes();
    Ok(())
}
