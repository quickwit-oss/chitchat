#![allow(clippy::bool_assert_comparison)]

mod helpers;

use std::net::TcpListener;
use std::process::Child;
use std::{thread, time::Duration};

use chitchat_test::{ApiResponse, SetKeyValueResponse};
use helpers::spawn_command;

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

struct NodeProcess {
    node_id: String,
    port: u16,
    seed_addr: Option<String>,
    _child: Option<KillOnDrop>, // Never accessed, but included to keep the value alive.
}

impl NodeProcess {
    pub fn new(node_id: String, port: u16, seed_addr: Option<&str>) -> Self {
        Self {
            node_id,
            port,
            seed_addr: seed_addr.map(|x| x.to_string()),
            _child: None,
        }
    }

    fn start(&mut self) -> anyhow::Result<()> {
        // Construct the command arguments
        let mut args = vec![
            "--listen_addr".to_string(),
            format!("127.0.0.1:{}", self.port),
            "--node_id".to_string(),
            self.node_id.clone(),
            "--interval_ms".to_string(),
            "50".to_string(),
        ];

        if let Some(seed) = &self.seed_addr {
            args.push("--seed".to_string());
            args.push(seed.clone());
        }

        // Start the node process
        let child = spawn_command(&args.join(" "))?;
        self._child = Some(KillOnDrop(child));
        return Ok(());
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

fn create_node_process(node_id: &str, seed_addr: Option<&str>) -> anyhow::Result<NodeProcess> {
    // First, find a free port by binding to port 0 to let the OS choose an available port.
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);

    // Second, use the port to construct a new node process.
    let mut process = NodeProcess::new(node_id.to_string(), port, seed_addr);
    process.start()?;

    Ok(process)
}

fn setup_nodes(
    num_nodes: usize,
    wait_stabilization_secs: u64,
    seed_requires_dns: bool,
) -> Vec<NodeProcess> {
    let child_handle = create_node_process("node_0", None).unwrap();
    let seed_port = child_handle.port;
    let mut child_process_handles = vec![child_handle];

    let seed_host_name = if seed_requires_dns {
        "localhost"
    } else {
        "127.0.0.1"
    };
    let seed_addr = format!("{}:{}", seed_host_name, seed_port);

    for i in 1..num_nodes {
        let node_id = format!("node_{i}");
        child_process_handles.push(create_node_process(&node_id, Some(&seed_addr)).unwrap());
    }
    thread::sleep(Duration::from_secs(wait_stabilization_secs));
    child_process_handles
}

fn get_node_info(node_api_endpoint: &str) -> anyhow::Result<ApiResponse> {
    let response = reqwest::blocking::get(node_api_endpoint)?.json::<ApiResponse>()?;
    Ok(response)
}

fn set_kv(node_api_endpoint: &str, key: &str, value: &str) -> anyhow::Result<SetKeyValueResponse> {
    let simple_set_kv = format!("{node_api_endpoint}?key={key}&value={value}");
    let response = reqwest::blocking::get(simple_set_kv)?.json::<SetKeyValueResponse>()?;
    Ok(response)
}
#[test]
fn test_multiple_nodes() {
    let child_handles = setup_nodes(5, 5, false);
    assert_eq!(child_handles.len(), 5);

    // Assert that we can set a key.
    let first_non_seed_node = &child_handles[1];
    let node_base_url = first_non_seed_node.base_url();
    let set_kv_response =
        set_kv(&format!("{node_base_url}/set_kv"), "some_key", "some_value").unwrap();
    assert_eq!(set_kv_response.status, true);

    // Check node states through api.
    let info = get_node_info(&node_base_url).unwrap();
    assert_eq!(info.cluster_id, "testing");
    assert_eq!(info.live_nodes.len(), 5);
    assert_eq!(info.dead_nodes.len(), 0);

    assert!(info.cluster_state.node_state_snapshots.get(3).is_some());
    // Check that "some_key" we set on this local node (localhost:13001) is
    // indeed set to be "some_value"
    let node = info.cluster_state.node_state_snapshots.get(1).unwrap();
    let versioned_value = node.node_state.get_versioned("some_key").unwrap();
    assert_eq!(versioned_value.value, "some_value");
}

#[test]
fn test_multiple_nodes_with_dns_resolution_for_seed() {
    let child_handles = setup_nodes(5, 5, true);
    let first_non_seed_node = &child_handles[1];

    // Check node states through api.
    let info = get_node_info(&first_non_seed_node.base_url()).unwrap();
    assert!(info.cluster_state.node_state_snapshots.get(3).is_some());
    assert_eq!(info.cluster_id, "testing");
    assert_eq!(info.live_nodes.len(), 5);
    assert_eq!(info.dead_nodes.len(), 0);
}
