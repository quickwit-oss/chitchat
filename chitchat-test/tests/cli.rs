#![allow(clippy::bool_assert_comparison)]

mod helpers;

use std::process::Child;
use std::thread;
use std::time::Duration;

use chitchat_test::ApiResponse;
use helpers::spawn_command;

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

fn setup_nodes(
    port_offset: usize,
    size: usize,
    wait_stabilization_secs: u64,
    dns_required_for_seed: bool,
) -> Vec<KillOnDrop> {
    let seed_port = port_offset;
    let seed_node = spawn_command(format!("--listen_addr 127.0.0.1:{seed_port}").as_str()).unwrap();
    let mut child_process_handles = vec![KillOnDrop(seed_node)];
    for i in 1..size {
        let node_port = seed_port + i;
        let seed_host_name = if dns_required_for_seed {
            "localhost"
        } else {
            "127.0.0.1"
        };
        let command_args = format!(
            "--listen_addr 127.0.0.1:{node_port} --seed {seed_host_name}:{seed_port} --node_id \
             node_{i}"
        );
        let node = spawn_command(&command_args).unwrap();
        child_process_handles.push(KillOnDrop(node));
    }
    thread::sleep(Duration::from_secs(wait_stabilization_secs));
    child_process_handles
}

fn get_node_info(node_api_endpoint: &str) -> anyhow::Result<ApiResponse> {
    let response = reqwest::blocking::get(node_api_endpoint)?.json::<ApiResponse>()?;
    Ok(response)
}

pub fn setup_logging_for_tests() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        env_logger::builder().format_timestamp(None).init();
    });
}

#[test]
fn test_multiple_nodes() -> anyhow::Result<()> {
    setup_logging_for_tests();
    let _child_handles = setup_nodes(10_000, 5, 4, false);
    // Check node states through api.
    let info = get_node_info("http://localhost:10001")?;
    assert!(info.cluster_state.node_states.get("node_3").is_some());
    assert_eq!(info.cluster_id, "testing");
    assert_eq!(info.live_nodes.len(), 4);
    assert_eq!(info.dead_nodes.len(), 0);
    Ok(())
}

#[test]
fn test_multiple_nodes_with_dns_resolution_for_seed() -> anyhow::Result<()> {
    setup_logging_for_tests();
    let _child_handles = setup_nodes(20_000, 4, 3, true);
    // Check node states through api.
    let info = get_node_info("http://localhost:20001")?;
    assert!(info.cluster_state.node_states.get("node_3").is_some());
    assert_eq!(info.cluster_id, "testing");
    assert_eq!(info.live_nodes.len(), 4);
    assert_eq!(info.dead_nodes.len(), 0);
    Ok(())
}
