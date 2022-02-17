// Copyright (C) 2021 Quickwit, Inc.
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

#![allow(clippy::bool_assert_comparison)]

mod helpers;

use std::process::Child;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{panic, thread};

use helpers::spawn_command;
use once_cell::sync::Lazy;
use scuttlebutt_test::ApiResponse;
use serial_test::serial;

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

#[test]
#[serial]
fn test_multiple_nodes() -> anyhow::Result<()> {
    panic::set_hook(Box::new(|error| {
        println!("{}", error);
        shutdown_nodes();
    }));
    setup_nodes(5, 3);

    // Check node states through api.
    let info = get_node_info("http://localhost:10001")?;
    assert!(info.cluster_state.node_state("localhost:10003").is_some());
    assert_eq!(info.live_nodes.len(), 4);
    assert_eq!(info.dead_nodes.len(), 0);

    shutdown_nodes();
    Ok(())
}

#[test]
#[serial]
fn test_node_goes_from_live_to_down_to_live() -> anyhow::Result<()> {
    panic::set_hook(Box::new(|error| {
        println!("{}", error);
        shutdown_nodes();
    }));
    setup_nodes(7, 3);

    // Check node states through api.
    let info = get_node_info("http://localhost:10001")?;
    assert!(info.cluster_state.node_state("localhost:10003").is_some());
    assert_eq!(info.live_nodes.len(), 6);
    assert_eq!(info.dead_nodes.len(), 0);

    // take down node at http://localhost:10003
    let mut peers_guard = PEERS.lock().unwrap();
    let (mut process, node_id) = peers_guard.remove(3);
    assert_eq!(node_id, "http://localhost:10003");
    process.kill().unwrap();
    drop(peers_guard);

    thread::sleep(Duration::from_secs(20));
    let info = get_node_info("http://localhost:10001")?;
    assert!(info.cluster_state.node_state("localhost:10003").is_some());
    assert_eq!(info.live_nodes.len(), 5);
    assert_eq!(info.dead_nodes, ["localhost:10003"]);

    // restart node at http://localhost:10003
    let mut node_process = spawn_command("-h localhost:10003 --seed localhost:10000").unwrap();
    thread::sleep(Duration::from_secs(6));
    let info = get_node_info("http://localhost:10001")?;
    assert!(info.cluster_state.node_state("localhost:10003").is_some());
    assert_eq!(info.live_nodes.len(), 6);
    assert_eq!(info.dead_nodes.len(), 0);

    let _ = node_process.kill();
    shutdown_nodes();
    Ok(())
}

#[test]
#[serial]
fn test_network_partition_nodes() -> anyhow::Result<()> {
    panic::set_hook(Box::new(|error| {
        println!("{}", error);
        shutdown_nodes();
    }));

    for node_id in 10000..=10006 {
        let seeds = (10000..=10006)
            .filter(|peer_id| peer_id != &node_id)
            .map(|peer_id| format!("localhost:{}", peer_id))
            .collect::<Vec<_>>()
            .join(" ");

        let moved_peers = PEERS.clone();
        thread::spawn(move || {
            let cmd_arguments = format!("-h localhost:{} --seed {}", node_id, seeds);
            let node = spawn_command(cmd_arguments.as_str()).unwrap();

            let mut peers_guard = moved_peers.lock().unwrap();
            peers_guard.push((node, format!("http://localhost:{}", node_id)))
        });
    }
    thread::sleep(Duration::from_secs(3));

    // Check nodes know each other.
    for node_id in 10000..=10006 {
        let info = get_node_info(format!("http://localhost:{}", node_id).as_str())?;
        for peer_id in (10000..=10006).filter(|peer_id| peer_id != &node_id) {
            assert!(info
                .cluster_state
                .node_state(format!("localhost:{}", peer_id).as_str())
                .is_some());
        }
    }

    shutdown_nodes();
    Ok(())
}
