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
use std::sync::Mutex;
use std::time::Duration;
use std::{panic, thread};

use helpers::spawn_command;
use once_cell::sync::Lazy;
use scuttlebutt::ClusterState;

static PEERS: Lazy<Mutex<Vec<(Child, String)>>> =
    Lazy::new(|| Mutex::new(Vec::<(Child, String)>::new()));

fn setup_nodes(size: usize, wait_stabilization_secs: u64) {
    let mut peers_guard = PEERS.lock().unwrap();
    let seed_port = 10000;
    let seed_node = spawn_command(format!("-h localhost:{}", seed_port).as_str()).unwrap();
    peers_guard.push((seed_node, format!("http://localhost:{}", seed_port)));
    for i in 1..=size {
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
}

fn get_node_state(node_api_endpoint: &str) -> anyhow::Result<ClusterState> {
    let state = reqwest::blocking::get(node_api_endpoint)?.json::<ClusterState>()?;
    Ok(state)
}

#[test]
fn test_multiple_nodes() -> anyhow::Result<()> {
    panic::set_hook(Box::new(|_| shutdown_nodes()));
    setup_nodes(3, 5);

    // Check node states through api.
    let state = get_node_state("http://localhost:10001")?;
    assert!(state.node_state("localhost:10003").is_some());

    // TODO: add more checks

    shutdown_nodes();
    Ok(())
}
