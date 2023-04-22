use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use chitchat::transport::{ChannelTransport, Transport, TransportExt};
use chitchat::{
    spawn_chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, MaxVersion,
};
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tracing::info;

async fn spawn_one(chitchat_id: u16, transport: &dyn Transport) -> ChitchatHandle {
    let listen_addr: SocketAddr = ([127, 0, 0, 1], 10_000u16 + chitchat_id).into();
    let chitchat_id = ChitchatId {
        node_id: format!("node_{chitchat_id}"),
        generation_id: 0,
        gossip_advertise_address: listen_addr,
    };
    let gossip_interval = Duration::from_millis(300);
    let config = ChitchatConfig {
        chitchat_id,
        cluster_id: "default-cluster".to_string(),
        gossip_interval,
        listen_addr,
        seed_nodes: vec!["127.0.0.1:10000".to_string()],
        failure_detector_config: FailureDetectorConfig {
            initial_interval: gossip_interval,
            ..Default::default()
        },
        marked_for_deletion_grace_period: 10_000,
    };
    spawn_chitchat(config, Vec::new(), transport).await.unwrap()
}

async fn spawn_nodes(num_nodes: u16, transport: &dyn Transport) -> Vec<ChitchatHandle> {
    let mut handles = Vec::new();
    for chitchat_id in 0..num_nodes {
        let handle = spawn_one(chitchat_id, transport).await;
        handles.push(handle);
    }
    handles
}

async fn wait_until<P: Fn(&BTreeMap<ChitchatId, MaxVersion>) -> bool>(
    handle: &ChitchatHandle,
    predicate: P,
) -> Duration {
    let start = Instant::now();
    let mut node_watcher = handle.chitchat().lock().await.live_nodes_watcher();
    while let Some(nodes) = node_watcher.next().await {
        if predicate(&nodes) {
            break;
        }
    }
    start.elapsed()
}

async fn delay_before_detection_sample(num_nodes: usize, transport: &dyn Transport) -> Duration {
    assert!(num_nodes > 2);
    let mut handles = spawn_nodes(num_nodes as u16, transport).await;
    info!("spawn {num_nodes} nodes");
    let _delay = wait_until(&handles[1], |nodes| nodes.len() == num_nodes).await;
    info!("converged on {num_nodes} nodes");
    handles.pop();
    let time_to_death_detection =
        wait_until(&handles[1], |nodes| nodes.len() == num_nodes - 1).await;
    for handle in handles {
        handle.shutdown().await.unwrap();
    }
    info!(time_to_death_detection=?time_to_death_detection);
    time_to_death_detection
}

#[tokio::test]
async fn test_delay_before_dead_detection_10() {
    let _ = tracing_subscriber::fmt::try_init();
    let transport = ChannelTransport::default();
    let delay = delay_before_detection_sample(40, &transport).await;
    assert!(delay < Duration::from_secs(4));
}

#[tokio::test]
async fn test_delay_before_dead_detection_20() {
    let _ = tracing_subscriber::fmt::try_init();
    let transport = ChannelTransport::default();
    let delay = delay_before_detection_sample(20, &transport).await;
    assert!(delay < Duration::from_secs(4));
}

#[tokio::test]
async fn test_delay_before_dead_detection_40() {
    let _ = tracing_subscriber::fmt::try_init();
    let transport = ChannelTransport::default();
    let delay = delay_before_detection_sample(40, &transport).await;
    assert!(delay < Duration::from_secs(5));
}

#[tokio::test]
async fn test_delay_before_dead_detection_100() {
    let _ = tracing_subscriber::fmt::try_init();
    let transport = ChannelTransport::default();
    let delay = delay_before_detection_sample(100, &transport).await;
    assert!(delay < Duration::from_secs(5));
}

#[tokio::test]
async fn test_delay_before_dead_detection_100_faulty() {
    let _ = tracing_subscriber::fmt::try_init();
    let transport = ChannelTransport::default().drop_message(0.5f64);
    let delay = delay_before_detection_sample(100, &*transport).await;
    assert!(delay < Duration::from_secs(10));
}

async fn test_bandwidth_aux(num_nodes: usize) -> u64 {
    let _ = tracing_subscriber::fmt::try_init();
    assert!(num_nodes > 2);
    let transport = ChannelTransport::default();
    let handles = spawn_nodes(num_nodes as u16, &transport).await;
    let instant = Instant::now();
    for handle in &handles {
        wait_until(handle, |nodes| nodes.len() == num_nodes).await;
        info!("success");
    }
    let cluster_convergence = instant.elapsed();
    info!(cluster_convergence=?cluster_convergence);
    const MEASUREMENT_FRAME_SECS: u64 = 3;
    let stat_before = transport.statistics();
    tokio::time::sleep(Duration::from_secs(MEASUREMENT_FRAME_SECS)).await;
    let stat_after = transport.statistics();
    let bytes_per_sec_per_node = (stat_after.cumulated_num_bytes - stat_before.cumulated_num_bytes)
        / (num_nodes as u64 * MEASUREMENT_FRAME_SECS);
    let num_messages_per_sec_per_node = (stat_after.num_messages - stat_before.num_messages)
        / (num_nodes as u64 * MEASUREMENT_FRAME_SECS);
    info!(num_messages_per_sec_per_node=num_messages_per_sec_per_node, bandwidth_per_node=%bytes_per_sec_per_node);
    assert!(num_messages_per_sec_per_node < 50);
    bytes_per_sec_per_node
}

#[tokio::test]
async fn test_bandwidth_10() {
    let _ = tracing_subscriber::fmt::try_init();
    assert!(test_bandwidth_aux(10).await < 15_000);
}

#[tokio::test]
async fn test_bandwidth_20() {
    let _ = tracing_subscriber::fmt::try_init();
    test_bandwidth_aux(20).await;
    assert!(test_bandwidth_aux(20).await < 25_000);
}

#[tokio::test]
async fn test_bandwidth_40() {
    let _ = tracing_subscriber::fmt::try_init();
    assert!(test_bandwidth_aux(40).await < 50_000);
}

#[tokio::test]
async fn test_bandwidth_100() {
    let _ = tracing_subscriber::fmt::try_init();
    assert!(test_bandwidth_aux(100).await < 120_000);
}

async fn test_faulty_network_stability_aux(num_nodes: usize, transport: &dyn Transport) {
    // 50% messages are dropped.
    assert!(num_nodes > 2);
    let handles = spawn_nodes(num_nodes as u16, transport).await;
    let start = Instant::now();
    for handle in &handles {
        wait_until(handle, |nodes| nodes.len() == num_nodes).await;
    }
    let elapsed = start.elapsed();
    info!("Convergence took {elapsed:?}");
    let lost_one_node = wait_until(&handles[1], |nodes| nodes.len() != num_nodes);
    // We want this to timeout!
    tokio::time::timeout(Duration::from_secs(30), lost_one_node)
        .await
        .unwrap_err();
}

#[tokio::test]
async fn test_faulty_network_stability_10() {
    let _ = tracing_subscriber::fmt::try_init();
    // 50% messages are dropped.
    use chitchat::transport::TransportExt;
    let transport: Box<dyn Transport> = ChannelTransport::default().drop_message(0.5f64);
    test_faulty_network_stability_aux(10, &*transport).await;
}

#[tokio::test]
async fn test_faulty_network_stability_100() {
    let _ = tracing_subscriber::fmt::try_init();
    // 50% messages are dropped.
    use chitchat::transport::TransportExt;
    let transport: Box<dyn Transport> = ChannelTransport::default().drop_message(0.5f64);
    test_faulty_network_stability_aux(10, &*transport).await;
}
