use chitchat::{
    ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, NodeState, spawn_chitchat,
    transport::{ChannelTransport, Transport, TransportExt},
};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tracing::info;

const DEFAULT_MTU: usize = 65_507;
const DETECTION_TIMEOUT_SECS: u64 = 10;
const FAULTY_DETECTION_TIMEOUT_SECS: u64 = 15;
const PACKET_LOSS_RATE: f64 = 0.5;
const MEASUREMENT_FRAME_SECS: u64 = 3;
const MAX_MESSAGES_PER_SEC_PER_NODE: u64 = 50;

fn init_tracing() {
    let _ = tracing_subscriber::fmt::try_init();
}

/// Spawn "num_nodes" chitchat nodes
async fn spawn_nodes(num_nodes: u16, transport: &dyn Transport) -> Vec<ChitchatHandle> {
    let mut handles: Vec<ChitchatHandle> = Vec::new();
    // per-node config
    let gossip_interval = Duration::from_millis(300);
    let cluster_id: String = "default-cluster".to_string();

    for chitchat_id in 0..num_nodes {
        let listen_addr: SocketAddr = ([127, 0, 0, 1], 10_000u16 + chitchat_id).into();
        let config = ChitchatConfig {
            chitchat_id: ChitchatId {
                node_id: Arc::from(format!("node_{chitchat_id}").as_str()),
                generation_id: 0,
                gossip_advertise_addr: listen_addr,
            },
            cluster_id: cluster_id.clone(),
            gossip_interval,
            listen_addr,
            seed_nodes: vec!["127.0.0.1:10000".to_string()],
            failure_detector_config: FailureDetectorConfig {
                initial_interval: gossip_interval,
                ..Default::default()
            },
            marked_for_deletion_grace_period: Duration::from_secs(10_000),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };
        let spawned_node = spawn_chitchat(config, Vec::new(), transport).await.unwrap();
        handles.push(spawned_node);
    }
    handles
}

async fn wait_until<P: Fn(&BTreeMap<ChitchatId, NodeState>) -> bool>(
    node_handle: &ChitchatHandle,
    predicate: P,
) -> Duration {
    let start = Instant::now();
    let mut node_watcher = node_handle
        .chitchat()
        .lock()
        .await
        .live_nodes_watch_stream();
    while let Some(nodes) = node_watcher.next().await {
        if predicate(&nodes) {
            break;
        }
    }
    start.elapsed()
}

async fn delay_before_detection_sample(num_nodes: usize, transport: &dyn Transport) -> Duration {
    assert!(num_nodes > 2);
    // spawned n nodes
    let mut node_handles = spawn_nodes(num_nodes as u16, transport).await;
    info!("spawn {num_nodes} nodes");

    // wait until all nodes are visible to node 1
    wait_until(&node_handles[1], |nodes| nodes.len() == num_nodes).await;
    info!("converged on {num_nodes} nodes");

    // one node died
    node_handles.pop();

    // time to detect failure (or to realize that we have n-1 nodes now)
    let time_to_death_detection =
        wait_until(&node_handles[1], |nodes| nodes.len() == num_nodes - 1).await;

    for handle in node_handles {
        handle.shutdown().await.unwrap();
    }
    info!(time_to_death_detection=?time_to_death_detection);
    time_to_death_detection
}

macro_rules! death_detection_test {
    ($name:ident, $num_nodes:expr, $timeout_secs:expr) => {
        #[tokio::test]
        async fn $name() {
            init_tracing();
            let transport = ChannelTransport::with_mtu(DEFAULT_MTU);
            let delay = delay_before_detection_sample($num_nodes, &transport).await;
            assert!(
                delay < Duration::from_secs($timeout_secs),
                "Delay Exceeded {:?}",
                delay,
            )
        }
    };
}

macro_rules! death_detection_test_faulty {
    ($name:ident, $num_nodes:expr, $timeout_secs:expr, $loss_rate:expr) => {
        #[tokio::test]
        async fn $name() {
            init_tracing();
            let transport = ChannelTransport::with_mtu(DEFAULT_MTU).drop_message($loss_rate);
            let delay = delay_before_detection_sample($num_nodes, &*transport).await;
            assert!(
                delay < Duration::from_secs($timeout_secs),
                "Delay Exceeded {:?}",
                delay,
            )
        }
    };
}

death_detection_test!(
    test_delay_before_dead_detection_10,
    10,
    DETECTION_TIMEOUT_SECS
);
death_detection_test!(
    test_delay_before_dead_detection_20,
    20,
    DETECTION_TIMEOUT_SECS
);
death_detection_test!(
    test_delay_before_dead_detection_40,
    40,
    DETECTION_TIMEOUT_SECS
);
death_detection_test!(
    test_delay_before_dead_detection_100,
    100,
    DETECTION_TIMEOUT_SECS
);
death_detection_test_faulty!(
    test_delay_before_dead_detection_100_faulty,
    100,
    FAULTY_DETECTION_TIMEOUT_SECS,
    PACKET_LOSS_RATE
);

struct BandwidthMetrics {
    bytes_per_sec_per_node: u64,
    messages_per_sec_per_node: u64,
}

async fn measure_bandwidth(num_nodes: usize) -> BandwidthMetrics {
    assert!(num_nodes > 2);
    let transport = ChannelTransport::with_mtu(DEFAULT_MTU);
    let handles = spawn_nodes(num_nodes as u16, &transport).await;
    let start = Instant::now();

    for node_handle in &handles {
        // wait for convergence
        wait_until(node_handle, |nodes| nodes.len() == num_nodes).await;
    }
    let convergence_time = start.elapsed();
    info!(?convergence_time, "Cluster converged");

    let stats_before = transport.statistics();
    tokio::time::sleep(Duration::from_secs(MEASUREMENT_FRAME_SECS)).await;
    let stats_after = transport.statistics();

    let total_bytes = stats_after.num_bytes_total - stats_before.num_bytes_total;
    let total_messages = stats_after.num_messages_total - stats_before.num_messages_total;

    BandwidthMetrics {
        bytes_per_sec_per_node: total_bytes / (num_nodes as u64 * MEASUREMENT_FRAME_SECS),
        messages_per_sec_per_node: total_messages / (num_nodes as u64 * MEASUREMENT_FRAME_SECS),
    }
}

macro_rules! bandwidth_test {
    ($name:ident, $num_nodes:expr, $max_bytes_per_sec:expr) => {
        #[tokio::test]
        async fn $name() {
            init_tracing();
            let metrics = measure_bandwidth($num_nodes).await;

            assert!(
                metrics.messages_per_sec_per_node < MAX_MESSAGES_PER_SEC_PER_NODE,
                "messages rate too high: {} msg's per node (limit:{} msg's/node) for {} nodes",
                metrics.messages_per_sec_per_node,
                MAX_MESSAGES_PER_SEC_PER_NODE,
                $num_nodes
            );
            assert!(
                metrics.bytes_per_sec_per_node < $max_bytes_per_sec,
                "bandwidth too high: {} byte's per node (limit:{} bytes's/node) for {} nodes",
                metrics.bytes_per_sec_per_node,
                $max_bytes_per_sec,
                $num_nodes
            );
        }
    };
}

bandwidth_test!(test_bandwidth_10, 10, 15_000);
bandwidth_test!(test_bandwidth_20, 20, 25_000);
bandwidth_test!(test_bandwidth_40, 40, 50_000);
bandwidth_test!(test_bandwidth_100, 100, 120_000);

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
    init_tracing();
    // 50% messages are dropped.
    let transport = ChannelTransport::with_mtu(DEFAULT_MTU).drop_message(PACKET_LOSS_RATE);
    test_faulty_network_stability_aux(10, &*transport).await;
}

#[tokio::test]
async fn test_faulty_network_stability_100() {
    init_tracing();
    // 50% messages are dropped.
    let transport = ChannelTransport::with_mtu(DEFAULT_MTU).drop_message(PACKET_LOSS_RATE);
    test_faulty_network_stability_aux(100, &*transport).await;
}
