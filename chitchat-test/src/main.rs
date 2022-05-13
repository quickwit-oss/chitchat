use std::net::SocketAddr;
use std::sync::Arc;

use chitchat::server::ChitchatServer;
use chitchat::transport::UdpTransport;
use chitchat::{Chitchat, FailureDetectorConfig, NodeId, SerializableClusterState};
use chitchat_test::ApiResponse;
use cool_id_generator::Size;
use poem::listener::TcpListener;
use poem::{Route, Server};
use poem_openapi::payload::Json;
use poem_openapi::{OpenApi, OpenApiService};
use structopt::StructOpt;
use tokio::sync::Mutex;

struct Api {
    chitchat: Arc<Mutex<Chitchat>>,
}

#[OpenApi]
impl Api {
    /// Chitchat state
    #[oai(path = "/", method = "get")]
    async fn index(&self) -> Json<serde_json::Value> {
        let chitchat_guard = self.chitchat.lock().await;
        let response = ApiResponse {
            cluster_id: chitchat_guard.cluster_id().to_string(),
            cluster_state: SerializableClusterState::from(chitchat_guard.cluster_state()),
            live_nodes: chitchat_guard.live_nodes().cloned().collect::<Vec<_>>(),
            dead_nodes: chitchat_guard.dead_nodes().cloned().collect::<Vec<_>>(),
        };
        Json(serde_json::to_value(&response).unwrap())
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "chitchat", about = "Chitchat test server.")]
struct Opt {
    /// Defines the socket addr on which we should listen to.
    #[structopt(long = "listen_addr", default_value = "127.0.0.1:10000")]
    listen_addr: SocketAddr,
    /// Defines the socket_address (host:port) other servers should use to
    /// reach this server.
    ///
    /// It defaults to the listen address, but this is only valid
    /// when all server are running on the same server.
    #[structopt(long = "public_addr")]
    public_addr: Option<SocketAddr>,

    /// Node id. Has to be unique. If None, the node_id will be generated from
    /// the public_addr and a random suffix.
    #[structopt(long = "node_id")]
    node_id: Option<String>,

    #[structopt(long = "seed")]
    seeds: Vec<String>,
}

fn generate_server_id(public_addr: SocketAddr) -> String {
    let cool_id = cool_id_generator::get_id(Size::Medium);
    format!("server:{}-{}", public_addr, cool_id)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let opt = Opt::from_args();
    println!("{:?}", opt);
    let public_addr = opt.public_addr.unwrap_or(opt.listen_addr);
    let node_id_str = opt
        .node_id
        .unwrap_or_else(|| generate_server_id(public_addr));
    let node_id = NodeId::new(node_id_str, public_addr);
    let udp_transport = Box::new(UdpTransport::open(opt.listen_addr).await?);
    let chitchat_server = ChitchatServer::spawn(
        node_id,
        &opt.seeds[..],
        udp_transport,
        "testing".to_string(),
        Vec::<(&str, &str)>::new(),
        FailureDetectorConfig::default(),
    )
    .await;
    let chitchat = chitchat_server.chitchat();
    let api = Api { chitchat };
    let api_service = OpenApiService::new(api, "Hello World", "1.0")
        .server(&format!("http://{}/", opt.listen_addr));
    let docs = api_service.swagger_ui();
    let app = Route::new().nest("/", api_service).nest("/docs", docs);
    Server::new(TcpListener::bind(&opt.listen_addr))
        .run(app)
        .await?;
    Ok(())
}
