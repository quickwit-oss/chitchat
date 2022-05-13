use std::sync::Arc;

use chitchat::server::ChitchatServer;
use chitchat::{Chitchat, FailureDetectorConfig, NodeId, SerializableClusterState};
use chitchat_test::{ApiResponse, SetKeyValueResponse};
use poem::listener::TcpListener;
use poem::{Route, Server};
use poem_openapi::param::Query;
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

    /// Set a key & value on this node (with no validation).
    #[oai(path = "/set_kv/", method = "get")]
    async fn set_kv(&self, key: Query<String>, value: Query<String>) -> Json<serde_json::Value> {
        let mut chitchat_guard = self.chitchat.lock().await;

        let cc_state = chitchat_guard.self_node_state();
        cc_state.set(key.as_str(), value.as_str());

        Json(serde_json::to_value(&SetKeyValueResponse { status: true }).unwrap())
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "chitchat", about = "Chitchat test server.")]
struct Opt {
    #[structopt(short = "h", default_value = "localhost:10000")]
    listen_addr: String,
    #[structopt(long = "seed")]
    seeds: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    tracing_subscriber::fmt::init();
    let opt = Opt::from_args();
    println!("{:?}", opt);

    let chitchat_server = ChitchatServer::spawn(
        NodeId::from(opt.listen_addr.as_str()),
        &opt.seeds[..],
        &opt.listen_addr,
        "testing".to_string(),
        Vec::<(&str, &str)>::new(),
        FailureDetectorConfig::default(),
    );
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
