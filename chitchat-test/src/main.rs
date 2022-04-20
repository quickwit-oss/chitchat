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

use std::sync::Arc;

use chitchat::server::ScuttleServer;
use chitchat::{FailureDetectorConfig, NodeId, ScuttleButt, SerializableClusterState};
use chitchat_test::ApiResponse;
use poem::listener::TcpListener;
use poem::{Route, Server};
use poem_openapi::payload::Json;
use poem_openapi::{OpenApi, OpenApiService};
use structopt::StructOpt;
use tokio::sync::Mutex;

struct Api {
    chitchat: Arc<Mutex<ScuttleButt>>,
}

#[OpenApi]
impl Api {
    /// Scuttlebutt state
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
#[structopt(name = "Scuttlebutt", about = "An example of StructOpt usage.")]
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

    let chitchat_server = ScuttleServer::spawn(
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
