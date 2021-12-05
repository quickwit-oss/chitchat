use std::error::Error;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinHandle};

use crate::ScuttleButt;

/// Buffer size for UDP messages.
const BUF_SIZE: usize = 4096;

/// UDP ScuttleButt server.
pub struct ScuttleServer {
    pub scuttlebutt: Arc<Mutex<ScuttleButt>>,

    channel: UnboundedSender<ChannelMessage>,
    join_handle: JoinHandle<()>,
}

impl ScuttleServer {
    /// Launch a new server.
    ///
    /// This will start the ScuttleButt server as a new Tokio background task.
    pub fn spawn(node_id: impl Into<String>, port: u16) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        let scuttlebutt = Arc::new(Mutex::new(ScuttleButt::with_node_id(node_id.into())));
        let scuttlebutt_server = scuttlebutt.clone();

        let join_handle = tokio::spawn(async move {
            let _ = listen(rx, scuttlebutt_server, port).await;
        });

        Self {
            channel: tx,
            scuttlebutt,
            join_handle,
        }
    }

    /// Perform a ScuttleButt "handshake" with another UDP server.
    pub fn gossip(&self, addr: impl Into<String>) {
        let _ = self.channel.send(ChannelMessage::Gossip(addr.into()));
    }

    /// Shut the server down.
    pub async fn shutdown(self) -> Result<(), JoinError> {
        let _ = self.channel.send(ChannelMessage::Shutdown);
        self.join_handle.await
    }
}

/// Listen on the specified UDP port for new ScuttleButt messages.
async fn listen(
    mut channel: UnboundedReceiver<ChannelMessage>,
    scuttlebutt: Arc<Mutex<ScuttleButt>>,
    port: u16,
) -> Result<(), Box<dyn Error>> {
    let addr = format!("0.0.0.0:{}", port);
    let socket = UdpSocket::bind(addr).await?;

    let mut buf = [0; BUF_SIZE];
    loop {
        tokio::select! {
            Ok((len, addr)) = socket.recv_from(&mut buf) => {
                // Handle gossip from other servers.
                let message = bincode::deserialize(&buf[..len])?;
                let response = scuttlebutt.lock().await.process_message(message);

                // Send reply if necessary.
                if let Some(message) = response {
                    let message = bincode::serialize(&message)?;
                    let _ = socket.send_to(&message, addr).await?;
                }
            },
            Some(message) = channel.recv() => match message {
                ChannelMessage::Gossip(addr) => {
                    let syn = scuttlebutt.lock().await.create_syn_message();
                    let message = bincode::serialize(&syn)?;
                    let _ = socket.send_to(&message, addr).await?;
                },
                ChannelMessage::Shutdown => break,
            },
        }
    }

    Ok(())
}

#[derive(Debug)]
enum ChannelMessage {
    Gossip(String),
    Shutdown,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ScuttleButtMessage;

    #[tokio::test]
    async fn syn() {
        let test_addr = "0.0.0.0:2222";
        let socket = UdpSocket::bind(test_addr).await.unwrap();

        let server = ScuttleServer::spawn("node1", 3333);

        server.gossip(test_addr);

        let mut buf = [0; BUF_SIZE];
        let (len, _addr) = socket.recv_from(&mut buf).await.unwrap();

        match bincode::deserialize(&buf[..len]).unwrap() {
            ScuttleButtMessage::Syn { .. } => (),
            message => panic!("unexpected message: {:?}", message),
        }

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn syn_ack() {
        let socket = UdpSocket::bind("0.0.0.0:4444").await.unwrap();
        let test_scuttlebutt = ScuttleButt::with_node_id("test1".into());

        let server = ScuttleServer::spawn("node1", 5555);

        let syn = test_scuttlebutt.create_syn_message();
        let message = bincode::serialize(&syn).unwrap();
        socket.send_to(&message, "0.0.0.0:5555").await.unwrap();

        let mut buf = [0; BUF_SIZE];
        let (len, _addr) = socket.recv_from(&mut buf).await.unwrap();

        match bincode::deserialize(&buf[..len]).unwrap() {
            ScuttleButtMessage::SynAck { .. } => (),
            message => panic!("unexpected message: {:?}", message),
        }

        server.shutdown().await.unwrap();
    }
}
