use std::error::Error;

use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use crate::ScuttleButt;

/// Buffer size for UDP messages.
const BUF_SIZE: usize = 4096;

/// UDP ScuttleButt server.
pub struct ScuttleServer {
    channel: UnboundedSender<ChannelMessage>,
    join_handle: JoinHandle<()>,
}

impl ScuttleServer {
    /// Launch a new server.
    ///
    /// This will start the ScuttleButt server as a new Tokio background task.
    pub fn spawn(node_id: impl Into<String>, port: u16) -> Result<Self, Box<dyn Error>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let node_id = node_id.into();

        let join_handle = tokio::spawn(async move {
            let _ = listen(rx, node_id, port).await;
        });

        Ok(Self {
            channel: tx,
            join_handle,
        })
    }

    /// Perform a ScuttleButt "handshake" with another UDP server.
    pub fn gossip(&self, addr: impl Into<String>) -> Result<(), Box<dyn Error>> {
        self.channel.send(ChannelMessage::Gossip(addr.into()))?;
        Ok(())
    }

    /// Shut the server down.
    pub async fn shutdown(self) -> Result<(), Box<dyn Error>> {
        self.channel.send(ChannelMessage::Shutdown)?;
        Ok(self.join_handle.await?)
    }
}

/// Listen on the specified UDP port for new ScuttleButt messages.
async fn listen(
    mut channel: UnboundedReceiver<ChannelMessage>,
    node_id: String,
    port: u16,
) -> Result<(), Box<dyn Error>> {
    let addr = format!("0.0.0.0:{}", port);
    let socket = UdpSocket::bind(addr).await?;

    // TODO: Can't mutate node state after creation.
    let mut scuttlebutt = ScuttleButt::with_node_id(node_id);

    let mut buf = [0; BUF_SIZE];
    loop {
        tokio::select! {
            Ok((len, addr)) = socket.recv_from(&mut buf) => {
                // Handle gossip from other servers.
                let message = bincode::deserialize(&buf[..len])?;
                let response = scuttlebutt.process_message(message);

                // Send reply if necessary.
                if let Some(message) = response {
                    let message = bincode::serialize(&message)?;
                    let _ = socket.send_to(&message, addr).await?;
                }
            },
            Some(message) = channel.recv() => match message {
                ChannelMessage::Gossip(addr) => {
                    let syn = scuttlebutt.create_syn_message();
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
