use bytes::Bytes;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;

use crate::clients::client::Client;

// Enum used to message-pass the requested command from the `BufferedClient` handle
#[derive(Debug)]
enum Command {
    Get(String),
    Set(String, Bytes),
}

type Message = (Command, oneshot::Sender<crate::FnResult<Option<Bytes>>>);

/// Receive commands sent through the channel and forward them to client.
///
/// The response is returned back to the caller via a `oneshot`.
async fn run(mut client: Client, mut rx: Receiver<Message>) {
    while let Some((cmd, tx)) = rx.recv().await {
        let response = match cmd {
            Command::Get(key) => client.get(&key).await,
            Command::Set(key, value) => client.set(&key, value).await.map(|_| None),
        };

        let _ = tx.send(response);
    }
}

#[derive(Clone)]
pub struct BufferedClient {
    tx: Sender<Message>,
}

impl BufferedClient {
    /// Create a new client request buffer.
    ///
    /// `Client` performs Redis commands directly on the TCP connection. Only a single request may be in-flight at any given time
    /// and operation requires mutable access to the `Client` handle. This prevents using a single Redis connection for multiple Tokio tasks.
    ///
    /// The strategy around this is to spawn a dedicated Tokio task to manage the Redis connection and use "message passing" to operate on the connection.
    ///
    /// Commands are pushed to a channel, the connection task pops commands off the channel and applies them to the Redis connection.
    ///
    /// When a response is received, it is forwarded to the original requester.
    pub fn buffer(client: Client) -> BufferedClient {
        let (tx, rx) = channel(32);

        tokio::spawn(async move { run(client, rx).await });

        BufferedClient { tx }
    }

    /// Get the value of a key.
    pub async fn get(&mut self, key: &str) -> crate::FnResult<Option<Bytes>> {
        let cmd = Command::Get(key.into());

        let (tx, rx) = oneshot::channel();

        self.tx.send((cmd, tx)).await?;

        match rx.await {
            Ok(res) => res,
            Err(err) => Err(err.into()),
        }
    }

     /// Set the value of a key.
    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::FnResult<()> {
        let cmd = Command::Set(key.into(), value);

        let (tx, rx) = oneshot::channel();

        self.tx.send((cmd, tx)).await?;

        match rx.await {
            Ok(res) => res.map(|_| ()),
            Err(err) => Err(err.into()),
        }
    }
}
