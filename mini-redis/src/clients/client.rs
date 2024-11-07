use async_stream::try_stream;
use bytes::Bytes;
use std::{
    io::{Error, ErrorKind},
    time::Duration,
};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_stream::Stream;
use tracing::debug;

use crate::{
    commands::{Get, Ping, Publish, Set, Subscribe, Unsubscribe},
    connection::Connection,
    frame::Frame,
};

/// Establish connection with a Redis server.
pub struct Client {
    connection: Connection,
}

/// A client that has entered pub/sub mode.
pub struct Subscriber {
    // Subscribed client
    client: Client,
    subscribed_channels: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

impl Client {
    /// Establish connection with a Redis server located at `addr`.
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::FnResult<Client> {
        let socket = TcpStream::connect(addr).await?;
        let connection = Connection::new(socket);

        Ok(Client { connection })
    }

    /// Ping to the server.
    pub async fn ping(&mut self, msg: Option<Bytes>) -> crate::FnResult<Option<Bytes>> {
        let frame = Ping::new(msg).into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            frame => Err(frame.to_error()),
        }
    }

    /// Get value of a key
    pub async fn get(&mut self, key: &str) -> crate::FnResult<Option<Bytes>> {
        let frame = Get::new(key).into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    /// Set value of a key.
    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::FnResult<()> {
        self.set_cmd(Set::new(key, value, None)).await
    }

    /// Set value of a key with an expiration time.
    pub async fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expires: Duration,
    ) -> crate::FnResult<()> {
        self.set_cmd(Set::new(key, value, Some(expires))).await
    }

    async fn set_cmd(&mut self, cmd: Set) -> crate::FnResult<()> {
        let frame = cmd.into_frame();
        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }

    /// Post `message` to the given `channel`.
    pub async fn publish(&mut self, channel: &str, message: Bytes) -> crate::FnResult<u64> {
        let frame = Publish::new(channel, message).into_frame();
        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Integer(response) => Ok(response),
            frame => Err(frame.to_error()),
        }
    }

    /// Subscribe to the specified channels.
    pub async fn subscribe(mut self, channels: Vec<String>) -> crate::FnResult<Subscriber> {
        self.subscribe_cmd(&channels).await?;

        Ok(Subscriber {
            client: self,
            subscribed_channels: channels,
        })
    }

    async fn subscribe_cmd(&mut self, channels: &[String]) -> crate::FnResult<()> {
        let frame = Subscribe::new(channels.to_vec()).into_frame();
        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        // For each channel subscribed to, server responds with a message confirming subscripton
        for channel in channels {
            let response = self.read_response().await?;

            // Verify it is confirmation of the subscription
            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    // Server responds with an array frame of this shape:
                    // ["subscribe", channel, num_subscribed]
                    [subscribe, schannel, ..]
                        if *subscribe == "subscribe" && *schannel == channel => {}
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }

        Ok(())
    }

    async fn read_response(&mut self) -> crate::FnResult<Frame> {
        let response = self.connection.read_frame().await?;
        debug!(?response);

        match response {
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                // Receiving `None` indicates that server has closed connection without sending a frame.
                let err = Error::new(ErrorKind::ConnectionReset, "connection reset by server");
                Err(err.into())
            }
        }
    }
}

impl Subscriber {
    /// Get list of subscribed channels.
    pub fn get_subscribed(&self) -> &[String] {
        &self.subscribed_channels
    }

    /// Receive next message published on a subscribed channel, waiting if necessary.
    ///
    /// `None` indicates that the subscription has been terminated.
    pub async fn next_message(&mut self) -> crate::FnResult<Option<Message>> {
        match self.client.connection.read_frame().await? {
            Some(frame) => {
                debug!(?frame);

                match frame {
                    Frame::Array(ref frames) => match frames.as_slice() {
                        [message, channel, content] if *message == "message" => Ok(Some(Message {
                            channel: channel.to_string(),
                            content: Bytes::from(content.to_string()),
                        })),
                        _ => Err(frame.to_error()),
                    },
                    frame => Err(frame.to_error()),
                }
            }
            None => Ok(None),
        }
    }

    /// Convert the subscriber into a `Stream` yielding new messages published on subscribed channels.
    pub fn into_stream(mut self) -> impl Stream<Item = crate::FnResult<Message>> {
        // Uses `try_stream!` from `async-stream` as generators aren't stable in Rust.
        // `async-stream` uses a macro to simulate generators on top of async/await (with limitations, read documentation)
        try_stream! {
            while let Some(message) = self.next_message().await? {
                yield message;
            }
        }
    }

    /// Subscribe to a list of new channels
    pub async fn subscribe(&mut self, channels: &[String]) -> crate::FnResult<()> {
        self.client.subscribe_cmd(channels).await?;

        self.subscribed_channels
            .extend(channels.iter().map(Clone::clone));

        Ok(())
    }

    /// Unsubscribe to a list of new channels
    pub async fn unsubscribe(&mut self, channels: &[String]) -> crate::FnResult<()> {
        let frame = Unsubscribe::new(channels).into_frame();
        debug!(request = ?frame);

        self.client.connection.write_frame(&frame).await?;

        // If input channel list is empty, we'll assume the unsubscribed list received
        // matches the client subscribed channels
        let num = if channels.is_empty() {
            self.subscribed_channels.len()
        } else {
            channels.len()
        };

        for _ in 0..num {
            let response = self.client.read_response().await?;

            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    [unsubscribe, channel, ..] if *unsubscribe == "unsubscribe" => {
                        let len = self.subscribed_channels.len();

                        if len == 0 {
                            // There must be at least one channel
                            return Err(response.to_error());
                        }

                        // Unsubscribed channel should exist in the subscribed list at this points
                        self.subscribed_channels.retain(|c| *channel != &c[..]);

                        // Only one channel should be removed from the list of subscribed channels
                        if self.subscribed_channels.len() != len - 1 {
                            return Err(response.to_error());
                        }
                    }
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            }
        }

        Ok(())
    }
}
