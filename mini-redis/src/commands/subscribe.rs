use bytes::Bytes;
use tokio_stream::StreamExt;
use std::pin::Pin;
use tokio::select;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamMap};

use crate::commands::Command;
use crate::commands::Unknown;
use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;
use crate::parse::{Parse, ParseError};
use crate::shutdown::Shutdown;

/// Subscribe the client to one of more channels.
///
/// Once the client enters the subscribed state, it's not supposed to issue any
/// other command except SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PING and QUIT.
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

/// Unsubscribe the client from one or more channels.
///
/// When no channel is specified, client is unsubscribed from all previously subscribed channels.
#[derive(Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

/// Stream of messages to use with `stream!`
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    pub fn new(channels: Vec<String>) -> Subscribe {
        Subscribe { channels }
    }

    /// Parse a `Subscribe` instance from a received frame.
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::FnResult<Subscribe> {
        // Note: the `SUBSCRIBE` string has already been consumed, next values are `channels`
        let mut channels = vec![parse.next_string()?];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(ParseError::EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe { channels })
    }

    /// Apply the `Subscribe` command to the specified `Db` instance.
    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::FnResult<()> {
        // Each individual subscription is handled using `sync::broadcast`.
        // A `StreamMap` is used to track active subscriptions, merging messages from individual channels as they are received.
        let mut subscriptions = StreamMap::new();

        loop {
            for channel_name in self.channels.drain(..) {
                subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            // Wait for one of the following to happen:
            // - Receives msg from subscribed channels => emit frame
            // - Receives subscribe/unsubscribe frame from client
            // - Server shutdown signal
            select! {
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }
                res = dst.read_frame() => {
                  let frame = match res? {
                    Some(frame) => frame,
                    None => return Ok(())
                  };

                  handle_sub_command(
                    frame,
                    &mut self.channels,
                    &mut subscriptions,
                    dst
                    ).await?;
                }
                _ = shutdown.recv() => {
                    return Ok(())
                }
            }
        }
    }

    /// Converts the command into an equivalent `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection,
) -> crate::FnResult<()> {
    let mut rx = db.subscribe(channel_name.clone());

    // Subscribe to the channel
    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                // if we lagged consuming messages, just resume
                Err(broadcast::error::RecvError::Lagged(_)) => {},
                Err(_) => break,
            }
        }
    });

    // Track subscription
    subscriptions.insert(channel_name.clone(), rx);

    let response = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(&response).await?;

    Ok(())
}

async fn handle_sub_command(
    frame: Frame,
    subscribed_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection,
) -> crate::FnResult<()> {
    // Only `SUBSCRIBE` and `UNSUBSCRIBE` commands are permitted in this context
    match Command::from_frame(frame)? {
        Command::Subscribe(subscribe) => {
            subscribed_to.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe) => {
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel_name| channel_name.to_string())
                    .collect();
            }

            for channel_name in unsubscribe.channels {
                subscriptions.remove(&channel_name);

                let response = make_unsubscribe_frame(channel_name, subscriptions.len());
                dst.write_frame(&response).await?;
            }
        }
        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }
    }

    Ok(())
}

/// Create response to a subscribe request.
fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut frame = Frame::array();
    frame.push_bulk(Bytes::from_static(b"subscribe"));
    frame.push_bulk(Bytes::from(channel_name.into_bytes()));
    frame.push_int(num_subs as u64);
    frame
}

/// Create response to an unsubscribe request.
fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut frame = Frame::array();
    frame.push_bulk(Bytes::from_static(b"unsubscribe"));
    frame.push_bulk(Bytes::from(channel_name.into_bytes()));
    frame.push_int(num_subs as u64);
    frame
}

/// Create message informing the client about a new message on specified subscribed channel
fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut frame = Frame::array();
    frame.push_bulk(Bytes::from_static(b"message"));
    frame.push_bulk(Bytes::from(channel_name.into_bytes()));
    frame.push_bulk(msg);
    frame
}

impl Unsubscribe {
    pub fn new(channels: &[String]) -> Unsubscribe {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    /// Parse a `Unubscribe` instance from a received frame.
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::FnResult<Unsubscribe> {
        // Note: the `UNSUBSCRIBE` string has already been consumed, next values are `channels`
        let mut channels = vec![];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(ParseError::EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Unsubscribe { channels })
    }

    /// Converts the command into an equivalent `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}
