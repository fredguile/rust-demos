use bytes::Bytes;
use std::time::Duration;
use tokio::{net::ToSocketAddrs, runtime::Runtime};

pub use crate::clients::client::Message;

pub struct BlockingClient {
    // The asynchronous `Client`
    inner: crate::clients::client::Client,

    // A `current_thread` runtime for executing operations
    // on the async client in a blocking manner.
    runtime: Runtime,
}

pub struct BlockingSubscriber {
    // The asynchronous `Subscriber`
    inner: crate::clients::client::Subscriber,

    // A `current_thread` runtime for executing operations
    // on the async client in a blocking manner.
    runtime: Runtime,
}

struct SubscriberIterator {
    // The asynchronous `Subscriber`
    inner: crate::clients::client::Subscriber,

    // A `current_thread` runtime for executing operations
    // on the async client in a blocking manner.
    runtime: Runtime,
}

impl BlockingClient {
    pub fn connect(addr: impl ToSocketAddrs) -> crate::FnResult<BlockingClient> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let inner = runtime.block_on(crate::clients::client::Client::connect(addr))?;

        Ok(BlockingClient { inner, runtime })
    }

    /// Get the value of a key.
    pub fn get(&mut self, key: &str) -> crate::FnResult<Option<Bytes>> {
        self.runtime.block_on(self.inner.get(key))
    }

    /// Set the value of a key.
    pub fn set(&mut self, key: &str, value: Bytes) -> crate::FnResult<()> {
        self.runtime.block_on(self.inner.set(key, value))
    }

    /// Set the value of a key with an expiration time.
    pub fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expires: Duration,
    ) -> crate::FnResult<()> {
        self.runtime
            .block_on(self.inner.set_expires(key, value, expires))
    }

    /// Post `message` to the given `channel`.
    pub fn publish(&mut self, channel: &str, message: Bytes) -> crate::FnResult<u64> {
        self.runtime.block_on(self.inner.publish(channel, message))
    }

    /// Subscribe to the specified channels.
    pub fn subscribe(self, channels: Vec<String>) -> crate::FnResult<BlockingSubscriber> {
        let subscriber = self.runtime.block_on(self.inner.subscribe(channels))?;

        Ok(BlockingSubscriber {
            inner: subscriber,
            runtime: self.runtime,
        })
    }
}

impl BlockingSubscriber {
    /// Returns the set of channels currently subscribed to.
    pub fn get_subscribed(&self) -> &[String] {
        self.inner.get_subscribed()
    }

    /// Receive next message published on a subscribed channel, waiting if necessary.
    ///
    /// `None` indicates that the subscription has been terminated.
    pub fn next_message(&mut self) -> crate::FnResult<Option<Message>> {
        self.runtime.block_on(self.inner.next_message())
    }

    /// Convert the subscriber into an `Iterator` yielding new messages published
    /// on subscribed channels.
    pub fn into_iter(self) -> impl Iterator<Item = crate::FnResult<Message>> {
        SubscriberIterator {
            inner: self.inner,
            runtime: self.runtime,
        }
    }

    /// Subscribe to a list of new channels
    pub fn subscribe(&mut self, channels: &[String]) -> crate::FnResult<()> {
        self.runtime.block_on(self.inner.subscribe(channels))
    }

    /// Unsubscribe to a list of new channels
    pub fn unsubscribe(&mut self, channels: &[String]) -> crate::FnResult<()> {
        self.runtime.block_on(self.inner.unsubscribe(channels))
    }
}

impl Iterator for SubscriberIterator {
    type Item = crate::FnResult<Message>;

    fn next(&mut self) -> Option<Self::Item> {
        self.runtime.block_on(self.inner.next_message()).transpose()
    }
}
