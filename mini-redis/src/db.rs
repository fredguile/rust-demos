use bytes::Bytes;
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{broadcast, Notify},
    time::{self, Instant},
};
use tracing::debug;

/// A wrapper around `Db` instances to allow orderly cleanup of
/// `Db` by signaling the background purge task to shutdown when
/// this struct is dropped.
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    db: Db,
}

/// Server state shared across all connections.
///
/// `Db` contains a `HashMap` storing the KV data and all
/// `broadcast::Sender` values from acive pub/sub channels.
///
/// When a `Db` is created, a background task is spawned. It is
/// set to expire values after the requested duration as elapsed.
///
/// The task runs until all instances of `Db` are dropped.
#[derive(Clone, Debug)]
pub(crate) struct Db {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// Note: it is a `std::sync::Mutex` and not a Tokio mutex as there is not
    /// asynchronous operations being performed while holding this mutex.
    ///
    /// As the critical sections are small, it won't block for long.
    ///
    /// (consider using a Tokio mutex and `tokio::task::spawn_blocking` for longer operations).
    state: Mutex<State>,

    /// Notify the background task handling entry expiration and shutdown.
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// KV store
    entries: HashMap<String, Entry>,

    /// Pub/sub key space (as Redis uses a separate key space for KV and pub/sub).
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// Tracks key TTLs.
    ///
    /// A `BTreeSet` is used to maintain expiration sorted by when they expire.
    /// This allows the background task to interate this map to find the value expiring next.
    ///
    /// While unlikely, it is possible for more than one expiration to be created for the same `Instant`.
    /// Hence we're adding a unique key `String` to our key.
    expirations: BTreeSet<(Instant, String)>,

    /// `true` when the `Db` instance is shutting down. It will signal to the background task to exit.
    shutdown: bool,
}

#[derive(Debug)]
struct Entry {
    data: Bytes,
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    /// Create a new `DbDropGuard` wrapping a `Db` instance.
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// Get the shared database.
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // Signal the 'Db' instance to shut down the task that purges expired key
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// Create a new, empty, `Db` instance. Allocate shared state and spawn
    /// a background task to manage key expiration.
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // Start background task.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// Get value associated with key.
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// Set value associated with key and optional expiration duration.
    ///
    /// If a value is already associated with the key, it is removed.
    pub(crate) fn set(&self, key: String, data: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();
        let mut notify_background_task = false;

        let expires_at = expire.map(|duration| {
            let expires_at = Instant::now() + duration;

            // only notify background task if the newly inserted expiration is the
            // next key to evict
            notify_background_task = state
                .next_expiration()
                .map(|expiration| expiration > expires_at)
                .unwrap_or(true);

            expires_at
        });

        let prev = state
            .entries
            .insert(key.clone(), Entry { data, expires_at });

        // If there is a value previously associated with the key **and** it has an expiration time,
        // the associated entry in `expirations` must be removed for avoiding leaking data.
        if let Some(prev) = prev {
            if let Some(prev_expires_at) = prev.expires_at {
                // clear expiration
                state.expirations.remove(&(prev_expires_at, key.clone()));
            }
        }

        // Track new entry expiration
        if let Some(expires_at) = expires_at {
            state.expirations.insert((expires_at, key));
        }

        // Release mutex before notifying background task
        drop(state);

        if notify_background_task {
            self.shared.background_task.notify_one();
        }
    }

    /// Returns a `Receiver` for the requested channel.
    ///
    /// The returned `Receiver` is used to receive values broadcast by `PUBLISH` commands.
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        let mut state = self.shared.state.lock().unwrap();

        // If there is no entry for the requested channel, then create a new broadcast
        // channel and associate it with the key.
        match state.pub_sub.entry(key) {
            Entry::Occupied(entry) => entry.get().subscribe(),
            Entry::Vacant(entry) => {
                // channel is created with a capacity of `1024` messages
                let (tx, rx) = broadcast::channel(1024);
                entry.insert(tx);
                rx
            }
        }
    }

    /// Publish a message to the channel. Returns the number of subscribers listening to that channel.
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // On a successful message sent to the broadcast channel, the number
            // of subscribers is returned
            .map(|tx| tx.send(value).unwrap_or(0))
            // If there's no entry for that key, then there's no subscriber
            .unwrap_or(0)
    }

    /// Signals the purge background task to shut down.
    fn shutdown_purge_task(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // drop lock before signaling the background task
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    /// Purge expired keys and return the `Instant` at which the next key will expire.
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            return None;
        }

        // This is needed to make the borrow checker happy. In short, `lock()`
        // returns a `MutexGuard` and not a `&mut State`. The borrow checker is
        // not able to see "through" the mutex guard and determine that it is
        // safe to access both `state.expirations` and `state.entries` mutably,
        // so we get a "real" mutable reference to `State` outside of the loop.
        let state = &mut *state;

        let now = Instant::now();

        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                return Some(when);
            }

            // the key has expired, remove it
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }

    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// Once notified, purge any expired key from the state handle.
///
/// If shutdown is set, terminates the task
async fn purge_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        if let Some(when) = shared.purge_expired_keys() {
            // wait until the next key expires, or until background task is notified.
            tokio::select! {
                _=time::sleep_until(when) => {},
                _=shared.background_task.notified() => {},
            }
        } else {
            shared.background_task.notified().await;
        }
    }

    debug!("Purge background task shut down");
}
