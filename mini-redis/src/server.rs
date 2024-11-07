use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info};

use crate::commands::Command;
use crate::connection::Connection;
use crate::db::{Db, DbDropGuard};
use crate::shutdown::Shutdown;

/// Server listener state.
///
/// Use `run()` to perform TCP listening and initialization of per-connection state.
#[derive(Debug)]
struct Listener {
    // Shared database handle
    db_holder: DbDropGuard,

    // TCP listener supplied by the `run` caller
    listener: TcpListener,

    // A `Semaphore` used to limit the max number of connections thru permits
    limit_connections: Arc<Semaphore>,

    // Broadcast shutdown signal to all active connections
    notify_shutdown: broadcast::Sender<()>,

    // Used as part of the grateful shutdown process to wait for the client
    // connections to complete processing.
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// Per-connection handler. Reads requests from `connection` and applies
/// commands to `db`.
#[derive(Debug)]
struct Handler {
    // Shared database handle
    db: Db,

    // TCP connection decorated with Redis protocol encoder / decoder
    connection: Connection,

    // Listen for shutdown notifications
    shutdown: Shutdown,

    // Used when `Handler` is dropped
    _shutdown_complete: mpsc::Sender<()>,
}

pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // Broadcast channel used to send shutdown message to all active connections
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    let mut server = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(crate::constants::MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    // Concurrently run the server and listen for the 'shutdown' signal
    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
        }
    }

    let Listener {
        notify_shutdown,
        shutdown_complete_tx,
        ..
    } = server;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    // Wait for all active connections to finish processing
    let _ = shutdown_complete_rx.recv().await;
}

impl Listener {
    /// Run server, listen for inbound connections.
    ///
    /// For each inbound connection, spawn a task to process that connection.
    async fn run(&mut self) -> crate::FnResult<()> {
        info!("accepting inbound connections");

        loop {
            // ait for permit to become available
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await?;

            // Accept a new socket. This will attempt to perform error handling
            let socket = self.accept().await?;

            // Create necessary per-connection handler state
            let mut handler = Handler {
                db: self.db_holder.db(),
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            // Spawn new task to process connections. Tokio tasks are like async green threads
            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
                drop(permit);
            });
        }
    }

    /// Accept an inbound connection.
    ///
    /// Errors are handled using exponential backoff.
    async fn accept(&mut self) -> crate::FnResult<TcpStream> {
        let mut backoff: u64 = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times, returns the error
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until back off period elapses
            time::sleep(Duration::from_secs(backoff)).await;

            // Double backoff value
            backoff *= 2;
        }
    }
}

impl Handler {
    /// Process a single connection
    async fn run(&mut self) -> crate::FnResult<()> {
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // Convert Redis frame into a command struct
            let cmd = Command::from_frame(frame)?;

            // Shorthand for `debug!(cmd = format!("{:?}", cmd));`
            debug!(?cmd);

            // Perform work needed to apply the command
            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
                .await?;
        }

        Ok(())
    }
}
