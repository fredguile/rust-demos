use tokio::sync::broadcast;

#[derive(Debug)]
pub (crate) struct Shutdown {
    // `true` if shutdown signal has been received
    is_shutdown: bool,

    // Receive channel used to listen for shutdown signal
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    pub(crate) async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }

        let _ = self.notify.recv().await;
        self.is_shutdown = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn receives_shutdown_notification() {
        let (tx, rx) = broadcast::channel(1);
        let mut shutdown = Shutdown::new(rx);
        assert_eq!(shutdown.is_shutdown(), false);

        tx.send(()).unwrap();
        shutdown.recv().await;
        assert_eq!(shutdown.is_shutdown(), true);
    }
}