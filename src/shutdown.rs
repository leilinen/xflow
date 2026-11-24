use tokio::sync::watch;

/// Process-wide shutdown signal shared by the poller and the share executor.
///
/// The token is owned by the signal monitor; every consumer holds a
/// [`ShutdownHandle`]. Shutdown is one-way: the first SIGINT/SIGTERM wins and
/// the flag never resets.
#[derive(Debug)]
pub struct ShutdownToken {
    tx: watch::Sender<bool>,
}

impl Default for ShutdownToken {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownToken {
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self { tx }
    }

    pub fn handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            rx: self.tx.subscribe(),
        }
    }

    /// Trigger shutdown. Idempotent; only the first call wakes receivers.
    pub fn trigger(&self) {
        self.tx.send_modify(|triggered| {
            if !*triggered {
                *triggered = true;
            }
        });
    }
}

#[derive(Debug, Clone)]
pub struct ShutdownHandle {
    rx: watch::Receiver<bool>,
}

impl ShutdownHandle {
    pub fn is_triggered(&self) -> bool {
        *self.rx.borrow()
    }

    /// Wait until shutdown is triggered. Returns immediately if shutdown was
    /// already triggered before this call.
    pub async fn wait(&mut self) {
        if *self.rx.borrow_and_update() {
            return;
        }
        loop {
            // A dropped token side means the monitor is gone; treat the
            // channel closing as a shutdown request instead of hanging.
            if self.rx.changed().await.is_err() {
                return;
            }
            if *self.rx.borrow_and_update() {
                return;
            }
        }
    }
}

/// Wait for SIGINT (Ctrl+C) or SIGTERM, then trigger the token once.
pub async fn monitor_signals(token: ShutdownToken) {
    let reason = wait_for_signal().await;
    tracing::info!(
        reason,
        "shutdown signal received; initiating graceful shutdown"
    );
    token.trigger();
}

async fn wait_for_signal() -> &'static str {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => "SIGINT",
        _ = wait_for_sigterm() => "SIGTERM",
    }
}

#[cfg(unix)]
async fn wait_for_sigterm() {
    use tokio::signal::unix::{signal, SignalKind};
    match signal(SignalKind::terminate()) {
        Ok(mut term) => {
            term.recv().await;
        }
        Err(err) => {
            tracing::warn!(?err, "SIGTERM handler unavailable; relying on SIGINT only");
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_sigterm() {
    std::future::pending::<()>().await;
}

#[cfg(test)]
mod tests {
    use super::ShutdownToken;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn handle_wait_returns_when_token_triggers() {
        let token = ShutdownToken::new();
        let mut handle = token.handle();
        assert!(!handle.is_triggered());
        token.trigger();
        timeout(Duration::from_secs(1), handle.wait())
            .await
            .expect("wait should return after trigger");
        assert!(handle.is_triggered());
    }

    #[tokio::test]
    async fn handle_wait_returns_immediately_when_already_triggered() {
        let token = ShutdownToken::new();
        token.trigger();
        let mut handle = token.handle();
        timeout(Duration::from_millis(100), handle.wait())
            .await
            .expect("wait should return without a later trigger");
        assert!(handle.is_triggered());
    }

    #[tokio::test]
    async fn handle_wait_returns_when_token_is_dropped() {
        let mut handle = {
            let token = ShutdownToken::new();
            token.handle()
        };
        timeout(Duration::from_millis(100), handle.wait())
            .await
            .expect("wait should return when the token side is dropped");
    }

    #[tokio::test]
    async fn handles_are_independent_and_trigger_is_idempotent() {
        let token = ShutdownToken::new();
        let mut first = token.handle();
        let mut second = token.handle();
        token.trigger();
        token.trigger();
        timeout(Duration::from_millis(100), first.wait())
            .await
            .expect("first handle should observe trigger");
        timeout(Duration::from_millis(100), second.wait())
            .await
            .expect("second handle should observe trigger");
        assert!(first.is_triggered() && second.is_triggered());
    }
}
