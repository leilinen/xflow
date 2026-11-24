//! Bounded, serial share executor (Stage 4A).
//!
//! One bounded MPSC queue (capacity 8), exactly one worker task running a
//! request at a time, panic isolation per task, and a bounded graceful
//! shutdown driven by the process-wide [`ShutdownHandle`]. No middleware, no
//! unbounded spawning, no deduplication.
//!
//! Ownership is the key to panic isolation: the queue Receiver lives in the
//! supervisor loop below, never inside a task that could panic. Each request
//! is executed in its own spawned task whose JoinHandle is awaited by the
//! supervisor, so a panicking handler is captured as a `JoinError` and the
//! supervisor moves on to the next request. At most one task handle exists at
//! any moment, so no recovery fan-out is possible.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};

use super::ShareRequest;
use crate::shutdown::ShutdownHandle;

/// Buffered queue capacity. At most this many requests are buffered plus one
/// in-flight task outside the buffer.
pub(crate) const SHARE_QUEUE_CAPACITY: usize = 8;

/// Graceful-shutdown drain deadline: the single in-flight task may take at
/// most this long to finish before it is aborted. This bounds the drain wait;
/// it does not guarantee that every external HTTP request fits within it.
pub(crate) const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

/// Outcome of one share task as reported by the processor.
pub(crate) type ShareTaskResult = Result<(), anyhow::Error>;

/// Processor future. Boxed so processors can be simple closures.
pub(crate) type ShareTaskFuture = Pin<Box<dyn Future<Output = ShareTaskResult> + Send>>;

/// Handles exactly one request. Stage 4B replaces the production instance
/// with the real lookup/fetch/persist/send pipeline.
pub(crate) type ShareTaskProcessor = Arc<dyn Fn(ShareRequest) -> ShareTaskFuture + Send + Sync>;

/// Why a request was not accepted. The request is returned so the caller can
/// log it (and, in Stage 4B, notify the user) without losing it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TryEnqueueError {
    /// The queue already holds its capacity of pending requests.
    QueueFull(ShareRequest),
    /// The executor is shut down and no longer accepts requests.
    Closed(ShareRequest),
}

/// What happened to queued and in-flight work during executor shutdown.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ExecutorSummary {
    /// Requests accepted but never started; each one also produced a
    /// structured warn log when it was dropped.
    pub(crate) dropped_queued: usize,
    /// The in-flight task exceeded the drain deadline and was aborted.
    pub(crate) aborted_inflight: bool,
}

#[derive(Debug)]
pub(crate) struct ShareExecutor {
    request_tx: mpsc::Sender<ShareRequest>,
    shutdown: ShutdownHandle,
    supervisor: JoinHandle<ExecutorSummary>,
}

impl ShareExecutor {
    /// Start the executor with the production 60s drain deadline.
    pub(crate) fn start(processor: ShareTaskProcessor, shutdown: ShutdownHandle) -> Self {
        Self::start_with_drain_timeout(processor, shutdown, SHUTDOWN_DRAIN_TIMEOUT)
    }

    /// Start the executor with an explicit drain deadline (used by tests).
    pub(crate) fn start_with_drain_timeout(
        processor: ShareTaskProcessor,
        shutdown: ShutdownHandle,
        drain_timeout: Duration,
    ) -> Self {
        let (request_tx, request_rx) = mpsc::channel(SHARE_QUEUE_CAPACITY);
        let supervisor = tokio::spawn(supervise(
            request_rx,
            processor,
            shutdown.clone(),
            drain_timeout,
        ));
        Self {
            request_tx,
            shutdown,
            supervisor,
        }
    }

    /// Non-blocking enqueue. Never waits for queue space.
    pub(crate) fn try_enqueue(&self, request: ShareRequest) -> Result<(), TryEnqueueError> {
        if self.shutdown.is_triggered() {
            return Err(TryEnqueueError::Closed(request));
        }
        match self.request_tx.try_send(request) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(request)) => {
                Err(TryEnqueueError::QueueFull(request))
            }
            Err(mpsc::error::TrySendError::Closed(request)) => {
                Err(TryEnqueueError::Closed(request))
            }
        }
    }

    /// Wait for the supervisor to finish. Must be called after shutdown is
    /// triggered; the supervisor's own drain is bounded by the deadline.
    pub(crate) async fn join(self) -> ExecutorSummary {
        match self.supervisor.await {
            Ok(summary) => summary,
            Err(err) => {
                tracing::error!(?err, "share executor supervisor did not report a summary");
                ExecutorSummary::default()
            }
        }
    }
}

/// Owns the queue receiver and the single in-flight task for the executor's
/// whole lifetime. A task panic is contained in its JoinHandle and can never
/// destroy the receiver or this loop.
async fn supervise(
    mut request_rx: mpsc::Receiver<ShareRequest>,
    processor: ShareTaskProcessor,
    mut shutdown: ShutdownHandle,
    drain_timeout: Duration,
) -> ExecutorSummary {
    // The request currently being executed, if the loop is exiting because
    // shutdown arrived while it was in flight.
    let mut inflight: Option<(ShareRequest, JoinHandle<()>)> = None;

    loop {
        let next = tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            next = request_rx.recv() => next,
        };
        let Some(request) = next else {
            // All senders dropped and the buffer is drained; nothing is in
            // flight because tasks are awaited inline below.
            break;
        };

        let mut task = tokio::spawn(run_task(request.clone(), Arc::clone(&processor)));
        let mut finished_here = false;
        tokio::select! {
            biased;
            _ = shutdown.wait() => {
                inflight = Some((request, task));
            }
            finished = &mut task => {
                log_task_finished(&request, finished);
                finished_here = true;
            }
        }
        if !finished_here {
            break;
        }
    }

    let summary = drain_after_shutdown(&mut request_rx, inflight, drain_timeout).await;
    if shutdown.is_triggered() {
        tracing::info!(
            dropped_queued = summary.dropped_queued,
            aborted_inflight = summary.aborted_inflight,
            "share executor stopped after shutdown signal"
        );
    } else {
        tracing::info!("share executor stopped: all senders dropped and queue drained");
    }
    summary
}

/// Shutdown drain: drop every queued request with a per-item warn log, then
/// wait at most `drain_timeout` for the in-flight task and abort it on
/// timeout. The aborted task is reaped so nothing keeps running in the
/// background after this returns.
async fn drain_after_shutdown(
    request_rx: &mut mpsc::Receiver<ShareRequest>,
    inflight: Option<(ShareRequest, JoinHandle<()>)>,
    drain_timeout: Duration,
) -> ExecutorSummary {
    let dropped = drop_queued(request_rx);
    let mut summary = ExecutorSummary {
        dropped_queued: dropped.len(),
        aborted_inflight: false,
    };

    let Some((request, mut task)) = inflight else {
        return summary;
    };

    match tokio::time::timeout(drain_timeout, &mut task).await {
        Ok(finished) => {
            log_task_finished(&request, finished);
        }
        Err(_elapsed) => {
            summary.aborted_inflight = true;
            tracing::error!(
                chat_id = request.chat_id,
                tweet_id = %request.tweet_id,
                stage = "executing",
                reason = "shutdown_timeout",
                "share task exceeded the shutdown drain deadline; aborting in-flight task"
            );
            task.abort();
            // Reap the aborted task so it cannot keep running in the
            // background after the executor reports itself stopped.
            if let Err(join_err) = task.await {
                if !join_err.is_cancelled() {
                    tracing::warn!(
                        ?join_err,
                        "aborted share task reported an unexpected join error"
                    );
                }
            }
        }
    }
    summary
}

/// Remove every buffered-but-not-started request, logging one structured warn
/// line per request. Returns the dropped requests.
fn drop_queued(request_rx: &mut mpsc::Receiver<ShareRequest>) -> Vec<ShareRequest> {
    let mut dropped = Vec::new();
    while let Ok(request) = request_rx.try_recv() {
        tracing::warn!(
            chat_id = request.chat_id,
            tweet_id = %request.tweet_id,
            stage = "queued",
            reason = "shutdown_dropped",
            "share request dropped: executor is shutting down before processing"
        );
        dropped.push(request);
    }
    dropped
}

/// Execute one request. Ordinary errors are logged and swallowed here: a
/// failed task must not stop the supervisor. A panicking processor surfaces
/// as a JoinError in the supervisor instead.
async fn run_task(request: ShareRequest, processor: ShareTaskProcessor) {
    match processor(request.clone()).await {
        Ok(()) => tracing::info!(
            chat_id = request.chat_id,
            tweet_id = %request.tweet_id,
            status = "completed",
            "share task processed"
        ),
        Err(err) => tracing::warn!(
            chat_id = request.chat_id,
            tweet_id = %request.tweet_id,
            status = "failed",
            error = %err,
            "share task failed"
        ),
    }
}

fn log_task_finished(request: &ShareRequest, finished: Result<(), JoinError>) {
    match finished {
        Ok(()) => {}
        Err(join_err) if join_err.is_panic() => {
            let panic_message = panic_message(&join_err.into_panic());
            tracing::error!(
                chat_id = request.chat_id,
                tweet_id = %request.tweet_id,
                stage = "executing",
                status = "panicked",
                panic = %panic_message,
                "share task panicked; supervisor continues with the next task"
            );
        }
        Err(join_err) => {
            tracing::warn!(
                chat_id = request.chat_id,
                tweet_id = %request.tweet_id,
                stage = "executing",
                status = "cancelled",
                error = %join_err,
                "share task was cancelled before completion"
            );
        }
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Production processor for Stage 4A. The real Share pipeline (database
/// lookup, X fetch, persistence, card send) lands in Stage 4B. This must
/// never pretend success: every accepted request is reported as explicitly
/// not processed via the executor's `failed` outcome log.
pub(crate) fn pipeline_disabled_processor() -> ShareTaskProcessor {
    Arc::new(move |_request: ShareRequest| {
        Box::pin(async move {
            Err(anyhow::anyhow!(
                "share pipeline is not enabled yet (stage 4b pending); request was accepted but not processed"
            ))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutorSummary, ShareExecutor, ShareRequest, ShareTaskProcessor, TryEnqueueError,
        SHARE_QUEUE_CAPACITY,
    };
    use crate::shutdown::ShutdownToken;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    fn request_with_id(id: u64) -> ShareRequest {
        ShareRequest {
            chat_id: 7,
            tweet_id: id.to_string(),
            canonical_url: format!("https://x.com/i/status/{id}"),
        }
    }

    fn spawn_with(
        processor: ShareTaskProcessor,
        drain_timeout: Duration,
    ) -> (ShareExecutor, ShutdownToken) {
        let token = ShutdownToken::new();
        let executor =
            ShareExecutor::start_with_drain_timeout(processor, token.handle(), drain_timeout);
        (executor, token)
    }

    /// Processor that records executed ids in order, with optional hooks:
    /// `hold_id` waits on `release` before finishing, `panic_id` panics
    /// before recording any progress, `error_id` returns an error after
    /// recording, and other tasks briefly sleep so overlap would be
    /// observable.
    fn scripted_processor(
        order: Arc<Mutex<Vec<u64>>>,
        hold_id: Option<u64>,
        release: Arc<Notify>,
        started: Arc<Notify>,
        panic_id: Option<u64>,
        error_id: Option<u64>,
    ) -> ShareTaskProcessor {
        Arc::new(move |request| {
            let order = Arc::clone(&order);
            let release = Arc::clone(&release);
            let started = Arc::clone(&started);
            Box::pin(async move {
                let id = request
                    .tweet_id
                    .parse::<u64>()
                    .map_err(|_| anyhow::anyhow!("non-numeric tweet id"))?;
                if Some(id) == hold_id {
                    started.notify_one();
                    release.notified().await;
                } else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                if Some(id) == panic_id {
                    panic!("boom from task {id}");
                }
                order.lock().expect("order lock").push(id);
                if Some(id) == error_id {
                    anyhow::bail!("task {id} failed");
                }
                Ok(())
            })
        })
    }

    async fn wait_until(deadline: Duration, mut condition: impl FnMut() -> bool) {
        timeout(deadline, async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("condition should become true within the deadline");
    }

    #[tokio::test]
    async fn single_task_is_executed() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let processor = scripted_processor(
            Arc::clone(&order),
            None,
            Arc::clone(&release),
            Arc::clone(&started),
            None,
            None,
        );
        let (executor, token) = spawn_with(processor, Duration::from_secs(2));

        assert_eq!(executor.try_enqueue(request_with_id(100)), Ok(()));
        wait_until(Duration::from_secs(2), || {
            !order.lock().expect("order lock").is_empty()
        })
        .await;
        assert_eq!(*order.lock().expect("order lock"), vec![100]);

        token.trigger();
        let summary = executor.join().await;
        assert_eq!(summary, ExecutorSummary::default());
    }

    #[tokio::test]
    async fn tasks_execute_in_fifo_order() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let processor = scripted_processor(
            Arc::clone(&order),
            None,
            Arc::clone(&release),
            Arc::clone(&started),
            None,
            None,
        );
        let (executor, token) = spawn_with(processor, Duration::from_secs(5));

        for id in 1..=5u64 {
            assert_eq!(executor.try_enqueue(request_with_id(id)), Ok(()));
        }
        wait_until(Duration::from_secs(5), || {
            order.lock().expect("order lock").len() == 5
        })
        .await;
        assert_eq!(*order.lock().expect("order lock"), vec![1, 2, 3, 4, 5]);

        token.trigger();
        let summary = executor.join().await;
        assert_eq!(summary, ExecutorSummary::default());
    }

    #[tokio::test]
    async fn only_one_task_executes_at_a_time() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let processor: ShareTaskProcessor = {
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            let completed = Arc::clone(&completed);
            Arc::new(move |_request| {
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let completed = Arc::clone(&completed);
                Box::pin(async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    completed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })
        };
        let (executor, token) = spawn_with(processor, Duration::from_secs(5));

        for id in 1..=4u64 {
            assert_eq!(executor.try_enqueue(request_with_id(id)), Ok(()));
        }
        wait_until(Duration::from_secs(5), || {
            completed.load(Ordering::SeqCst) == 4
        })
        .await;
        assert_eq!(
            max_active.load(Ordering::SeqCst),
            1,
            "worker concurrency must never exceed one"
        );

        token.trigger();
        executor.join().await;
    }

    #[tokio::test]
    async fn enqueue_returns_immediately_when_queue_is_full() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        // The first task holds the worker until released, so nothing else is
        // taken from the buffer during the test.
        let processor = scripted_processor(
            Arc::clone(&order),
            Some(1),
            Arc::clone(&release),
            Arc::clone(&started),
            None,
            None,
        );
        let (executor, token) = spawn_with(processor, Duration::from_secs(2));

        assert_eq!(executor.try_enqueue(request_with_id(1)), Ok(()));
        timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("first task should start");

        // The first task left the buffer, so the buffer accepts exactly
        // SHARE_QUEUE_CAPACITY more requests.
        for id in 2..(2 + SHARE_QUEUE_CAPACITY as u64) {
            assert_eq!(executor.try_enqueue(request_with_id(id)), Ok(()));
        }

        // Non-blocking by construction: this call cannot wait for space.
        match executor.try_enqueue(request_with_id(99)) {
            Err(TryEnqueueError::QueueFull(request)) => {
                assert_eq!(request.tweet_id, "99");
                assert_eq!(request.chat_id, 7);
            }
            other => panic!("expected QueueFull, got {other:?}"),
        }

        release.notify_one();
        wait_until(Duration::from_secs(2), || {
            order.lock().expect("order lock").len() == SHARE_QUEUE_CAPACITY + 1
        })
        .await;

        token.trigger();
        let summary = executor.join().await;
        assert_eq!(summary, ExecutorSummary::default());
    }

    #[tokio::test]
    async fn enqueue_rejected_after_executor_shutdown() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let processor = scripted_processor(
            Arc::clone(&order),
            None,
            Arc::clone(&release),
            Arc::clone(&started),
            None,
            None,
        );
        let (executor, token) = spawn_with(processor, Duration::from_secs(2));

        token.trigger();
        match executor.try_enqueue(request_with_id(1)) {
            Err(TryEnqueueError::Closed(request)) => assert_eq!(request.tweet_id, "1"),
            other => panic!("expected Closed, got {other:?}"),
        }
        let summary = executor.join().await;
        assert_eq!(summary, ExecutorSummary::default());
        assert!(order.lock().expect("order lock").is_empty());
    }

    #[tokio::test]
    async fn task_error_does_not_block_following_tasks() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let processor = scripted_processor(
            Arc::clone(&order),
            None,
            Arc::clone(&release),
            Arc::clone(&started),
            None,
            Some(1),
        );
        let (executor, token) = spawn_with(processor, Duration::from_secs(5));

        for id in 1..=3u64 {
            assert_eq!(executor.try_enqueue(request_with_id(id)), Ok(()));
        }
        wait_until(Duration::from_secs(5), || {
            order.lock().expect("order lock").len() == 3
        })
        .await;
        assert_eq!(*order.lock().expect("order lock"), vec![1, 2, 3]);

        token.trigger();
        let summary = executor.join().await;
        assert_eq!(summary, ExecutorSummary::default());
    }

    #[tokio::test]
    async fn task_continues_after_panic() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let processor = scripted_processor(
            Arc::clone(&order),
            None,
            Arc::clone(&release),
            Arc::clone(&started),
            Some(1),
            None,
        );
        let (executor, token) = spawn_with(processor, Duration::from_secs(5));

        for id in 1..=3u64 {
            assert_eq!(executor.try_enqueue(request_with_id(id)), Ok(()));
        }
        wait_until(Duration::from_secs(5), || {
            order.lock().expect("order lock").len() == 2
        })
        .await;
        assert_eq!(
            *order.lock().expect("order lock"),
            vec![2, 3],
            "tasks after the panicking task must still execute in FIFO order"
        );

        token.trigger();
        let summary = executor.join().await;
        assert_eq!(summary, ExecutorSummary::default());
    }

    #[tokio::test]
    async fn queue_capacity_recovers_after_panic() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        // Task 1 panics after we release it; tasks 2..=N proceed normally.
        let processor = scripted_processor(
            Arc::clone(&order),
            Some(1),
            Arc::clone(&release),
            Arc::clone(&started),
            Some(1),
            None,
        );
        let (executor, token) = spawn_with(processor, Duration::from_secs(5));

        assert_eq!(executor.try_enqueue(request_with_id(1)), Ok(()));
        timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("first task should start");

        // With task 1 held outside the buffer, exactly SHARE_QUEUE_CAPACITY
        // requests fit; the boundary is the same as before the panic.
        for id in 2..(2 + SHARE_QUEUE_CAPACITY as u64) {
            assert_eq!(executor.try_enqueue(request_with_id(id)), Ok(()));
        }
        match executor.try_enqueue(request_with_id(99)) {
            Err(TryEnqueueError::QueueFull(_)) => {}
            other => panic!("expected QueueFull, got {other:?}"),
        }

        // Release the panicking task; every buffered request must still run.
        release.notify_one();
        wait_until(Duration::from_secs(5), || {
            order.lock().expect("order lock").len() == SHARE_QUEUE_CAPACITY
        })
        .await;
        assert_eq!(
            *order.lock().expect("order lock"),
            (2..(2 + SHARE_QUEUE_CAPACITY as u64)).collect::<Vec<u64>>()
        );

        // The queue is drained again, so new requests are accepted.
        assert_eq!(executor.try_enqueue(request_with_id(50)), Ok(()));
        wait_until(Duration::from_secs(5), || {
            order.lock().expect("order lock").last() == Some(&50)
        })
        .await;

        token.trigger();
        let summary = executor.join().await;
        assert_eq!(summary, ExecutorSummary::default());
    }

    #[tokio::test]
    async fn executor_survives_task_panic() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let processor = scripted_processor(
            Arc::clone(&order),
            None,
            Arc::clone(&release),
            Arc::clone(&started),
            Some(1),
            None,
        );
        let (executor, token) = spawn_with(processor, Duration::from_secs(5));

        assert_eq!(executor.try_enqueue(request_with_id(1)), Ok(()));
        // The executor is still alive right after the panic was triggered.
        assert_eq!(executor.try_enqueue(request_with_id(2)), Ok(()));
        wait_until(Duration::from_secs(5), || {
            order.lock().expect("order lock").len() == 1
        })
        .await;
        assert_eq!(
            *order.lock().expect("order lock"),
            vec![2],
            "the executor must keep serving new requests after a panic"
        );

        token.trigger();
        let summary = executor.join().await;
        // No supervisor crash, nothing dropped: the panic was contained.
        assert_eq!(summary, ExecutorSummary::default());
    }

    #[tokio::test]
    async fn inflight_task_completes_within_shutdown_deadline() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let processor = scripted_processor(
            Arc::clone(&order),
            Some(1),
            Arc::clone(&release),
            Arc::clone(&started),
            None,
            None,
        );
        let (executor, token) = spawn_with(processor, Duration::from_secs(2));

        assert_eq!(executor.try_enqueue(request_with_id(1)), Ok(()));
        timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("first task should start");

        token.trigger();
        release.notify_one();
        let summary = executor.join().await;
        assert_eq!(summary, ExecutorSummary::default());
        assert_eq!(*order.lock().expect("order lock"), vec![1]);
    }

    #[tokio::test]
    async fn queued_tasks_are_dropped_on_shutdown() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let processor = scripted_processor(
            Arc::clone(&order),
            Some(1),
            Arc::clone(&release),
            Arc::clone(&started),
            None,
            None,
        );
        let (executor, token) = spawn_with(processor, Duration::from_secs(2));

        assert_eq!(executor.try_enqueue(request_with_id(1)), Ok(()));
        timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("first task should start");
        for id in 2..=4u64 {
            assert_eq!(executor.try_enqueue(request_with_id(id)), Ok(()));
        }

        // Shutdown: the in-flight task finishes inside the deadline, but every
        // queued request is dropped without being executed.
        token.trigger();
        release.notify_one();
        let summary = executor.join().await;
        assert_eq!(summary.dropped_queued, 3);
        assert!(!summary.aborted_inflight);
        assert_eq!(
            *order.lock().expect("order lock"),
            vec![1],
            "queued tasks must not execute after shutdown"
        );
    }

    #[tokio::test]
    async fn inflight_task_is_aborted_after_drain_deadline() {
        let completed = Arc::new(AtomicBool::new(false));
        let completed_for_task = Arc::clone(&completed);
        let started = Arc::new(Notify::new());
        let started_for_task = Arc::clone(&started);
        let processor: ShareTaskProcessor = Arc::new(move |_request| {
            let completed = Arc::clone(&completed_for_task);
            let started = Arc::clone(&started_for_task);
            Box::pin(async move {
                started.notify_one();
                // Far beyond the 200ms drain deadline of this test.
                tokio::time::sleep(Duration::from_secs(10)).await;
                completed.store(true, Ordering::SeqCst);
                Ok(())
            })
        });
        let (executor, token) = spawn_with(processor, Duration::from_millis(200));

        assert_eq!(executor.try_enqueue(request_with_id(1)), Ok(()));
        timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("task should start");

        token.trigger();
        let summary = executor.join().await;
        assert!(summary.aborted_inflight, "overdue task must be aborted");
        assert_eq!(summary.dropped_queued, 0);

        // The aborted task was reaped: nothing keeps running in the background.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !completed.load(Ordering::SeqCst),
            "aborted task must not make progress after the executor stopped"
        );
    }
}
