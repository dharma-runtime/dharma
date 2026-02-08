use crate::config;
use crate::error::DharmaError;
use crate::fabric::types::AdStore;
use crate::identity::IdentityState;
use crate::metrics;
use crate::net::handshake;
use crate::net::peer::verify_peer_identity;
use crate::net::policy::{OverlayAccess, OverlayPolicy};
use crate::net::sync::{sync_loop_with, SyncOptions};
use crate::net::trust::PeerPolicy;
use crate::store::index::FrontierIndex;
use crate::store::Store;
use std::collections::HashMap;
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration, Instant};
use tracing::{debug, error, info, info_span, warn};

const DEFAULT_MAX_CONNECTIONS: usize = 256;
const ACCEPT_POLL_DELAY: Duration = Duration::from_millis(100);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

struct ConnectionGauge;

impl ConnectionGauge {
    fn new() -> Self {
        metrics::peers_connected_inc();
        Self
    }
}

impl Drop for ConnectionGauge {
    fn drop(&mut self) {
        metrics::peers_connected_dec();
    }
}

#[derive(Clone)]
pub struct ServerOptions {
    pub relay: bool,
    pub ad_store: Arc<Mutex<AdStore>>,
    pub verbose: bool,
    pub trace: Option<Arc<Mutex<Vec<String>>>>,
    pub max_connections: usize,
    pub shutdown_timeout: Duration,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            relay: false,
            ad_store: Arc::new(Mutex::new(AdStore::new())),
            verbose: false,
            trace: None,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

pub async fn listen(addr: &str, identity: IdentityState, store: Store) -> Result<(), DharmaError> {
    listen_with_options(addr, identity, store, ServerOptions::default()).await
}

pub async fn listen_with_options(
    addr: &str,
    identity: IdentityState,
    store: Store,
    options: ServerOptions,
) -> Result<(), DharmaError> {
    let listener = TcpListener::bind(addr).await?;
    info!(listen_addr = %addr, "server listening");
    listen_loop(listener, identity, store, options, None).await
}

pub async fn listen_with_shutdown(
    listener: TcpListener,
    identity: IdentityState,
    store: Store,
    options: ServerOptions,
    shutdown: Arc<AtomicBool>,
) -> Result<(), DharmaError> {
    listen_loop(listener, identity, store, options, Some(shutdown)).await
}

async fn listen_loop(
    listener: TcpListener,
    identity: IdentityState,
    store: Store,
    options: ServerOptions,
    shutdown: Option<Arc<AtomicBool>>,
) -> Result<(), DharmaError> {
    let max_connections = effective_max_connections(options.max_connections);
    let worker_count = default_worker_count(max_connections);
    let pool = ConnectionPool::new(worker_count, max_connections);
    let limiter = ConnectionLimiter::new(max_connections);
    let connections = ConnectionTracker::new();

    loop {
        if shutdown_requested(&shutdown) {
            break;
        }

        let accepted = if shutdown.is_some() {
            match timeout(ACCEPT_POLL_DELAY, listener.accept()).await {
                Ok(result) => Some(result),
                Err(_) => None,
            }
        } else {
            Some(listener.accept().await)
        };

        let Some(accept_result) = accepted else {
            continue;
        };

        let (stream, _) = match accept_result {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = ?err, "accept error");
                continue;
            }
        };

        let Some(permit) = limiter.try_acquire() else {
            log_server_event(
                "warn",
                "connection_limit_reached",
                "dropping connection because max connections is reached",
                &[("max_connections", max_connections.to_string())],
            );
            continue;
        };

        let stream = match stream.into_std() {
            Ok(stream) => stream,
            Err(err) => {
                warn!(error = ?err, "failed to convert accepted socket to std stream");
                continue;
            }
        };
        if let Err(err) = stream.set_nonblocking(false) {
            warn!(error = ?err, "failed to switch accepted socket to blocking mode");
            continue;
        }

        let tracked_connection = connections.track(&stream);
        let task = ConnectionTask {
            stream,
            identity: identity.clone(),
            store: store.clone(),
            options: options.clone(),
            _permit: permit,
            _connection: tracked_connection,
        };

        if let Err(err) = pool.submit(task) {
            match err {
                TrySendError::Full(_) => {
                    log_server_event(
                        "warn",
                        "connection_backlog_full",
                        "dropping connection because worker backlog is full",
                        &[("max_connections", max_connections.to_string())],
                    );
                }
                TrySendError::Closed(_) => {
                    log_server_event(
                        "error",
                        "worker_pool_unavailable",
                        "dropping connection because worker pool is unavailable",
                        &[("max_connections", max_connections.to_string())],
                    );
                }
            }
        }
    }

    connections.close_all();
    pool.shutdown(options.shutdown_timeout).await;
    Ok(())
}

fn shutdown_requested(shutdown: &Option<Arc<AtomicBool>>) -> bool {
    shutdown
        .as_ref()
        .map(|flag| flag.load(Ordering::SeqCst))
        .unwrap_or(false)
}

fn effective_max_connections(value: usize) -> usize {
    value.max(1)
}

fn default_worker_count(max_connections: usize) -> usize {
    let suggested = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    suggested.clamp(1, max_connections)
}

struct ConnectionTask {
    stream: TcpStream,
    identity: IdentityState,
    store: Store,
    options: ServerOptions,
    _permit: OwnedSemaphorePermit,
    _connection: Option<TrackedConnection>,
}

struct ConnectionPool {
    sender: mpsc::Sender<ConnectionTask>,
    workers: Vec<JoinHandle<()>>,
}

impl ConnectionPool {
    fn new(worker_count: usize, queue_capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(queue_capacity.max(1));
        let receiver = Arc::new(AsyncMutex::new(receiver));
        let target_workers = worker_count.max(1);
        let mut workers = Vec::with_capacity(target_workers);
        for idx in 0..target_workers {
            let receiver = Arc::clone(&receiver);
            workers.push(tokio::spawn(async move {
                worker_loop(idx, receiver).await;
            }));
        }
        Self { sender, workers }
    }

    fn submit(&self, task: ConnectionTask) -> Result<(), TrySendError<ConnectionTask>> {
        self.sender.try_send(task)
    }

    async fn shutdown(self, timeout_duration: Duration) {
        drop(self.sender);
        let deadline = Instant::now() + timeout_duration;
        for mut worker in self.workers {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                worker.abort();
                continue;
            }
            match timeout(remaining, &mut worker).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    warn!(error = ?err, "connection worker join failed");
                }
                Err(_) => {
                    warn!("worker shutdown timeout reached, aborting");
                    worker.abort();
                }
            }
        }
    }
}

async fn worker_loop(_idx: usize, receiver: Arc<AsyncMutex<mpsc::Receiver<ConnectionTask>>>) {
    loop {
        let task = {
            let mut guard = receiver.lock().await;
            guard.recv().await
        };
        let Some(task) = task else {
            return;
        };

        let ConnectionTask {
            stream,
            identity,
            store,
            options,
            _permit,
            _connection,
        } = task;

        let peer_addr = stream.peer_addr().ok().map(|addr| addr.to_string());
        let peer_addr_field = peer_addr.as_deref().unwrap_or("unknown").to_string();
        let span = info_span!("server_connection", peer_addr = %peer_addr_field);
        let result = tokio::task::spawn_blocking(move || {
            let _entered = span.enter();
            let _permit = _permit;
            let _connection = _connection;
            handle_connection(stream, identity, store, options)
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                if !is_disconnect(&err) {
                    warn!(peer_addr = %peer_addr_field, error = %err, "connection error");
                }
            }
            Err(err) => {
                warn!(peer_addr = %peer_addr_field, error = ?err, "connection task panicked");
            }
        }
    }
}

#[derive(Clone)]
struct ConnectionLimiter {
    permits: Arc<Semaphore>,
}

impl ConnectionLimiter {
    fn new(max_connections: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(effective_max_connections(max_connections))),
        }
    }

    fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.permits.clone().try_acquire_owned().ok()
    }
}

#[derive(Clone)]
struct ConnectionTracker {
    inner: Arc<ConnectionTrackerInner>,
}

struct ConnectionTrackerInner {
    next_id: AtomicU64,
    streams: Mutex<HashMap<u64, TcpStream>>,
}

impl ConnectionTracker {
    fn new() -> Self {
        Self {
            inner: Arc::new(ConnectionTrackerInner {
                next_id: AtomicU64::new(1),
                streams: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn track(&self, stream: &TcpStream) -> Option<TrackedConnection> {
        let cloned = match stream.try_clone() {
            Ok(cloned) => cloned,
            Err(err) => {
                warn!(
                    error = ?err,
                    "failed to clone connection stream for shutdown tracking"
                );
                return None;
            }
        };
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut guard) = self.inner.streams.lock() {
            guard.insert(id, cloned);
        }
        Some(TrackedConnection {
            id,
            tracker: self.clone(),
        })
    }

    fn close_all(&self) {
        let mut to_close = Vec::new();
        if let Ok(mut guard) = self.inner.streams.lock() {
            for (_, stream) in guard.drain() {
                to_close.push(stream);
            }
        }
        for stream in to_close {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    fn untrack(&self, id: u64) {
        if let Ok(mut guard) = self.inner.streams.lock() {
            guard.remove(&id);
        }
    }
}

struct TrackedConnection {
    id: u64,
    tracker: ConnectionTracker,
}

impl Drop for TrackedConnection {
    fn drop(&mut self) {
        self.tracker.untrack(self.id);
    }
}

fn handle_connection(
    mut stream: TcpStream,
    identity: IdentityState,
    store: Store,
    options: ServerOptions,
) -> Result<(), DharmaError> {
    let _gauge = ConnectionGauge::new();
    if let Ok(root) = std::env::current_dir() {
        if let Ok(cfg) = config::Config::load(&root) {
            cfg.apply_timeouts(&stream);
        }
    }
    let (session, mut peer) = handshake::server_handshake(&mut stream, &identity)?;
    let claims = verify_peer_identity(store.env(), &peer.subject, &peer.public_key)?;
    peer.verified = claims.is_some();
    info!(verified = peer.verified, "handshake complete");
    if options.verbose {
        debug!(
            peer_id = %peer.public_key.to_hex(),
            subject_id = %peer.subject.to_hex(),
            verified = peer.verified,
            "handshake identity"
        );
    }
    let peer_policy = PeerPolicy::load(store.root());
    if !peer_policy.allows(peer.subject, peer.public_key) {
        warn!(
            peer_id = %peer.public_key.to_hex(),
            subject_id = %peer.subject.to_hex(),
            "peer denied by policy"
        );
        return Err(DharmaError::Validation("peer denied by policy".to_string()));
    }
    let mut legacy_keys = HashMap::new();
    legacy_keys.insert(identity.subject_id, identity.subject_key);
    let mut keys = crate::keys::Keyring::from_subject_keys(&legacy_keys);
    keys.insert_hpke_secret(identity.public_key, identity.noise_sk);
    let mut index = FrontierIndex::build(&store, &keys)?;
    let policy = OverlayPolicy::load(store.root());
    let claims = claims.unwrap_or_default();
    let access = OverlayAccess::new(&policy, Some(peer.subject), peer.verified, &claims);
    if options.relay {
        let mut relay_keys = crate::keys::Keyring::new();
        sync_loop_with(
            &mut stream,
            session,
            &store,
            &mut index,
            &mut relay_keys,
            &identity,
            &access,
            SyncOptions {
                relay: true,
                ad_store: Some(options.ad_store),
                local_subs: None,
                verbose: options.verbose,
                exit_on_idle: false,
                trace: options.trace.clone(),
            },
        )?;
    } else {
        sync_loop_with(
            &mut stream,
            session,
            &store,
            &mut index,
            &mut keys,
            &identity,
            &access,
            SyncOptions {
                relay: false,
                ad_store: Some(options.ad_store),
                local_subs: None,
                verbose: options.verbose,
                exit_on_idle: false,
                trace: options.trace.clone(),
            },
        )?;
    }
    Ok(())
}

fn is_disconnect(err: &DharmaError) -> bool {
    match err {
        DharmaError::Network(_) => true,
        DharmaError::Io(io_err) => matches!(
            io_err.kind(),
            std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::NotConnected
        ),
        DharmaError::Cbor(msg) => msg.contains("failed to fill whole buffer"),
        _ => false,
    }
}

fn log_server_event(level: &str, event: &str, message: &str, fields: &[(&str, String)]) {
    match level {
        "error" => {
            error!(
                component = "net.server",
                event = %event,
                detail = %message,
                fields = ?fields,
                "server event"
            );
        }
        "warn" => {
            warn!(
                component = "net.server",
                event = %event,
                detail = %message,
                fields = ?fields,
                "server event"
            );
        }
        _ => {
            info!(
                component = "net.server",
                event = %event,
                detail = %message,
                fields = ?fields,
                "server event"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::StdEnv;
    use crate::identity_store;

    #[test]
    fn disconnect_errors_are_suppressed() {
        assert!(is_disconnect(&DharmaError::Network(
            "unexpected eof".to_string()
        )));
        assert!(is_disconnect(&DharmaError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset"
        ))));
        assert!(is_disconnect(&DharmaError::Cbor(
            "failed to fill whole buffer".to_string()
        )));
        assert!(!is_disconnect(&DharmaError::Validation("bad".to_string())));
    }

    #[test]
    fn max_connections_is_clamped_to_at_least_one() {
        assert_eq!(effective_max_connections(0), 1);
        assert_eq!(effective_max_connections(64), 64);
    }

    #[test]
    fn limiter_blocks_acquire_when_capacity_is_reached() {
        let limiter = ConnectionLimiter::new(2);
        let permit_a = limiter.try_acquire().expect("first permit");
        let permit_b = limiter.try_acquire().expect("second permit");
        assert!(limiter.try_acquire().is_none());
        drop(permit_a);
        assert!(limiter.try_acquire().is_some());
        drop(permit_b);
    }

    #[test]
    fn default_server_options_set_connection_cap() {
        assert!(ServerOptions::default().max_connections >= 1);
    }

    #[test]
    fn default_server_options_set_shutdown_timeout() {
        assert!(ServerOptions::default().shutdown_timeout >= Duration::from_secs(1));
    }

    #[test]
    fn async_server_handles_concurrent_peers_and_shutdown() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build tokio runtime");

        rt.block_on(async {
            let temp = tempfile::tempdir().expect("tempdir");
            let env = StdEnv::new(temp.path());
            identity_store::init_identity(&env, "stress", "pw")
                .expect("init identity")
                .expect("identity created");
            let identity = identity_store::load_identity(&env, "pw").expect("load identity");
            let store = Store::new(&env);

            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind listener");
            let addr = listener.local_addr().expect("local addr");
            let shutdown = Arc::new(AtomicBool::new(false));
            let server_shutdown = Arc::clone(&shutdown);
            let server = tokio::spawn(async move {
                listen_with_shutdown(
                    listener,
                    identity,
                    store,
                    ServerOptions {
                        max_connections: 8,
                        ..Default::default()
                    },
                    server_shutdown,
                )
                .await
            });

            let mut peers = Vec::new();
            for _ in 0..64 {
                peers.push(tokio::spawn(async move {
                    if let Ok(stream) = tokio::net::TcpStream::connect(addr).await {
                        drop(stream);
                    }
                }));
            }

            for peer in peers {
                let _ = peer.await;
            }

            tokio::time::sleep(Duration::from_millis(150)).await;
            shutdown.store(true, Ordering::SeqCst);

            let result = tokio::time::timeout(Duration::from_secs(3), server)
                .await
                .expect("server shutdown timed out")
                .expect("server task join failed");
            assert!(result.is_ok());
        });
    }

    #[test]
    fn async_server_shutdown_closes_hung_connections() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build tokio runtime");

        rt.block_on(async {
            let temp = tempfile::tempdir().expect("tempdir");
            let env = StdEnv::new(temp.path());
            identity_store::init_identity(&env, "hung", "pw")
                .expect("init identity")
                .expect("identity created");
            let identity = identity_store::load_identity(&env, "pw").expect("load identity");
            let store = Store::new(&env);

            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind listener");
            let addr = listener.local_addr().expect("local addr");
            let shutdown = Arc::new(AtomicBool::new(false));
            let server_shutdown = Arc::clone(&shutdown);
            let server = tokio::spawn(async move {
                listen_with_shutdown(
                    listener,
                    identity,
                    store,
                    ServerOptions {
                        max_connections: 8,
                        shutdown_timeout: Duration::from_secs(2),
                        ..Default::default()
                    },
                    server_shutdown,
                )
                .await
            });

            let _hung_peer = tokio::net::TcpStream::connect(addr)
                .await
                .expect("connect hung peer");
            tokio::time::sleep(Duration::from_millis(100)).await;
            shutdown.store(true, Ordering::SeqCst);

            let result = tokio::time::timeout(Duration::from_secs(3), server)
                .await
                .expect("server shutdown timed out")
                .expect("server task join failed");
            assert!(result.is_ok());
        });
    }
}
