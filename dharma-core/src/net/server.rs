use crate::config;
use crate::error::DharmaError;
use crate::identity::IdentityState;
use crate::net::handshake;
use crate::net::peer::verify_peer_identity;
use crate::fabric::types::AdStore;
use crate::net::policy::{OverlayAccess, OverlayPolicy};
use crate::net::trust::PeerPolicy;
use crate::net::sync::{sync_loop_with, SyncOptions};
use crate::store::index::FrontierIndex;
use crate::store::Store;
use crate::metrics;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DEFAULT_MAX_CONNECTIONS: usize = 256;
const ACCEPT_POLL_DELAY: Duration = Duration::from_millis(10);

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
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            relay: false,
            ad_store: Arc::new(Mutex::new(AdStore::new())),
            verbose: false,
            trace: None,
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

pub fn listen(addr: &str, identity: IdentityState, store: Store) -> Result<(), DharmaError> {
    listen_with_options(addr, identity, store, ServerOptions::default())
}

pub fn listen_with_options(
    addr: &str,
    identity: IdentityState,
    store: Store,
    options: ServerOptions,
) -> Result<(), DharmaError> {
    let listener = TcpListener::bind(addr)?;
    println!("Listening on {addr}");
    listen_loop(listener, identity, store, options, None)
}

pub fn listen_with_shutdown(
    listener: TcpListener,
    identity: IdentityState,
    store: Store,
    options: ServerOptions,
    shutdown: Arc<AtomicBool>,
) -> Result<(), DharmaError> {
    listen_loop(listener, identity, store, options, Some(shutdown))
}

fn listen_loop(
    listener: TcpListener,
    identity: IdentityState,
    store: Store,
    options: ServerOptions,
    shutdown: Option<Arc<AtomicBool>>,
) -> Result<(), DharmaError> {
    listener.set_nonblocking(true)?;
    let max_connections = effective_max_connections(options.max_connections);
    let worker_count = default_worker_count(max_connections);
    let pool = ConnectionPool::new(worker_count, max_connections)?;
    let limiter = Arc::new(ConnectionLimiter::new(max_connections));

    loop {
        if shutdown_requested(&shutdown) {
            break;
        }
        if !limiter.has_capacity() {
            thread::sleep(ACCEPT_POLL_DELAY);
            continue;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                let Some(permit) = limiter.try_acquire() else {
                    continue;
                };
                let task = ConnectionTask {
                    stream,
                    identity: identity.clone(),
                    store: store.clone(),
                    options: options.clone(),
                    _permit: permit,
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
                        TrySendError::Disconnected(_) => {
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
            Err(err) => {
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    thread::sleep(ACCEPT_POLL_DELAY);
                    continue;
                }
                eprintln!("Accept error: {err}");
            }
        }
    }
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
    _permit: ConnectionPermit,
}

struct ConnectionPool {
    sender: SyncSender<ConnectionTask>,
}

impl ConnectionPool {
    fn new(worker_count: usize, queue_capacity: usize) -> Result<Self, DharmaError> {
        let (sender, receiver) = mpsc::sync_channel(queue_capacity.max(1));
        let receiver = Arc::new(Mutex::new(receiver));
        let target_workers = worker_count.max(1);
        let mut started_workers = 0usize;
        for idx in 0..target_workers {
            let receiver = Arc::clone(&receiver);
            let builder = thread::Builder::new().name(format!("dharma-net-worker-{idx}"));
            match builder.spawn(move || worker_loop(receiver)) {
                Ok(_) => {
                    started_workers += 1;
                }
                Err(err) => {
                    log_server_event(
                        "error",
                        "worker_spawn_failed",
                        "failed to spawn connection worker thread",
                        &[
                            ("worker_index", idx.to_string()),
                            ("error", err.to_string()),
                        ],
                    );
                }
            }
        }
        if started_workers == 0 {
            return Err(DharmaError::Network(
                "failed to spawn connection worker threads".to_string(),
            ));
        }
        if started_workers < target_workers {
            log_server_event(
                "warn",
                "worker_pool_degraded",
                "started fewer worker threads than requested",
                &[
                    ("requested_workers", target_workers.to_string()),
                    ("started_workers", started_workers.to_string()),
                ],
            );
        }
        Ok(Self { sender })
    }

    fn submit(&self, task: ConnectionTask) -> Result<(), TrySendError<ConnectionTask>> {
        self.sender.try_send(task)
    }
}

fn worker_loop(receiver: Arc<Mutex<Receiver<ConnectionTask>>>) {
    loop {
        let task = {
            let guard = match receiver.lock() {
                Ok(guard) => guard,
                Err(_) => return,
            };
            guard.recv()
        };
        let task = match task {
            Ok(task) => task,
            Err(_) => return,
        };
        let ConnectionTask {
            stream,
            identity,
            store,
            options,
            _permit,
        } = task;
        if let Err(err) = handle_connection(stream, identity, store, options) {
            if !is_disconnect(&err) {
                eprintln!("Connection error: {err}");
            }
        }
        drop(_permit);
    }
}

struct ConnectionLimiter {
    in_flight: AtomicUsize,
    max_connections: usize,
}

impl ConnectionLimiter {
    fn new(max_connections: usize) -> Self {
        Self {
            in_flight: AtomicUsize::new(0),
            max_connections: effective_max_connections(max_connections),
        }
    }

    fn has_capacity(&self) -> bool {
        self.in_flight.load(Ordering::Relaxed) < self.max_connections
    }

    fn try_acquire(self: &Arc<Self>) -> Option<ConnectionPermit> {
        let mut current = self.in_flight.load(Ordering::Relaxed);
        loop {
            if current >= self.max_connections {
                return None;
            }
            match self.in_flight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(ConnectionPermit {
                        limiter: Arc::clone(self),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn release(&self) {
        let _ = self
            .in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |value| {
                value.checked_sub(1)
            });
    }
}

struct ConnectionPermit {
    limiter: Arc<ConnectionLimiter>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.limiter.release();
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
    let (session, peer) = handshake::server_handshake(&mut stream, &identity)?;
    let claims = verify_peer_identity(store.env(), &peer.subject, &peer.public_key)?;
    let identity_verified = claims.is_some();
    if peer.verified && identity_verified {
        println!("Handshake complete. Auth verified.");
    } else {
        println!("Handshake complete. Auth unverified.");
    }
    let peer_policy = PeerPolicy::load(store.root());
    if !peer_policy.allows(peer.subject, peer.public_key) {
        return Err(DharmaError::Validation("peer denied by policy".to_string()));
    }
    let mut legacy_keys = HashMap::new();
    legacy_keys.insert(identity.subject_id, identity.subject_key);
    let mut keys = crate::keys::Keyring::from_subject_keys(&legacy_keys);
    keys.insert_hpke_secret(identity.public_key, identity.noise_sk);
    let mut index = FrontierIndex::build(&store, &keys)?;
    let policy = OverlayPolicy::load(store.root());
    let claims = claims.unwrap_or_default();
    let access = OverlayAccess::new(
        &policy,
        Some(peer.subject),
        peer.verified && identity_verified,
        &claims,
    );
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
    let mut payload = format!(
        "{{\"level\":\"{}\",\"component\":\"net.server\",\"event\":\"{}\",\"message\":\"{}\"",
        json_escape(level),
        json_escape(event),
        json_escape(message),
    );
    for (key, value) in fields {
        payload.push_str(",\"");
        payload.push_str(&json_escape(key));
        payload.push_str("\":\"");
        payload.push_str(&json_escape(value));
        payload.push('"');
    }
    payload.push('}');
    eprintln!("{payload}");
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn disconnect_errors_are_suppressed() {
        assert!(is_disconnect(&DharmaError::Network("unexpected eof".to_string())));
        assert!(is_disconnect(&DharmaError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset"
        ))));
        assert!(is_disconnect(&DharmaError::Cbor(
            "failed to fill whole buffer".to_string()
        )));
        assert!(!is_disconnect(&DharmaError::Validation(
            "bad".to_string()
        )));
    }

    #[test]
    fn max_connections_is_clamped_to_at_least_one() {
        assert_eq!(effective_max_connections(0), 1);
        assert_eq!(effective_max_connections(64), 64);
    }

    #[test]
    fn limiter_blocks_acquire_when_capacity_is_reached() {
        let limiter = Arc::new(ConnectionLimiter::new(2));
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
}
