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
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            relay: false,
            ad_store: Arc::new(Mutex::new(AdStore::new())),
            verbose: false,
            trace: None,
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
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                spawn_connection(stream, identity.clone(), store.clone(), options.clone());
            }
            Err(err) => {
                eprintln!("Accept error: {err}");
            }
        }
    }
    Ok(())
}

pub fn listen_with_shutdown(
    listener: TcpListener,
    identity: IdentityState,
    store: Store,
    options: ServerOptions,
    shutdown: Arc<AtomicBool>,
) -> Result<(), DharmaError> {
    listener.set_nonblocking(true)?;
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                spawn_connection(stream, identity.clone(), store.clone(), options.clone());
            }
            Err(err) => {
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
                eprintln!("Accept error: {err}");
            }
        }
    }
    Ok(())
}

fn spawn_connection(
    stream: TcpStream,
    identity: IdentityState,
    store: Store,
    options: ServerOptions,
) {
    thread::spawn(move || {
        if let Err(err) = handle_connection(stream, identity, store, options) {
            if !is_disconnect(&err) {
                eprintln!("Connection error: {err}");
            }
        }
    });
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
    if peer.verified {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
