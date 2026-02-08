use dharma_core::assertion::AssertionPlaintext;
use dharma_core::config::Config;
use dharma_core::env::StdEnv;
use dharma_core::envelope;
use dharma_core::identity::IdentityState;
use dharma_core::identity_store;
#[cfg(feature = "server")]
use dharma_core::metrics;
use dharma_core::net;
use dharma_core::store::state::list_assertions;
use dharma_core::store::Store;
use dharma_core::DharmaError;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
#[cfg(feature = "server")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
#[cfg(feature = "server")]
use tokio::net::TcpStream;
#[cfg(feature = "server")]
use tokio::task::JoinHandle;
#[cfg(feature = "server")]
use tokio::time::{timeout, Duration};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

const APP_BANNER: &str = r#"       ____                              
  ____╱ ╱ ╱_  ____ __________ ___  ____ _
 ╱ __  ╱ __ ╲╱ __ `╱ ___╱ __ `__ ╲╱ __ `╱
╱ ╱_╱ ╱ ╱ ╱ ╱ ╱_╱ ╱ ╱  ╱ ╱ ╱ ╱ ╱ ╱ ╱_╱ ╱ 
╲__,_╱_╱ ╱_╱╲__,_╱_╱  ╱_╱ ╱_╱ ╱_╱╲__,_╱  
                                         
"#;

#[cfg(feature = "server")]
const METRICS_ACCEPT_POLL_DELAY: Duration = Duration::from_millis(100);

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    init_tracing("dharma_runtime=info,dharma_core=info,warn");
    info!(banner = APP_BANNER.trim_end(), "runtime banner");
    if let Err(err) = run().await {
        error!(error = %err, "runtime exited with error");
        std::process::exit(1);
    }
}

fn init_tracing(default_directive: &str) {
    let filter = EnvFilter::try_from_env("DHARMA_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new(default_directive));
    if let Err(err) = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .compact()
        .try_init()
    {
        eprintln!("warning: tracing initialization failed: {err}");
    }
}

async fn run() -> Result<(), DharmaError> {
    let relay = std::env::args().any(|arg| arg == "--relay");
    let root = std::env::current_dir()?;
    let config = Config::load(&root)?;
    let data_dir = ensure_data_dir()?;
    let env = StdEnv::new(&data_dir);
    if identity_store::ensure_identity_present(&env).is_err() {
        return Ok(());
    }
    let identity = load_identity(&env)?;
    let head = mount_self(&env, &identity)?;
    info!(head_seq = head, "identity unlocked");
    let store = Store::new(&env);

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_signal = {
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                shutdown.store(true, Ordering::SeqCst);
                info!("shutdown signal received");
            }
        })
    };

    #[cfg(feature = "server")]
    let metrics_handle = match start_metrics_server(
        config.network.listen_port,
        store.clone(),
        Arc::clone(&shutdown),
    )
    .await
    {
        Ok(handle) => handle,
        Err(err) => {
            tracing::warn!(error = %err, "metrics disabled");
            None
        }
    };

    let addr = format!("0.0.0.0:{}", config.network.listen_port);
    let options = net::server::ServerOptions {
        relay,
        max_connections: config.network.max_connections,
        ..Default::default()
    };

    #[cfg(feature = "server")]
    {
        let listener = TcpListener::bind(&addr).await?;
        info!(listen_addr = %addr, "runtime server listening");
        let serve_result = net::server::listen_with_shutdown(
            listener,
            identity,
            store,
            options,
            Arc::clone(&shutdown),
        )
        .await;

        shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = metrics_handle {
            let _ = handle.await;
        }
        if !shutdown_signal.is_finished() {
            shutdown_signal.abort();
        }
        let _ = shutdown_signal.await;
        serve_result?;
    }

    #[cfg(not(feature = "server"))]
    {
        let listener = TcpListener::bind(&addr).await?;
        info!(listen_addr = %addr, "runtime server listening");
        let serve_result = net::server::listen_with_shutdown(
            listener,
            identity,
            store,
            options,
            Arc::clone(&shutdown),
        )
        .await;
        shutdown.store(true, Ordering::SeqCst);
        if !shutdown_signal.is_finished() {
            shutdown_signal.abort();
        }
        let _ = shutdown_signal.await;
        serve_result?;
    }

    Ok(())
}

#[cfg(feature = "server")]
async fn start_metrics_server(
    listen_port: u16,
    store: Store,
    shutdown: Arc<AtomicBool>,
) -> Result<Option<JoinHandle<()>>, DharmaError> {
    let Some(metrics_port) = listen_port.checked_add(1) else {
        return Ok(None);
    };
    let addr = format!("0.0.0.0:{metrics_port}");
    let listener = TcpListener::bind(&addr).await?;
    info!(listen_addr = %addr, "metrics listening");

    let handle = tokio::spawn(async move {
        loop {
            if shutdown.load(Ordering::SeqCst) {
                return;
            }

            let accepted = match timeout(METRICS_ACCEPT_POLL_DELAY, listener.accept()).await {
                Ok(result) => result,
                Err(_) => continue,
            };

            let (stream, _) = match accepted {
                Ok(pair) => pair,
                Err(err) => {
                    tracing::warn!(error = %err, "metrics accept error");
                    continue;
                }
            };

            let store = store.clone();
            tokio::spawn(async move {
                let _ = handle_metrics(stream, store).await;
            });
        }
    });

    Ok(Some(handle))
}

#[cfg(feature = "server")]
async fn handle_metrics(mut stream: TcpStream, store: Store) -> Result<(), DharmaError> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    let mut parts = req.lines().next().unwrap_or("").split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    if method != "GET" || path != "/metrics" {
        let body = "Not Found\n";
        let resp = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(resp.as_bytes()).await?;
        return Ok(());
    }
    let subject_count = store.list_subjects().map(|s| s.len()).unwrap_or(0) as u64;
    let body = metrics::render_prometheus(subject_count);
    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(resp.as_bytes()).await?;
    Ok(())
}

fn ensure_data_dir() -> Result<PathBuf, DharmaError> {
    let root = std::env::current_dir()?;
    let config = Config::load(&root)?;
    let dir = config.storage_path(&root);
    if !dir.exists() {
        fs::create_dir_all(&dir)?;
    }
    Ok(dir)
}

fn load_identity<E: dharma_core::env::Env>(env: &E) -> Result<IdentityState, DharmaError> {
    let passphrase = prompt("Password: ")?;
    identity_store::load_identity(env, &passphrase)
}

fn prompt(label: &str) -> Result<String, DharmaError> {
    let mut input = String::new();
    print!("{label}");
    io::stdout().flush()?;
    io::stdin().read_line(&mut input)?;
    Ok(input.trim_end().to_string())
}

fn mount_self<E>(env: &E, identity: &IdentityState) -> Result<u64, DharmaError>
where
    E: dharma_core::env::Env + Clone + Send + Sync + 'static,
{
    let store = Store::new(env);
    let mut head_seq = 0;
    let mut head: Option<AssertionPlaintext> = None;

    let records = list_assertions(store.env(), &identity.subject_id)?;
    for record in records {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        if assertion.header.auth != identity.root_public_key {
            continue;
        }
        if assertion.header.seq > head_seq {
            head_seq = assertion.header.seq;
            head = Some(assertion);
        }
    }

    if head.is_none() {
        for object_id in store.list_objects()? {
            let bytes = store.get_assertion(&identity.subject_id, &object_id)?;
            let envelope = match envelope::AssertionEnvelope::from_cbor(&bytes) {
                Ok(env) => env,
                Err(_) => continue,
            };
            let plaintext = match envelope::decrypt_assertion(&envelope, &identity.subject_key) {
                Ok(pt) => pt,
                Err(_) => continue,
            };
            let assertion = match AssertionPlaintext::from_cbor(&plaintext) {
                Ok(a) => a,
                Err(_) => continue,
            };
            if assertion.header.auth != identity.root_public_key {
                continue;
            }
            if assertion.header.seq > head_seq {
                head_seq = assertion.header.seq;
                head = Some(assertion);
            }
        }
    }

    let head = head.ok_or_else(|| DharmaError::Validation("No identity assertions".to_string()))?;
    if !head.verify_signature()? {
        return Err(DharmaError::Validation(
            "Invalid identity head signature".to_string(),
        ));
    }
    Ok(head_seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dharma_core::identity_store;

    #[test]
    fn mount_self_accepts_root_signed_identity_head() {
        let temp = tempfile::tempdir().unwrap();
        let env = dharma_core::env::StdEnv::new(temp.path());
        identity_store::init_identity(&env, "alice", "pw")
            .unwrap()
            .expect("identity should be created");
        let identity = identity_store::load_identity(&env, "pw").unwrap();
        let head = mount_self(&env, &identity).unwrap();
        assert!(head >= 3);
    }
}
