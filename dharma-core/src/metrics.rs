use std::sync::atomic::{AtomicU64, Ordering};

static ASSERTIONS_INGESTED: AtomicU64 = AtomicU64::new(0);
static WASM_EXECUTIONS: AtomicU64 = AtomicU64::new(0);
static PEERS_CONNECTED: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default)]
pub struct MetricsSnapshot {
    pub assertions_ingested: u64,
    pub wasm_executions: u64,
    pub peers_connected: u64,
}

pub fn assertions_ingested_inc() {
    ASSERTIONS_INGESTED.fetch_add(1, Ordering::Relaxed);
}

pub fn wasm_executions_inc() {
    WASM_EXECUTIONS.fetch_add(1, Ordering::Relaxed);
}

pub fn peers_connected_inc() {
    PEERS_CONNECTED.fetch_add(1, Ordering::Relaxed);
}

pub fn peers_connected_dec() {
    PEERS_CONNECTED.fetch_sub(1, Ordering::Relaxed);
}

pub fn snapshot() -> MetricsSnapshot {
    MetricsSnapshot {
        assertions_ingested: ASSERTIONS_INGESTED.load(Ordering::Relaxed),
        wasm_executions: WASM_EXECUTIONS.load(Ordering::Relaxed),
        peers_connected: PEERS_CONNECTED.load(Ordering::Relaxed),
    }
}

pub fn render_prometheus(subject_count: u64) -> String {
    let snap = snapshot();
    format!(
        "dharma_peers_connected {}\n\
dharma_subject_count {}\n\
dharma_assertions_ingested {}\n\
dharma_wasm_executions {}\n",
        snap.peers_connected, subject_count, snap.assertions_ingested, snap.wasm_executions,
    )
}
