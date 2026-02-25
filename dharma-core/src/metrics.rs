use std::sync::atomic::{AtomicU64, Ordering};

static ASSERTIONS_INGESTED: AtomicU64 = AtomicU64::new(0);
static WASM_EXECUTIONS: AtomicU64 = AtomicU64::new(0);
static PEERS_CONNECTED: AtomicU64 = AtomicU64::new(0);
static ANALYTICS_WATERMARK_SEQ: AtomicU64 = AtomicU64::new(0);
static ANALYTICS_COMMITTED_SEQ: AtomicU64 = AtomicU64::new(0);
static ANALYTICS_LAG_MS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default)]
pub struct MetricsSnapshot {
    pub assertions_ingested: u64,
    pub wasm_executions: u64,
    pub peers_connected: u64,
    pub analytics_watermark_seq: u64,
    pub analytics_committed_seq: u64,
    pub analytics_lag_ms: u64,
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

pub fn analytics_watermark_seq_set(value: u64) {
    ANALYTICS_WATERMARK_SEQ.store(value, Ordering::Relaxed);
}

pub fn analytics_committed_seq_set(value: u64) {
    ANALYTICS_COMMITTED_SEQ.store(value, Ordering::Relaxed);
}

pub fn analytics_lag_ms_set(value: u64) {
    ANALYTICS_LAG_MS.store(value, Ordering::Relaxed);
}

pub fn snapshot() -> MetricsSnapshot {
    MetricsSnapshot {
        assertions_ingested: ASSERTIONS_INGESTED.load(Ordering::Relaxed),
        wasm_executions: WASM_EXECUTIONS.load(Ordering::Relaxed),
        peers_connected: PEERS_CONNECTED.load(Ordering::Relaxed),
        analytics_watermark_seq: ANALYTICS_WATERMARK_SEQ.load(Ordering::Relaxed),
        analytics_committed_seq: ANALYTICS_COMMITTED_SEQ.load(Ordering::Relaxed),
        analytics_lag_ms: ANALYTICS_LAG_MS.load(Ordering::Relaxed),
    }
}

pub fn render_prometheus(subject_count: u64) -> String {
    let snap = snapshot();
    format!(
        "dharma_peers_connected {}\n\
dharma_subject_count {}\n\
dharma_assertions_ingested {}\n\
dharma_wasm_executions {}\n\
dharma_analytics_watermark_seq {}\n\
dharma_analytics_committed_seq {}\n\
dharma_analytics_lag_ms {}\n",
        snap.peers_connected,
        subject_count,
        snap.assertions_ingested,
        snap.wasm_executions,
        snap.analytics_watermark_seq,
        snap.analytics_committed_seq,
        snap.analytics_lag_ms,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clickhouse_metrics_expose_watermark_and_lag() {
        analytics_watermark_seq_set(21);
        analytics_committed_seq_set(34);
        analytics_lag_ms_set(1300);
        let rendered = render_prometheus(7);
        assert!(rendered.contains("dharma_analytics_watermark_seq 21"));
        assert!(rendered.contains("dharma_analytics_committed_seq 34"));
        assert!(rendered.contains("dharma_analytics_lag_ms 1300"));
    }
}
