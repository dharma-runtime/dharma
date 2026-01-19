use dharma_core::assertion::{add_signer_meta, AssertionHeader, AssertionPlaintext};
use dharma_core::assertion::DEFAULT_DATA_VERSION;
use dharma_core::cbor;
use dharma_core::crypto;
use dharma_core::builtins;
use dharma_core::envelope;
use dharma_core::net::handshake::{client_handshake, server_handshake};
use dharma_core::net::ingest::{ingest_object, IngestStatus};
use dharma_core::net::policy::{OverlayAccess, OverlayPolicy, PeerClaims};
use dharma_core::net::sync::{sync_loop_with, SyncOptions};
use dharma_core::dharmaq::{self, CmpOp, Filter, Predicate, QueryPlan};
use dharma_core::pdl::schema::{
    ActionSchema, ConcurrencyMode, CqrsSchema, FieldSchema, TypeSpec, Visibility,
};
use dharma_core::runtime::cqrs::{decode_state, load_state};
use dharma_core::store::index::FrontierIndex;
use dharma_core::store::state::append_assertion;
use dharma_core::store::state::list_assertions;
use dharma_core::store::Store;
use dharma_core::types::{AssertionId, ContractId, EnvelopeId, KeyId, Nonce12, SchemaId, SubjectId};
use dharma_core::validation::{order_assertions, structural_validate, StructuralStatus};
use dharma_core::{DharmaError, IdentityState};
use dharma_core::env::{Env, StdEnv};
use dharma_core::identity_store;
use dharma_core::net::{self, server};
use dharma_core::sync::Subscriptions;
use dharma_sim::{
    ClockFaultConfig, FaultConfig, FaultEvent, FaultTimeline, FsFaultConfig, NodeId, SimEnv,
    SimHub, TraceSink,
};
use rand_chacha::ChaCha20Rng;
use rand_core::{RngCore, SeedableRng};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::IsTerminal;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use std::sync::mpsc;
use tempfile::TempDir;

mod renderer;
use renderer::{HeadlessRenderer, Phase, Renderer, Status, TuiRenderer};

static LAST_LOG: Mutex<Option<Instant>> = Mutex::new(None);

#[derive(Clone, Debug)]
pub struct TestOptions {
    pub deep: bool,
    pub chaos: bool,
    pub ci: bool,
    pub replay_seed: Option<u64>,
    pub relay_only: bool,
}

impl TestOptions {
    pub fn seed_count(&self) -> usize {
        if self.replay_seed.is_some() {
            1
        } else if self.deep {
            1000
        } else {
            1
        }
    }

    pub fn iterations(&self) -> usize {
        if self.deep { 200 } else { 25 }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TestSummary {
    pub passed: usize,
    pub failed: usize,
    pub tickets: Vec<PathBuf>,
    pub seed: u64,
}

#[derive(Clone, Debug)]
struct TestFailure {
    property: &'static str,
    seed: u64,
    details: String,
    trace: Option<Vec<String>>,
}

pub fn run(opts: TestOptions) -> Result<TestSummary, DharmaError> {
    let mut summary = TestSummary::default();
    let base_seed = opts.replay_seed.unwrap_or_else(random_seed);
    summary.seed = base_seed;
    let mut status = Status::new(base_seed, opts.seed_count());
    let mut renderer: Box<dyn Renderer> = if opts.ci || !std::io::stdout().is_terminal() {
        Box::new(HeadlessRenderer::new())
    } else {
        match TuiRenderer::new() {
            Ok(renderer) => Box::new(renderer),
            Err(_) => Box::new(HeadlessRenderer::new()),
        }
    };
    renderer.start(&status);
    let interactive = renderer.is_interactive();

    if opts.relay_only {
        status.phase = Phase::Properties;
        status.current = Some("P-RELAY-001".to_string());
        status.iterations = 1;
        renderer.update(&status);
        match run_relay_suite(base_seed) {
            Ok(()) => {
                summary.passed += 1;
                status.passed = summary.passed;
            }
            Err(failure) => {
                summary.failed += 1;
                status.failed = summary.failed;
                let ticket = write_ticket(&failure, &opts)?;
                summary.tickets.push(ticket);
                let message = format!("{}: {}", failure.property, failure.details);
                status.error = Some(message.clone());
                renderer.log(&message);
                renderer.update(&status);
                if !interactive {
                    renderer.finish(&status);
                    return Ok(summary);
                }
                renderer.pause();
                status.error = None;
            }
        }
        status.phase = Phase::Done;
        renderer.finish(&status);
        return Ok(summary);
    }

    for idx in 0..opts.seed_count() {
        let seed = base_seed.wrapping_add(idx as u64);
        status.seed = seed;
        status.seed_index = idx + 1;
        status.seed_total = opts.seed_count();
        status.phase = Phase::Properties;
        status.current = None;
        status.iteration = 0;
        status.iterations = opts.iterations();
        status.nodes = 0;
        status.error = None;
        renderer.update(&status);
        if let Err(failure) = run_properties(seed, &opts, renderer.as_mut(), &mut status) {
            summary.failed += 1;
            status.failed = summary.failed;
            let ticket = write_ticket(&failure, &opts)?;
            summary.tickets.push(ticket);
            let message = format!("{}: {}", failure.property, failure.details);
            status.error = Some(message.clone());
            renderer.log(&message);
            renderer.update(&status);
            if !interactive {
                renderer.finish(&status);
                return Ok(summary);
            }
            renderer.pause();
            status.error = None;
            renderer.update(&status);
            continue;
        }
        summary.passed += 1;
        status.passed = summary.passed;
        renderer.update(&status);
    }

    status.phase = Phase::Vectors;
    status.current = None;
    status.iteration = 0;
    status.iterations = 0;
    status.nodes = 0;
    status.error = None;
    renderer.update(&status);
    let vectors_ok = match run_vectors(renderer.as_mut(), &mut status) {
        Ok(()) => true,
        Err(failure) => {
            summary.failed += 1;
            status.failed = summary.failed;
            let ticket = write_ticket(&failure, &opts)?;
            summary.tickets.push(ticket);
            let message = format!("{}: {}", failure.property, failure.details);
            status.error = Some(message.clone());
            renderer.log(&message);
            renderer.update(&status);
            if !interactive {
                renderer.finish(&status);
                return Ok(summary);
            }
            renderer.pause();
            status.error = None;
            renderer.update(&status);
            false
        }
    };
    if vectors_ok {
        summary.passed += 1;
        status.passed = summary.passed;
        renderer.update(&status);
    }
    if opts.deep {
        status.phase = Phase::Simulation;
        status.current = None;
        status.iteration = 0;
        status.iterations = 0;
        status.nodes = 0;
        status.error = None;
        renderer.update(&status);
        let sim_ok = match run_simulation(base_seed, &opts, renderer.as_mut(), &mut status) {
            Ok(()) => true,
            Err(failure) => {
                summary.failed += 1;
                status.failed = summary.failed;
                let ticket = write_ticket(&failure, &opts)?;
                summary.tickets.push(ticket);
                let message = format!("{}: {}", failure.property, failure.details);
                status.error = Some(message.clone());
                renderer.log(&message);
                renderer.update(&status);
                if !interactive {
                    renderer.finish(&status);
                    return Ok(summary);
                }
                renderer.pause();
                status.error = None;
                renderer.update(&status);
                false
            }
        };
        if sim_ok {
            summary.passed += 1;
            status.passed = summary.passed;
            renderer.update(&status);
        }
    }
    status.phase = Phase::Done;
    renderer.finish(&status);
    Ok(summary)
}

fn random_seed() -> u64 {
    let mut buf = [0u8; 8];
    rand_core::OsRng.fill_bytes(&mut buf);
    u64::from_le_bytes(buf)
}

fn run_properties(
    seed: u64,
    opts: &TestOptions,
    renderer: &mut dyn Renderer,
    status: &mut Status,
) -> Result<(), TestFailure> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let iters = opts.iterations();
    let properties: &[(&str, fn(&mut ChaCha20Rng, usize) -> Result<(), String>)] = &[
        ("P-CBOR-001", prop_cbor_roundtrip),
        ("P-CBOR-002", prop_cbor_rejects_noncanonical),
        ("P-ASSERT-001", prop_assert_signature_roundtrip),
        ("P-ASSERT-002", prop_assert_signature_rejects_bitflip),
        ("P-ASSERT-003", prop_assert_structural_rejects_seq_prev),
        ("P-ENV-001", prop_envelope_roundtrip),
        ("P-ENV-002", prop_envelope_wrong_key),
        ("P-ENV-003", prop_envelope_id_changes_on_mutation),
        ("P-DAG-001", prop_dag_ordering_respects_deps),
        ("P-DAG-002", prop_dag_detects_cycle),
        ("P-STORE-001", prop_store_ingest_idempotent),
        ("P-STORE-002", prop_store_frontier_deterministic),
        ("P-DET-001", prop_replay_deterministic),
        ("P-CONV-001", prop_convergence),
        ("P-CQRS-001", prop_cqrs_replay_deterministic),
        ("P-CQRS-002", prop_cqrs_decode_deterministic),
        ("D-Q-001", prop_dharmaq_rebuild_deterministic),
        ("D-Q-002", prop_dharmaq_and_commutative),
    ];
    for (name, prop) in properties {
        status.current = Some((*name).to_string());
        status.iteration = 0;
        status.iterations = iters;
        renderer.update(status);
        if let Err(err) = prop(&mut rng, iters) {
            return Err(TestFailure {
                property: name,
                seed,
                details: err,
                trace: None,
            });
        }
        status.iteration = iters;
        renderer.update(status);
    }
    Ok(())
}

fn run_relay_suite(seed: u64) -> Result<(), TestFailure> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let mut trace = Vec::new();
    if let Err(err) = prop_relay_baseline(&mut rng, &mut trace) {
        return Err(TestFailure {
            property: "P-RELAY-001",
            seed,
            details: err,
            trace: Some(trace),
        });
    }
    Ok(())
}

fn run_vectors(renderer: &mut dyn Renderer, status: &mut Status) -> Result<(), TestFailure> {
    let root = std::env::current_dir().map_err(|err| TestFailure {
        property: "VECTOR-ROOT",
        seed: 0,
        details: err.to_string(),
        trace: None,
    })?;
    let vectors_root = root.join("tests").join("vectors");
    if !vectors_root.exists() {
        return Ok(());
    }
    let mut failures = Vec::new();
    for entry in fs::read_dir(&vectors_root).map_err(|err| TestFailure {
        property: "VECTOR-READ",
        seed: 0,
        details: err.to_string(),
        trace: None,
    })? {
        let entry = entry.map_err(|err| TestFailure {
            property: "VECTOR-READ",
            seed: 0,
            details: err.to_string(),
            trace: None,
        })?;
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let category = entry.file_name().to_string_lossy().to_string();
        let dir = entry.path();
        let metas = match list_meta_files(&dir) {
            Ok(metas) => metas,
            Err(err) => {
                failures.push(TestFailure {
                    property: "VECTOR-META",
                    seed: 0,
                    details: err.to_string(),
                    trace: None,
                });
                continue;
            }
        };
        let total = metas.len();
        let mut idx = 0usize;
        for meta_path in metas {
            idx = idx.saturating_add(1);
            status.current = Some(format!(
                "{}/{}",
                category,
                meta_path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("vector")
            ));
            status.iteration = idx;
            status.iterations = total;
            renderer.update(status);
            if let Err(err) = run_vector_case(&category, &meta_path) {
                failures.push(err);
            }
        }
    }
    if let Some(first) = failures.into_iter().next() {
        return Err(first);
    }
    Ok(())
}

fn run_simulation(
    seed: u64,
    opts: &TestOptions,
    renderer: &mut dyn Renderer,
    status: &mut Status,
) -> Result<(), TestFailure> {
    status.current = Some("sim".to_string());
    renderer.update(status);
    match sim_convergence(seed, opts, renderer, status, false) {
        Ok(()) => Ok(()),
        Err((details, trace)) => {
            let mut full_trace = trace;
            if let Err((deep_details, deep_trace)) =
                sim_convergence(seed, opts, renderer, status, true)
            {
                if !deep_trace.is_empty() {
                    full_trace = deep_trace;
                }
                if deep_details != details {
                    full_trace.push(format!("deep_trace_details: {}", deep_details));
                }
            } else {
                full_trace.push("deep_trace_replay: ok".to_string());
            }
            Err(TestFailure {
                property: "P-SIM-001",
                seed,
                details,
                trace: Some(full_trace),
            })
        }
    }
}

fn sim_convergence(
    seed: u64,
    opts: &TestOptions,
    renderer: &mut dyn Renderer,
    status: &mut Status,
    deep_trace: bool,
) -> Result<(), (String, Vec<String>)> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed ^ 0x5a5a_1234);
    let hub = SimHub::new(rng.next_u64());
    let mut trace_extra: Vec<String> = Vec::new();
    touch_log();
    let trace_sink = if deep_trace {
        Some(Arc::new(TraceSink::new(20000)))
    } else {
        None
    };
    let trace_ref = trace_sink.as_ref().map(|sink| sink.as_ref());
    let mut timeline = FaultTimeline::new();
    record_trace(
        &mut trace_extra,
        renderer,
        format!(
            "sim_start seed={} deep={} chaos={} trace={}",
            seed,
            opts.deep,
            opts.chaos,
            if deep_trace { "deep" } else { "normal" }
        ),
    );
    if opts.chaos {
        let warmup_faults = FaultConfig {
            drop_rate: 0.0,
            dup_rate: 0.0,
            min_delay: 1,
            max_delay: 1,
        };
        let net_faults = FaultConfig {
            drop_rate: 0.1,
            dup_rate: 0.05,
            min_delay: 0,
            max_delay: 5,
        };
        let warmup_end = 4;
        timeline.schedule_net(0, warmup_faults.clone());
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script net t=0 drop={} dup={} min_delay={} max_delay={}",
                warmup_faults.drop_rate,
                warmup_faults.dup_rate,
                warmup_faults.min_delay,
                warmup_faults.max_delay
            ),
        );
        timeline.schedule_net(warmup_end, net_faults.clone());
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script net t={} drop={} dup={} min_delay={} max_delay={}",
                warmup_end,
                net_faults.drop_rate,
                net_faults.dup_rate,
                net_faults.min_delay,
                net_faults.max_delay
            ),
        );
        let clear_at = warmup_end + 25 + (rng.next_u32() % 25) as i64;
        timeline.schedule_net(clear_at, FaultConfig::default());
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script net t={} drop=0 dup=0 min_delay=0 max_delay=0",
                clear_at
            ),
        );
    }

    let mut nodes = Vec::new();
    for _ in 0..3 {
        let mut env = SimEnv::new(rng.next_u64(), 0);
        if let Some(trace) = trace_sink.as_ref() {
            env.enable_trace(Arc::clone(trace));
        }
        let mut clock_cfg = None;
        let mut fs_cfg = None;
        if opts.chaos {
            fs_cfg = Some(FsFaultConfig {
                enospc_after: Some(64 * 1024),
                torn_every: Some(17),
                torn_max: 32,
                fsync_lie_every: Some(19),
                read_corrupt_every: Some(23),
                read_corrupt_bits: 0x01,
            });
            let drift = (rng.next_u32() % 3) as i64;
            let jump_every = 5 + (rng.next_u32() % 5) as u64;
            let jump_amount = (rng.next_u32() % 20) as i64;
            let mono_every = 7 + (rng.next_u32() % 5) as u64;
            let backstep = 1 + (rng.next_u32() % 3) as i64;
            clock_cfg = Some((drift, jump_every, jump_amount, mono_every, backstep));
        }
        let store = Store::new(&env);
        let index = FrontierIndex::build(&store, &HashMap::new())
            .map_err(|e| (e.to_string(), merge_trace(&trace_extra, &hub, trace_ref)))?;
        let id = hub.register_node();
        record_trace(
            &mut trace_extra,
            renderer,
            format!("node_init id={} root={}", id, env.root().display()),
        );
        if let Some(cfg) = fs_cfg.clone() {
            timeline.schedule_fs(0, id, cfg.clone());
            record_trace(
                &mut trace_extra,
                renderer,
                format!(
                    "fault_script fs t=0 node={} enospc_after={:?} torn_every={:?} torn_max={} fsync_lie_every={:?} read_corrupt_every={:?} read_corrupt_bits={}",
                    id,
                    cfg.enospc_after,
                    cfg.torn_every,
                    cfg.torn_max,
                    cfg.fsync_lie_every,
                    cfg.read_corrupt_every,
                    cfg.read_corrupt_bits
                ),
            );
        }
        if let Some((drift, jump_every, jump_amount, mono_every, backstep)) = clock_cfg {
            timeline.schedule_clock(
                0,
                id,
                ClockFaultConfig {
                    drift_per_call: drift,
                    jump_every: Some(jump_every),
                    jump_amount,
                    monotonic_violation_every: Some(mono_every),
                    monotonic_backstep: backstep,
                },
            );
            record_trace(
                &mut trace_extra,
                renderer,
                format!(
                    "clock_faults node={} drift={} jump_every={} jump_amount={} mono_every={} backstep={}",
                    id, drift, jump_every, jump_amount, mono_every, backstep
                ),
            );
        }
        let identity = make_identity(&mut rng);
        nodes.push(SimNode {
            id,
            env,
            store,
            index: Arc::new(Mutex::new(index)),
            schema_id: None,
            contract_id: None,
            identity,
        });
    }

    for node in &mut nodes {
        let (schema_id, contract_id) =
            store_cqrs_artifacts(&node.store)
                .map_err(|e| (e, merge_trace(&trace_extra, &hub, trace_ref)))?;
        node.schema_id = Some(schema_id);
        node.contract_id = Some(contract_id);
    }

    let mut identity_genesis = Vec::with_capacity(nodes.len());
    for idx in 0..nodes.len() {
        let mut index = nodes[idx].index.lock().unwrap();
        let genesis_id = seed_identity(
            &nodes[idx].store,
            &mut index,
            &nodes[idx].identity,
            &mut |line| {
                record_trace(&mut trace_extra, renderer, line);
            },
        )
        .map_err(|e| (e, merge_trace(&trace_extra, &hub, trace_ref)))?;
        identity_genesis.push(genesis_id);
    }

    for idx in 0..nodes.len() {
        let subject = nodes[idx].identity.subject_id;
        let start_seq = 2;
        let prev = Some(identity_genesis[idx]);
        nodes[idx]
            .create_chain(subject, 3, start_seq, prev, &mut |line| {
                record_trace(&mut trace_extra, renderer, line);
            })
            .map_err(|e| (e, merge_trace(&trace_extra, &hub, trace_ref)))?;
        record_trace(
            &mut trace_extra,
            renderer,
            format!("node_chain node={} subject={} count=3", nodes[idx].id, subject.to_hex()),
        );
    }

    status.nodes = nodes.len();
    renderer.update(status);

    let mut timeline_nodes: HashMap<NodeId, TimelineNode> = HashMap::new();
    for node in &nodes {
        timeline_nodes.insert(
            node.id,
            TimelineNode {
                env: node.env.clone(),
                store: node.store.clone(),
                index: node.index.clone(),
            },
        );
    }

    if opts.chaos {
        let crash_id = nodes[1].id;
        let crash_at = 5 + (rng.next_u32() % 5) as i64;
        let restart_at = crash_at + 5 + (rng.next_u32() % 10) as i64;
        timeline.schedule_online(crash_at, crash_id, false);
        timeline.schedule_online(restart_at, crash_id, true);
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script online t={} node={} state=down",
                crash_at, crash_id
            ),
        );
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script online t={} node={} state=up",
                restart_at, crash_id
            ),
        );

        let part_at = 3 + (rng.next_u32() % 5) as i64;
        let heal_at = part_at + 4 + (rng.next_u32() % 6) as i64;
        let a = nodes[0].id;
        let b = nodes[2].id;
        timeline.schedule_partition(part_at, a, b, true);
        timeline.schedule_partition(heal_at, a, b, false);
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script partition t={} a={} b={} state=blocked",
                part_at, a, b
            ),
        );
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script partition t={} a={} b={} state=open",
                heal_at, a, b
            ),
        );
    }

    if opts.chaos {
        apply_timeline(&mut timeline, hub.now(), &hub, &timeline_nodes);
    }

    let _peer_ids: Vec<_> = nodes.iter().map(|n| n.id).collect();
    let running = Arc::new(AtomicBool::new(true));
    let hub_runner = hub.clone();
    let running_runner = running.clone();
    let mut timeline_runner = timeline;
    let timeline_nodes_runner = timeline_nodes.clone();
    let runner = thread::spawn(move || {
        while running_runner.load(Ordering::SeqCst) {
            let stepped = hub_runner.step();
            let now = hub_runner.now();
            apply_timeline(&mut timeline_runner, now, &hub_runner, &timeline_nodes_runner);
            if !stepped {
                thread::yield_now();
            }
        }
        let now = hub_runner.now();
        apply_timeline(&mut timeline_runner, now, &hub_runner, &timeline_nodes_runner);
        hub_runner.step();
    });

    let sync_timeout = if opts.chaos {
        Duration::from_secs(10)
    } else {
        Duration::from_secs(5)
    };
    let max_attempts = if opts.chaos { 4 } else { 1 };
    let mut sync_pair = |a: &SimNode, b: &SimNode| -> Result<(), (String, Vec<String>)> {
        for attempt in 1..=max_attempts {
            record_trace(
                &mut trace_extra,
                renderer,
                format!("sync_pair a={} b={} attempt={}", a.id, b.id, attempt),
            );
            match sync_pair_real(a, b, hub.clone(), sync_timeout) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if opts.chaos && is_transient_sync_error(&err) {
                        record_trace(
                            &mut trace_extra,
                            renderer,
                            format!(
                                "sync_pair_err a={} b={} attempt={} err={}",
                                a.id, b.id, attempt, err
                            ),
                        );
                        if attempt < max_attempts {
                            thread::sleep(Duration::from_millis(10));
                            continue;
                        }
                        return Ok(());
                    }
                    return Err((err, merge_trace(&trace_extra, &hub, trace_ref)));
                }
            }
        }
        Ok(())
    };

    sync_pair(&nodes[0], &nodes[1])?;
    sync_pair(&nodes[1], &nodes[2])?;
    sync_pair(&nodes[0], &nodes[2])?;

    if opts.chaos {
        sync_pair(&nodes[0], &nodes[2])?;
        sync_pair(&nodes[0], &nodes[1])?;
        sync_pair(&nodes[1], &nodes[2])?;
    }

    running.store(false, Ordering::SeqCst);
    runner
        .join()
        .map_err(|_| ("net runner failed".to_string(), merge_trace(&trace_extra, &hub, trace_ref)))?;

    let expected =
        snapshot_env(&nodes[0].env).map_err(|e| (e, merge_trace(&trace_extra, &hub, trace_ref)))?;
    for node in nodes.iter().skip(1) {
        let snapshot =
            snapshot_env(&node.env).map_err(|e| (e, merge_trace(&trace_extra, &hub, trace_ref)))?;
        if snapshot != expected {
            let diff = diff_snapshots(&expected, &snapshot);
            return Err((
                format!("simulation convergence mismatch (node {}): {diff}", node.id),
                merge_trace(&trace_extra, &hub, trace_ref),
            ));
        }
    }
    Ok(())
}

fn record_trace(trace: &mut Vec<String>, renderer: &mut dyn Renderer, line: String) {
    trace.push(line.clone());
    renderer.log(&line);
    touch_log();
}

fn touch_log() {
    if let Ok(mut guard) = LAST_LOG.lock() {
        *guard = Some(Instant::now());
    }
}

fn log_stale(timeout: Duration) -> bool {
    let Ok(guard) = LAST_LOG.lock() else {
        return false;
    };
    match *guard {
        Some(ts) => ts.elapsed() > timeout,
        None => false,
    }
}

fn seed_identity(
    store: &Store,
    index: &mut FrontierIndex,
    identity: &dharma_core::identity::IdentityState,
    trace: &mut dyn FnMut(String),
) -> Result<AssertionId, String> {
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(store).map_err(|e| e.to_string())?;
    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: identity.subject_id,
        typ: "core.genesis".to_string(),
        auth: identity.root_public_key,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: Some("identity genesis".to_string()),
        meta: add_signer_meta(None, &identity.subject_id),
    };
    let body = ciborium::value::Value::Map(vec![]);
    let assertion = AssertionPlaintext::sign(header, body, &identity.root_signing_key)
        .map_err(|e| e.to_string())?;
    let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
    let envelope_id = crypto::envelope_id(&bytes);
    let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
    store
        .put_assertion(&identity.subject_id, &envelope_id, &bytes)
        .map_err(|e| e.to_string())?;
    store
        .record_semantic(&assertion_id, &envelope_id)
        .map_err(|e| e.to_string())?;
    append_assertion(
        store.env(),
        &identity.subject_id,
        1,
        assertion_id,
        envelope_id,
        "core.genesis",
        &bytes,
    )
    .map_err(|e| e.to_string())?;
    index
        .update(assertion_id, &assertion.header)
        .map_err(|e| e.to_string())?;
    trace(format!(
        "identity_seed subject={} id={}",
        identity.subject_id.to_hex(),
        assertion_id.to_hex()
    ));
    Ok(assertion_id)
}

fn apply_timeline(
    timeline: &mut FaultTimeline,
    now: i64,
    hub: &SimHub,
    nodes: &HashMap<NodeId, TimelineNode>,
) {
    for event in timeline.drain_ready(now) {
        match event {
            FaultEvent::Net(config) => {
                hub.set_faults(config);
            }
            FaultEvent::Partition { a, b, blocked } => {
                hub.set_partition(a, b, blocked);
            }
            FaultEvent::Online { node, online } => {
                hub.set_online(node, online);
                if online {
                    if let Some(handle) = nodes.get(&node) {
                        if let Ok(rebuilt) = FrontierIndex::build(&handle.store, &HashMap::new()) {
                            if let Ok(mut guard) = handle.index.lock() {
                                *guard = rebuilt;
                            }
                        }
                    }
                }
            }
            FaultEvent::Fs { node, config } => {
                if let Some(handle) = nodes.get(&node) {
                    handle.env.set_fs_faults(config);
                }
            }
            FaultEvent::Clock { node, config } => {
                if let Some(handle) = nodes.get(&node) {
                    handle.env.set_clock_faults(config);
                }
            }
        }
    }
}

fn merge_trace(extra: &[String], hub: &SimHub, sink: Option<&TraceSink>) -> Vec<String> {
    let mut trace = Vec::new();
    if !extra.is_empty() {
        trace.extend(extra.iter().cloned());
    }
    if let Some(sink) = sink {
        for line in sink.snapshot() {
            trace.push(format!("io {}", line));
        }
    }
    trace.extend(hub.trace_snapshot());
    trace
}

#[derive(Clone)]
struct TimelineNode {
    env: SimEnv,
    store: Store,
    index: Arc<Mutex<FrontierIndex>>,
}

struct SimNode {
    id: u64,
    env: SimEnv,
    store: Store,
    index: Arc<Mutex<FrontierIndex>>,
    schema_id: Option<SchemaId>,
    contract_id: Option<ContractId>,
    identity: dharma_core::identity::IdentityState,
}

impl SimNode {
    fn create_chain(
        &mut self,
        subject: SubjectId,
        count: u64,
        start_seq: u64,
        mut prev: Option<AssertionId>,
        trace: &mut dyn FnMut(String),
    ) -> Result<(), String> {
        let (schema_id, contract_id) =
            builtins::ensure_note_artifacts(&self.store).map_err(|e| e.to_string())?;
        let sk = &self.identity.root_signing_key;
        let pk = self.identity.root_public_key;
        for offset in 0..count {
            let seq = start_seq + offset;
            trace(format!(
                "assert_build node={} subject={} signer={} seq={} prev={}",
                self.id,
                subject.to_hex(),
                self.identity.subject_id.to_hex(),
                seq,
                prev.map(|id| id.to_hex()).unwrap_or_else(|| "-".to_string())
            ));
            let header = AssertionHeader {
                v: crypto::PROTOCOL_VERSION,
                ver: DEFAULT_DATA_VERSION,
                sub: subject,
                typ: "note.text".to_string(),
                auth: pk,
                seq,
                prev,
                refs: vec![],
                ts: None,
                schema: schema_id,
                contract: contract_id,
                note: None,
                meta: add_signer_meta(None, &self.identity.subject_id),
            };
            let body = ciborium::value::Value::Map(vec![(
                ciborium::value::Value::Text("text".to_string()),
                ciborium::value::Value::Text(format!("value {}", seq)),
            )]);
            let assertion = AssertionPlaintext::sign(header, body, sk).map_err(|e| e.to_string())?;
            let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
            let mut index = self.index.lock().unwrap();
            let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
            match ingest_object(&self.store, &mut index, &bytes, &HashMap::new()) {
                Ok(_) => {
                    trace(format!(
                        "assert_ingest_ok node={} subject={} signer={} seq={} id={}",
                        self.id,
                        subject.to_hex(),
                        self.identity.subject_id.to_hex(),
                        seq,
                        assertion_id.to_hex()
                    ));
                }
                Err(err) => {
                    trace(format!(
                        "assert_ingest_err node={} subject={} signer={} seq={} id={} err={:?}",
                        self.id,
                        subject.to_hex(),
                        self.identity.subject_id.to_hex(),
                        seq,
                        assertion_id.to_hex(),
                        err
                    ));
                    return Err(format!("ingest failed: {err:?}"));
                }
            }
            prev = Some(assertion_id);
        }
        Ok(())
    }
}

fn sync_pair_real(
    a: &SimNode,
    b: &SimNode,
    hub: Arc<SimHub>,
    timeout: Duration,
) -> Result<(), String> {
    let (mut stream_a, mut stream_b, control) = dharma_sim::SimStream::pair(hub.clone(), a.id, b.id);
    let ready = Arc::new(AtomicUsize::new(0));
    let store_a = a.store.clone();
    let identity_a = a.identity.clone();
    let index_a = a.index.clone();
    let keys_a: HashMap<SubjectId, [u8; 32]> = HashMap::new();
    let ready_a = ready.clone();
    let thread_a = thread::spawn(move || {
        let policy = OverlayPolicy::default();
        let claims = PeerClaims::default();
        let access = OverlayAccess::new(&policy, None, false, &claims);
        let session = server_handshake(&mut stream_a, &identity_a).map_err(|e| e.to_string())?;
        let options = SyncOptions {
            relay: false,
            ad_store: None,
            local_subs: None,
            verbose: false,
            exit_on_idle: true,
            trace: None,
        };
        ready_a.fetch_add(1, Ordering::SeqCst);
        let mut index = index_a.lock().unwrap();
        sync_loop_with(
            &mut stream_a,
            session.0,
            &store_a,
            &mut index,
            &keys_a,
            &identity_a,
            &access,
            options,
        )
        .map_err(|e| e.to_string())
    });

    let store_b = b.store.clone();
    let identity_b = b.identity.clone();
    let index_b = b.index.clone();
    let keys_b: HashMap<SubjectId, [u8; 32]> = HashMap::new();
    let ready_b = ready.clone();
    let thread_b = thread::spawn(move || {
        let policy = OverlayPolicy::default();
        let claims = PeerClaims::default();
        let access = OverlayAccess::new(&policy, None, false, &claims);
        let session = client_handshake(&mut stream_b, &identity_b).map_err(|e| e.to_string())?;
        let options = SyncOptions {
            relay: false,
            ad_store: None,
            local_subs: None,
            verbose: false,
            exit_on_idle: true,
            trace: None,
        };
        ready_b.fetch_add(1, Ordering::SeqCst);
        let mut index = index_b.lock().unwrap();
        sync_loop_with(
            &mut stream_b,
            session,
            &store_b,
            &mut index,
            &keys_b,
            &identity_b,
            &access,
            options,
        )
        .map_err(|e| e.to_string())
    });

    let deadline = Instant::now() + timeout;
    let mut timed_out = false;
    while ready.load(Ordering::SeqCst) < 2 {
        if Instant::now() >= deadline {
            timed_out = true;
            break;
        }
        if hub.has_pending_between(a.id, b.id) || hub.has_pending_between(b.id, a.id) {
            touch_log();
        }
        thread::yield_now();
        thread::sleep(Duration::from_millis(1));
    }

    let quiet = Duration::from_millis(50);
    let mut quiet_since: Option<Instant> = None;
    if !timed_out {
        loop {
            if Instant::now() >= deadline {
                timed_out = true;
                break;
            }
            if log_stale(timeout) {
                control.close();
                return Err("no activity for 5s".to_string());
            }
            let pending = hub.has_pending_between(a.id, b.id) || hub.has_pending_between(b.id, a.id);
            if pending {
                touch_log();
                quiet_since = None;
            } else if let Some(since) = quiet_since {
                if since.elapsed() >= quiet {
                    break;
                }
            } else {
                quiet_since = Some(Instant::now());
            }
            thread::yield_now();
        }
    }
    control.close();
    let join_grace = std::cmp::max(Duration::from_millis(200), timeout / 4);
    let join_deadline = Instant::now() + join_grace;
    while !thread_a.is_finished() || !thread_b.is_finished() {
        if Instant::now() >= join_deadline {
            timed_out = true;
            break;
        }
        if log_stale(timeout) {
            return Err("no activity for 5s".to_string());
        }
        if hub.has_pending_between(a.id, b.id) || hub.has_pending_between(b.id, a.id) {
            touch_log();
        }
        thread::yield_now();
    }
    if !thread_a.is_finished() || !thread_b.is_finished() {
        return Err("sync timeout".to_string());
    }
    let res_a = thread_a.join().map_err(|_| "sync thread A failed".to_string())?;
    res_a?;
    let res_b = thread_b.join().map_err(|_| "sync thread B failed".to_string())?;
    res_b?;
    if timed_out {
        return Err("sync timeout".to_string());
    }
    Ok(())
}

fn is_transient_sync_error(err: &str) -> bool {
    err.contains("network error")
        || err.contains("unexpected eof")
        || err.contains("no activity")
        || err.contains("io error")
        || err.contains("torn write")
        || err.contains("ENOSPC")
        || err.contains("sync timeout")
        || err.contains("cbor error")
        || err.contains("missing payload")
        || err.contains("missing n")
        || err.contains("invalid frame")
        || err.contains("expected hello")
        || err.contains("expected auth")
}

fn make_identity(rng: &mut ChaCha20Rng) -> dharma_core::identity::IdentityState {
    let (signing_key, public_key) = crypto::generate_identity_keypair(rng);
    let root_signing_key = signing_key.clone();
    let root_public_key = public_key;
    let mut subject_key = [0u8; 32];
    rng.fill_bytes(&mut subject_key);
    let mut noise_sk = [0u8; 32];
    rng.fill_bytes(&mut noise_sk);
    dharma_core::identity::IdentityState {
        subject_id: SubjectId::from_bytes(rand_bytes32(rng)),
        signing_key,
        public_key,
        root_signing_key,
        root_public_key,
        subject_key,
        noise_sk,
        schema: SchemaId::from_bytes([0u8; 32]),
        contract: ContractId::from_bytes([0u8; 32]),
    }
}

fn list_meta_files(dir: &Path) -> Result<Vec<PathBuf>, DharmaError> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("meta") {
            out.push(path);
        }
    }
    Ok(out)
}

fn run_vector_case(category: &str, meta_path: &Path) -> Result<(), TestFailure> {
    let meta = load_meta(meta_path).map_err(|err| TestFailure {
        property: "VECTOR-META",
        seed: 0,
        details: err.to_string(),
        trace: None,
    })?;
    let data_path = meta_data_path(meta_path, &meta).map_err(|err| TestFailure {
        property: "VECTOR-DATA",
        seed: 0,
        details: err.to_string(),
        trace: None,
    })?;
    let bytes = fs::read(&data_path).map_err(|err| TestFailure {
        property: "VECTOR-DATA",
        seed: 0,
        details: err.to_string(),
        trace: None,
    })?;
    match category {
        "cbor" => vector_cbor(&bytes, &meta).map_err(|details| TestFailure {
            property: "VECTOR-CBOR",
            seed: 0,
            details,
            trace: None,
        }),
        "assertion" => vector_assertion(&bytes, &meta).map_err(|details| TestFailure {
            property: "VECTOR-ASSERTION",
            seed: 0,
            details,
            trace: None,
        }),
        "envelope" => vector_envelope(&bytes, &meta).map_err(|details| TestFailure {
            property: "VECTOR-ENVELOPE",
            seed: 0,
            details,
            trace: None,
        }),
        "schema" => vector_schema(&bytes, &meta).map_err(|details| TestFailure {
            property: "VECTOR-SCHEMA",
            seed: 0,
            details,
            trace: None,
        }),
        "contract" => vector_contract(&bytes, &meta, &data_path).map_err(|details| TestFailure {
            property: "VECTOR-CONTRACT",
            seed: 0,
            details,
            trace: None,
        }),
        other => Err(TestFailure {
            property: "VECTOR-UNKNOWN",
            seed: 0,
            details: format!("unknown vector category {other}"),
            trace: None,
        }),
    }
}

#[derive(Clone, Debug)]
struct VectorMeta {
    expect: String,
    key_hex: Option<String>,
    data_ext: Option<String>,
}

fn load_meta(path: &Path) -> Result<VectorMeta, DharmaError> {
    let contents = fs::read_to_string(path)?;
    let mut expect = None;
    let mut key_hex = None;
    let mut data_ext = None;
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            let key = k.trim();
            let value = v.trim().trim_matches('"');
            match key {
                "expect" => expect = Some(value.to_string()),
                "key" => key_hex = Some(value.to_string()),
                "data_ext" => data_ext = Some(value.to_string()),
                _ => {}
            }
        }
    }
    let expect = expect.ok_or_else(|| DharmaError::Validation("missing expect".to_string()))?;
    Ok(VectorMeta {
        expect,
        key_hex,
        data_ext,
    })
}

fn meta_data_path(meta_path: &Path, meta: &VectorMeta) -> Result<PathBuf, DharmaError> {
    let stem = meta_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| DharmaError::Validation("invalid meta file".to_string()))?;
    let ext = meta
        .data_ext
        .clone()
        .unwrap_or_else(|| "cbor".to_string());
    let data = meta_path.with_file_name(format!("{stem}.{ext}"));
    Ok(data)
}

fn vector_cbor(bytes: &[u8], meta: &VectorMeta) -> Result<(), String> {
    match meta.expect.as_str() {
        "canonical" => {
            cbor::ensure_canonical(bytes).map_err(|e| e.to_string())?;
            Ok(())
        }
        "reject" => {
            if cbor::ensure_canonical(bytes).is_ok() {
                return Err("expected non-canonical rejection".to_string());
            }
            Ok(())
        }
        other => Err(format!("unknown expect {other}")),
    }
}

fn vector_assertion(bytes: &[u8], meta: &VectorMeta) -> Result<(), String> {
    let assertion = match AssertionPlaintext::from_cbor(bytes) {
        Ok(a) => a,
        Err(err) => {
            return match meta.expect.as_str() {
                "reject" => Ok(()),
                _ => Err(err.to_string()),
            }
        }
    };
    let sig_ok = assertion.verify_signature().map_err(|e| e.to_string())?;
    match meta.expect.as_str() {
        "accept" => {
            if !sig_ok {
                return Err("signature rejected".to_string());
            }
            Ok(())
        }
        "reject" => {
            if sig_ok {
                return Err("expected signature rejection".to_string());
            }
            Ok(())
        }
        other => Err(format!("unknown expect {other}")),
    }
}

fn vector_envelope(bytes: &[u8], meta: &VectorMeta) -> Result<(), String> {
    let env = envelope::AssertionEnvelope::from_cbor(bytes).map_err(|e| e.to_string())?;
    let key_hex = meta
        .key_hex
        .clone()
        .ok_or_else(|| "missing key".to_string())?;
    let key_bytes = hex::decode(key_hex).map_err(|e| e.to_string())?;
    let key = <[u8; 32]>::try_from(key_bytes.as_slice())
        .map_err(|_| "invalid key length".to_string())?;
    let out = envelope::decrypt_assertion(&env, &key);
    match meta.expect.as_str() {
        "decrypt" => {
            out.map_err(|e| e.to_string())?;
            Ok(())
        }
        "reject" => {
            if out.is_ok() {
                return Err("expected decrypt failure".to_string());
            }
            Ok(())
        }
        other => Err(format!("unknown expect {other}")),
    }
}

fn vector_schema(bytes: &[u8], meta: &VectorMeta) -> Result<(), String> {
    match meta.expect.as_str() {
        "accept" => {
            dharma_core::schema::parse_schema(bytes).map_err(|e| e.to_string())?;
            Ok(())
        }
        "reject" => {
            if dharma_core::schema::parse_schema(bytes).is_ok() {
                return Err("expected schema rejection".to_string());
            }
            Ok(())
        }
        other => Err(format!("unknown expect {other}")),
    }
}

fn vector_contract(bytes: &[u8], meta: &VectorMeta, data_path: &Path) -> Result<(), String> {
    let wasm = if data_path.extension().and_then(|s| s.to_str()) == Some("wat") {
        wat::parse_str(std::str::from_utf8(bytes).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?
    } else {
        bytes.to_vec()
    };
    let vm = dharma_core::runtime::vm::RuntimeVm::new(wasm);
    let env = StdEnv::new(std::env::temp_dir());
    let mut state = vec![0u8; dharma_core::runtime::vm::STATE_SIZE];
    let args = vec![0u8; 4];
    let result = vm.validate(&env, &mut state, &args, None);
    match meta.expect.as_str() {
        "accept" => result.map_err(|e| e.to_string()),
        "reject" => {
            if result.is_ok() {
                return Err("expected validate failure".to_string());
            }
            Ok(())
        }
        other => Err(format!("unknown expect {other}")),
    }
}

fn prop_cbor_roundtrip(rng: &mut ChaCha20Rng, iterations: usize) -> Result<(), String> {
    for _ in 0..iterations {
        let value = rand_value(rng, 0);
        let bytes = cbor::encode_canonical_value(&value).map_err(|e| e.to_string())?;
        let decoded = cbor::ensure_canonical(&bytes).map_err(|e| e.to_string())?;
        let re = cbor::encode_canonical_value(&decoded).map_err(|e| e.to_string())?;
        if bytes != re {
            return Err("canonical roundtrip mismatch".to_string());
        }
    }
    Ok(())
}

fn prop_cbor_rejects_noncanonical(_rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let bytes = vec![0xbf, 0x61, 0x61, 0x01, 0xff];
    if cbor::ensure_canonical(&bytes).is_ok() {
        return Err("non-canonical bytes accepted".to_string());
    }
    Ok(())
}

fn prop_assert_signature_roundtrip(rng: &mut ChaCha20Rng, iterations: usize) -> Result<(), String> {
    for _ in 0..iterations {
        let (sk, pk) = crypto::generate_identity_keypair(rng);
        let subject = SubjectId::from_bytes(rand_bytes32(rng));
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: pk,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes(rand_bytes32(rng)),
            contract: ContractId::from_bytes(rand_bytes32(rng)),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("value".to_string()),
            ciborium::value::Value::Integer((rng.next_u32() as i64).into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        if !assertion.verify_signature().map_err(|e| e.to_string())? {
            return Err("signature failed".to_string());
        }
    }
    Ok(())
}

fn prop_assert_signature_rejects_bitflip(
    rng: &mut ChaCha20Rng,
    iterations: usize,
) -> Result<(), String> {
    for _ in 0..iterations {
        let (sk, pk) = crypto::generate_identity_keypair(rng);
        let subject = SubjectId::from_bytes(rand_bytes32(rng));
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: pk,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes(rand_bytes32(rng)),
            contract: ContractId::from_bytes(rand_bytes32(rng)),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("value".to_string()),
            ciborium::value::Value::Integer(1.into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let mut tampered = assertion.clone();
        tampered.body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("value".to_string()),
            ciborium::value::Value::Integer(2.into()),
        )]);
        if tampered.verify_signature().map_err(|e| e.to_string())? {
            return Err("tampered signature accepted".to_string());
        }
    }
    Ok(())
}

fn prop_assert_structural_rejects_seq_prev(
    rng: &mut ChaCha20Rng,
    iterations: usize,
) -> Result<(), String> {
    for _ in 0..iterations {
        let (sk, pk) = crypto::generate_identity_keypair(rng);
        let subject = SubjectId::from_bytes(rand_bytes32(rng));
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: pk,
            seq: 2,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes(rand_bytes32(rng)),
            contract: ContractId::from_bytes(rand_bytes32(rng)),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let status = structural_validate(&assertion, None).map_err(|e| e.to_string())?;
        if !matches!(status, StructuralStatus::Reject(_)) {
            return Err("expected structural reject".to_string());
        }
    }
    Ok(())
}

fn prop_envelope_roundtrip(rng: &mut ChaCha20Rng, iterations: usize) -> Result<(), String> {
    for _ in 0..iterations {
        let mut key = [0u8; 32];
        rng.fill_bytes(&mut key);
        let kid = KeyId::from_bytes(rand_bytes32(rng));
        let nonce = Nonce12::from_bytes(rand_nonce(rng));
        let plaintext = rand_bytes(rng, 32);
        let env = envelope::encrypt_assertion(&plaintext, kid, &key, nonce).map_err(|e| e.to_string())?;
        let out = envelope::decrypt_assertion(&env, &key).map_err(|e| e.to_string())?;
        if out != plaintext {
            return Err("envelope roundtrip mismatch".to_string());
        }
    }
    Ok(())
}

fn prop_envelope_wrong_key(rng: &mut ChaCha20Rng, iterations: usize) -> Result<(), String> {
    for _ in 0..iterations {
        let mut key = [0u8; 32];
        rng.fill_bytes(&mut key);
        let mut bad = [0u8; 32];
        rng.fill_bytes(&mut bad);
        let kid = KeyId::from_bytes(rand_bytes32(rng));
        let nonce = Nonce12::from_bytes(rand_nonce(rng));
        let plaintext = rand_bytes(rng, 16);
        let env = envelope::encrypt_assertion(&plaintext, kid, &key, nonce).map_err(|e| e.to_string())?;
        if envelope::decrypt_assertion(&env, &bad).is_ok() {
            return Err("decrypt succeeded with wrong key".to_string());
        }
    }
    Ok(())
}

fn prop_envelope_id_changes_on_mutation(
    rng: &mut ChaCha20Rng,
    iterations: usize,
) -> Result<(), String> {
    for _ in 0..iterations {
        let mut key = [0u8; 32];
        rng.fill_bytes(&mut key);
        let kid = KeyId::from_bytes(rand_bytes32(rng));
        let nonce = Nonce12::from_bytes(rand_nonce(rng));
        let plaintext = rand_bytes(rng, 16);
        let env = envelope::encrypt_assertion(&plaintext, kid, &key, nonce).map_err(|e| e.to_string())?;
        let bytes = env.to_cbor().map_err(|e| e.to_string())?;
        let id = crypto::envelope_id(&bytes);
        let mut mutated = bytes.clone();
        if let Some(first) = mutated.first_mut() {
            *first ^= 0x01;
        }
        let id2 = crypto::envelope_id(&mutated);
        if id == id2 {
            return Err("envelope id unchanged after mutation".to_string());
        }
    }
    Ok(())
}

fn prop_dag_ordering_respects_deps(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let mut map = HashMap::new();
    let mut prev = None;
    for seq in 1..=3 {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Step".to_string(),
            auth: pk,
            seq,
            prev,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes(rand_bytes32(rng)),
            contract: ContractId::from_bytes(rand_bytes32(rng)),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let id = assertion.assertion_id().map_err(|e| e.to_string())?;
        prev = Some(id);
        map.insert(id, assertion);
    }
    let order = order_assertions(&map).map_err(|e| e.to_string())?;
    if order.len() != 3 {
        return Err("unexpected topo order length".to_string());
    }
    Ok(())
}

fn prop_dag_detects_cycle(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let header_a = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: 1,
        sub: subject,
        typ: "action.A".to_string(),
        auth: pk,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: SchemaId::from_bytes(rand_bytes32(rng)),
        contract: ContractId::from_bytes(rand_bytes32(rng)),
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let a = AssertionPlaintext::sign(header_a, ciborium::value::Value::Map(vec![]), &sk)
        .map_err(|e| e.to_string())?;
    let id_a = a.assertion_id().map_err(|e| e.to_string())?;
    let header_b = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: 1,
        sub: subject,
        typ: "action.B".to_string(),
        auth: pk,
        seq: 2,
        prev: Some(id_a),
        refs: vec![id_a],
        ts: None,
        schema: SchemaId::from_bytes(rand_bytes32(rng)),
        contract: ContractId::from_bytes(rand_bytes32(rng)),
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let mut b = AssertionPlaintext::sign(header_b, ciborium::value::Value::Map(vec![]), &sk)
        .map_err(|e| e.to_string())?;
    let id_b = b.assertion_id().map_err(|e| e.to_string())?;
    b.header.prev = Some(id_b);
    b.header.refs = vec![id_b];
    let mut map = HashMap::new();
    map.insert(id_a, a);
    map.insert(id_b, b);
    if order_assertions(&map).is_ok() {
        return Err("cycle not detected".to_string());
    }
    Ok(())
}

fn prop_store_ingest_idempotent(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let store = Store::new(&env);
    let (schema_id, contract_id) = store_cqrs_artifacts(&store)?;
    let keys = HashMap::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (assertion, subject_key) = make_assertion(rng, 1, None, schema_id, contract_id)?;
    let bytes = wrap_envelope(&assertion, &subject_key)?;
    let mut keys = HashMap::new();
    keys.insert(assertion.header.sub, subject_key);
    let first = ingest_object(&store, &mut index, &bytes, &keys).map_err(|e| format!("{e:?}"))?;
    if !matches!(first, IngestStatus::Accepted(_) | IngestStatus::Pending(_, _)) {
        return Err("first ingest unexpected status".to_string());
    }
    let second = ingest_object(&store, &mut index, &bytes, &keys).map_err(|e| format!("{e:?}"))?;
    if !matches!(second, IngestStatus::Accepted(_) | IngestStatus::Pending(_, _)) {
        return Err("second ingest unexpected status".to_string());
    }
    Ok(())
}

fn prop_store_frontier_deterministic(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let store = Store::new(&env);
    let (schema_id, contract_id) = store_cqrs_artifacts(&store)?;
    let keys = HashMap::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (assertion, subject_key) = make_assertion(rng, 1, None, schema_id, contract_id)?;
    let bytes = wrap_envelope(&assertion, &subject_key)?;
    let mut keys = HashMap::new();
    keys.insert(assertion.header.sub, subject_key);
    ingest_object(&store, &mut index, &bytes, &keys).map_err(|e| format!("{e:?}"))?;
    let tips_a = index.get_tips(&assertion.header.sub).to_vec();
    let store_b = Store::new(&env);
    let index_b = FrontierIndex::build(&store_b, &HashMap::new()).map_err(|e| e.to_string())?;
    let tips_b = index_b.get_tips(&assertion.header.sub).to_vec();
    if tips_a != tips_b {
        return Err("frontier tips mismatch".to_string());
    }
    Ok(())
}

fn prop_replay_deterministic(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let env_a = SimEnv::new(rng.next_u64(), 0);
    let env_b = SimEnv::new(rng.next_u64(), 0);
    let store_a = Store::new(&env_a);
    let store_b = Store::new(&env_b);
    let (schema_id, contract_id) = store_cqrs_artifacts(&store_a)?;
    let _ = store_cqrs_artifacts(&store_b)?;
    let (schema, wasm) = cqrs_schema_and_wasm();
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let mut prev = None;
    for seq in 1..=5 {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: pk,
            seq,
            prev,
            refs: vec![],
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: None,
            meta: add_signer_meta(None, &subject),
        };
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("value".to_string()),
            ciborium::value::Value::Integer((seq as i64).into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
        let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
        let env_id = crypto::envelope_id(&bytes);
        append_assertion(&env_a, &subject, seq, assertion_id, env_id, "action.Set", &bytes)
            .map_err(|e| e.to_string())?;
        prev = Some(assertion_id);
    }
    for record in list_assertions(&env_a, &subject).map_err(|e| e.to_string())? {
        append_assertion(
            &env_b,
            &subject,
            record.seq,
            record.assertion_id,
            record.envelope_id,
            "action.Set",
            &record.bytes,
        )
        .map_err(|e| e.to_string())?;
    }
    let state_a = load_state(&env_a, &subject, &schema, &wasm, 1).map_err(|e| e.to_string())?;
    let state_b = load_state(&env_b, &subject, &schema, &wasm, 1).map_err(|e| e.to_string())?;
    if state_a.memory != state_b.memory {
        return Err("replay state mismatch".to_string());
    }
    Ok(())
}

fn prop_convergence(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    struct Node {
        env: SimEnv,
        subject: SubjectId,
    }

    let mut nodes = Vec::new();
    for _ in 0..3 {
        let env = SimEnv::new(rng.next_u64(), 0);
        let subject = SubjectId::from_bytes(rand_bytes32(rng));
        for seq in 1..=3 {
            let (sk, pk) = crypto::generate_identity_keypair(rng);
            let header = AssertionHeader {
                v: crypto::PROTOCOL_VERSION,
                ver: 1,
                sub: subject,
                typ: "action.Note".to_string(),
                auth: pk,
                seq,
                prev: None,
                refs: vec![],
                ts: None,
                schema: SchemaId::from_bytes([1u8; 32]),
                contract: ContractId::from_bytes([2u8; 32]),
                note: None,
                meta: add_signer_meta(None, &subject),
            };
            let body = ciborium::value::Value::Map(vec![(
                ciborium::value::Value::Text("text".to_string()),
                ciborium::value::Value::Text(rand_text(rng, 8)),
            )]);
            let assertion =
                AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
            let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
            let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
            let env_id = crypto::envelope_id(&bytes);
            append_assertion(&env, &subject, seq, assertion_id, env_id, "action.Note", &bytes)
                .map_err(|e| e.to_string())?;
        }
        nodes.push(Node { env, subject });
    }

    for idx in 0..nodes.len() {
        let subject = nodes[idx].subject;
        let records = list_assertions(&nodes[idx].env, &subject).map_err(|e| e.to_string())?;
        for node in &nodes {
            for record in &records {
                append_assertion(
                    &node.env,
                    &subject,
                    record.seq,
                    record.assertion_id,
                    record.envelope_id,
                    "action.Note",
                    &record.bytes,
                )
                .map_err(|e| e.to_string())?;
            }
        }
    }

    let expected = snapshot_env(&nodes[0].env)?;
    for node in nodes.iter().skip(1) {
        let snapshot = snapshot_env(&node.env)?;
        if snapshot != expected {
            return Err("convergence mismatch".to_string());
        }
    }
    Ok(())
}

fn prop_cqrs_replay_deterministic(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let (schema, wasm) = cqrs_schema_and_wasm();
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let mut prev = None;
    for seq in 1..=3 {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: pk,
            seq,
            prev,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("value".to_string()),
            ciborium::value::Value::Integer((seq as i64).into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
        let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
        let env_id = crypto::envelope_id(&bytes);
        append_assertion(&env, &subject, seq, assertion_id, env_id, "action.Set", &bytes)
            .map_err(|e| e.to_string())?;
        prev = Some(assertion_id);
    }
    let state_a = load_state(&env, &subject, &schema, &wasm, 1).map_err(|e| e.to_string())?;
    let state_b = load_state(&env, &subject, &schema, &wasm, 1).map_err(|e| e.to_string())?;
    if state_a.memory != state_b.memory {
        return Err("state memory mismatch".to_string());
    }
    Ok(())
}

fn prop_cqrs_decode_deterministic(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let (schema, wasm) = cqrs_schema_and_wasm();
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: 1,
        sub: subject,
        typ: "action.Set".to_string(),
        auth: pk,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: SchemaId::from_bytes([1u8; 32]),
        contract: ContractId::from_bytes([2u8; 32]),
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let body = ciborium::value::Value::Map(vec![(
        ciborium::value::Value::Text("value".to_string()),
        ciborium::value::Value::Integer(5.into()),
    )]);
    let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
    let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
    let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
    let env_id = crypto::envelope_id(&bytes);
    append_assertion(&env, &subject, 1, assertion_id, env_id, "action.Set", &bytes)
        .map_err(|e| e.to_string())?;
    let state = load_state(&env, &subject, &schema, &wasm, 1).map_err(|e| e.to_string())?;
    let decoded_a = decode_state(&state.memory, &schema).map_err(|e| e.to_string())?;
    let decoded_b = decode_state(&state.memory, &schema).map_err(|e| e.to_string())?;
    if decoded_a != decoded_b {
        return Err("decode mismatch".to_string());
    }
    Ok(())
}

fn prop_dharmaq_rebuild_deterministic(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    for seq in 1..=3 {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Note".to_string(),
            auth: pk,
            seq,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("text".to_string()),
            ciborium::value::Value::Text("hello".to_string()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
        let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
        let env_id = crypto::envelope_id(&bytes);
        append_assertion(&env, &subject, seq, assertion_id, env_id, "action.Note", &bytes)
            .map_err(|e| e.to_string())?;
    }
    dharmaq::rebuild_env(&env).map_err(|e| e.to_string())?;
    let results_a = dharmaq::search_env(&env, "hello", 10).map_err(|e| e.to_string())?;
    dharmaq::rebuild_env(&env).map_err(|e| e.to_string())?;
    let results_b = dharmaq::search_env(&env, "hello", 10).map_err(|e| e.to_string())?;
    if rows_key(&results_a) != rows_key(&results_b) {
        return Err("rebuild results mismatch".to_string());
    }
    Ok(())
}

fn prop_dharmaq_and_commutative(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    for seq in 1..=3 {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Note".to_string(),
            auth: pk,
            seq,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
        let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
        let env_id = crypto::envelope_id(&bytes);
        append_assertion(&env, &subject, seq, assertion_id, env_id, "action.Note", &bytes)
            .map_err(|e| e.to_string())?;
    }
    dharmaq::rebuild_env(&env).map_err(|e| e.to_string())?;
    let plan_a = QueryPlan {
        table: "assertions".to_string(),
        filter: Some(Filter::And(vec![
            Filter::Leaf(Predicate::Seq {
                op: CmpOp::Gte,
                value: 1,
            }),
            Filter::Leaf(Predicate::TypEq("action.Note".to_string())),
        ])),
        limit: 10,
    };
    let plan_b = QueryPlan {
        table: "assertions".to_string(),
        filter: Some(Filter::And(vec![
            Filter::Leaf(Predicate::TypEq("action.Note".to_string())),
            Filter::Leaf(Predicate::Seq {
                op: CmpOp::Gte,
                value: 1,
            }),
        ])),
        limit: 10,
    };
    let results_a = dharmaq::execute_env(&env, &plan_a).map_err(|e| e.to_string())?;
    let results_b = dharmaq::execute_env(&env, &plan_b).map_err(|e| e.to_string())?;
    if rows_key(&results_a) != rows_key(&results_b) {
        return Err("AND results differ".to_string());
    }
    Ok(())
}

fn write_ticket(failure: &TestFailure, opts: &TestOptions) -> Result<PathBuf, DharmaError> {
    let dir = PathBuf::from("tests").join("failures");
    fs::create_dir_all(&dir)?;
    let name = format!(
        "DHARMA-FAILURE-{}-{}.md",
        failure.seed,
        failure.property.replace(':', "_")
    );
    let path = dir.join(name);
    let contents = format_ticket(failure, opts);
    fs::write(&path, contents)?;
    Ok(path)
}

fn format_ticket(failure: &TestFailure, opts: &TestOptions) -> String {
    let mut contents = format!(
        "# DHARMA Failure\n\n- Property: {}\n- Seed: {}\n- Deep: {}\n- Chaos: {}\n- CI: {}\n\n## Details\n{}\n",
        failure.property, failure.seed, opts.deep, opts.chaos, opts.ci, failure.details
    );
    contents.push_str("\n## Trace\n");
    match &failure.trace {
        Some(trace) if !trace.is_empty() => {
            for line in trace {
                contents.push_str("- ");
                contents.push_str(line);
                contents.push('\n');
            }
        }
        Some(_) => {
            contents.push_str("- <empty>\n");
        }
        None => {
            contents.push_str("- <none>\n");
        }
    }
    contents
}

#[cfg(test)]
mod ticket_tests {
    use super::*;

    #[test]
    fn format_ticket_includes_trace_section() {
        let failure = TestFailure {
            property: "P-SIM-001",
            seed: 7,
            details: "boom".to_string(),
            trace: None,
        };
        let opts = TestOptions {
            deep: true,
            chaos: false,
            ci: true,
            replay_seed: None,
            relay_only: false,
        };
        let content = format_ticket(&failure, &opts);
        assert!(content.contains("## Trace"));
        assert!(content.contains("<none>"));
    }

    #[test]
    fn format_ticket_renders_trace_lines() {
        let failure = TestFailure {
            property: "P-SIM-001",
            seed: 9,
            details: "boom".to_string(),
            trace: Some(vec!["line-a".to_string(), "line-b".to_string()]),
        };
        let opts = TestOptions {
            deep: true,
            chaos: true,
            ci: false,
            replay_seed: None,
            relay_only: false,
        };
        let content = format_ticket(&failure, &opts);
        assert!(content.contains("- line-a"));
        assert!(content.contains("- line-b"));
    }
}

fn rand_bytes(rng: &mut ChaCha20Rng, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    rng.fill_bytes(&mut out);
    out
}

fn rand_nonce(rng: &mut ChaCha20Rng) -> [u8; 12] {
    let mut out = [0u8; 12];
    rng.fill_bytes(&mut out);
    out
}

fn rand_bytes32(rng: &mut ChaCha20Rng) -> [u8; 32] {
    let mut out = [0u8; 32];
    rng.fill_bytes(&mut out);
    out
}

fn rand_value(rng: &mut ChaCha20Rng, depth: usize) -> ciborium::value::Value {
    let choice = (rng.next_u32() % 5) as usize;
    if depth > 2 {
        return match choice % 3 {
            0 => ciborium::value::Value::Integer((rng.next_u32() as i64).into()),
            1 => ciborium::value::Value::Bool((rng.next_u32() % 2) == 0),
            _ => ciborium::value::Value::Text(rand_text(rng, 8)),
        };
    }
    match choice {
        0 => ciborium::value::Value::Integer((rng.next_u32() as i64).into()),
        1 => ciborium::value::Value::Bool((rng.next_u32() % 2) == 0),
        2 => ciborium::value::Value::Text(rand_text(rng, 8)),
        3 => {
            let len = (rng.next_u32() % 4) as usize;
            let mut items = Vec::new();
            for _ in 0..len {
                items.push(rand_value(rng, depth + 1));
            }
            ciborium::value::Value::Array(items)
        }
        _ => {
            let len = (rng.next_u32() % 3) as usize;
            let mut entries = BTreeMap::new();
            for _ in 0..len {
                entries.insert(rand_text(rng, 6), rand_value(rng, depth + 1));
            }
            let mut map = Vec::new();
            for (k, v) in entries {
                map.push((ciborium::value::Value::Text(k), v));
            }
            ciborium::value::Value::Map(map)
        }
    }
}

fn rand_text(rng: &mut ChaCha20Rng, max: usize) -> String {
    let len = (rng.next_u32() as usize % max).max(1);
    let mut s = String::new();
    for _ in 0..len {
        let c = b'a' + (rng.next_u32() % 26) as u8;
        s.push(c as char);
    }
    s
}

fn rows_key(rows: &[dharmaq::QueryRow]) -> Vec<(AssertionId, SubjectId, u64, u32)> {
    rows
        .iter()
        .map(|row| (row.assertion_id, row.subject, row.seq, row.score))
        .collect()
}

fn snapshot_env(env: &SimEnv) -> Result<Vec<(SubjectId, u64, AssertionId)>, String> {
    let store = Store::new(env);
    let mut out = Vec::new();
    let mut subjects = store.list_subjects().map_err(|e| e.to_string())?;
    subjects.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    for subject in subjects {
        for record in list_assertions(env, &subject).map_err(|e| e.to_string())? {
            out.push((subject, record.seq, record.assertion_id));
        }
    }
    out.sort_by(|a, b| {
        a.0.as_bytes()
            .cmp(b.0.as_bytes())
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.as_bytes().cmp(b.2.as_bytes()))
    });
    Ok(out)
}

fn diff_snapshots(
    expected: &[(SubjectId, u64, AssertionId)],
    actual: &[(SubjectId, u64, AssertionId)],
) -> String {
    use std::collections::BTreeSet;
    let exp: BTreeSet<_> = expected.iter().copied().collect();
    let act: BTreeSet<_> = actual.iter().copied().collect();
    let missing: Vec<_> = exp.difference(&act).cloned().collect();
    let extra: Vec<_> = act.difference(&exp).cloned().collect();
    let mut parts = Vec::new();
    if !missing.is_empty() {
        parts.push(format!(
            "missing {} entries (first {:?})",
            missing.len(),
            missing.first()
        ));
    }
    if !extra.is_empty() {
        parts.push(format!(
            "extra {} entries (first {:?})",
            extra.len(),
            extra.first()
        ));
    }
    if parts.is_empty() {
        "snapshots differ".to_string()
    } else {
        parts.join("; ")
    }
}

fn cqrs_schema_and_wasm() -> (CqrsSchema, Vec<u8>) {
    let mut fields = BTreeMap::new();
    fields.insert(
        "total".to_string(),
        FieldSchema {
            typ: TypeSpec::Int,
            default: Some(ciborium::value::Value::Integer(0.into())),
            visibility: Visibility::Public,
        },
    );
    let mut actions = BTreeMap::new();
    let mut args = BTreeMap::new();
    args.insert("value".to_string(), TypeSpec::Int);
    let mut arg_vis = BTreeMap::new();
    arg_vis.insert("value".to_string(), Visibility::Public);
    actions.insert(
        "Set".to_string(),
        ActionSchema {
            args,
            arg_vis,
            doc: None,
        },
    );
    let schema = CqrsSchema {
        namespace: "test".to_string(),
        version: "1.0.0".to_string(),
        aggregate: "Dummy".to_string(),
        extends: None,
        fields,
        actions,
        concurrency: ConcurrencyMode::Strict,
    };
    let wasm = wat::parse_str(
        r#"(module
            (memory (export "memory") 1)
            (func (export "validate") (result i32)
              i32.const 0)
            (func (export "reduce") (result i32)
              (local $val i64)
              i32.const 0x2004
              i64.load
              local.set $val
              i32.const 0
              local.get $val
              i64.store
              i32.const 0)
          )"#,
    )
    .unwrap();
    (schema, wasm)
}

fn make_assertion(
    rng: &mut ChaCha20Rng,
    seq: u64,
    prev: Option<AssertionId>,
    schema: SchemaId,
    contract: ContractId,
) -> Result<(AssertionPlaintext, [u8; 32]), String> {
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: 1,
        sub: subject,
        typ: "action.Set".to_string(),
        auth: pk,
        seq,
        prev,
        refs: vec![],
        ts: None,
        schema,
        contract,
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let body = ciborium::value::Value::Map(vec![]);
    let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
    let mut key = [0u8; 32];
    rng.fill_bytes(&mut key);
    Ok((assertion, key))
}

fn wrap_envelope(assertion: &AssertionPlaintext, key: &[u8; 32]) -> Result<Vec<u8>, String> {
    let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
    let kid = KeyId::from_bytes([7u8; 32]);
    let nonce = Nonce12::from_bytes([3u8; 12]);
    let env = envelope::encrypt_assertion(&bytes, kid, key, nonce).map_err(|e| e.to_string())?;
    env.to_cbor().map_err(|e| e.to_string())
}

fn store_cqrs_artifacts(store: &Store) -> Result<(SchemaId, ContractId), String> {
    let (schema, wasm) = cqrs_schema_and_wasm();
    let schema_bytes = schema.to_cbor().map_err(|e| e.to_string())?;
    let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
    let schema_obj = EnvelopeId::from_bytes(*schema_id.as_bytes());
    store
        .put_object(&schema_obj, &schema_bytes)
        .map_err(|e| e.to_string())?;
    let contract_id = ContractId::from_bytes(crypto::sha256(&wasm));
    let contract_obj = EnvelopeId::from_bytes(*contract_id.as_bytes());
    store
        .put_object(&contract_obj, &wasm)
        .map_err(|e| e.to_string())?;
    Ok((schema_id, contract_id))
}

struct TestNode {
    dir: TempDir,
    env: StdEnv,
    store: Store,
    identity: IdentityState,
    schema_id: SchemaId,
    contract_id: ContractId,
    subjects: HashMap<SubjectId, (u64, Option<AssertionId>)>,
}

impl TestNode {
    fn new(name: &str, rng: &mut ChaCha20Rng) -> Result<Self, String> {
        let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
        let env = StdEnv::new(dir.path());
        identity_store::init_identity(&env, name, "test-pass")
            .map_err(|e| e.to_string())?;
        let identity = identity_store::load_identity(&env, "test-pass")
            .map_err(|e| e.to_string())?;
        let store = Store::new(&env);
        let (schema_id, contract_id) = store_cqrs_artifacts(&store)?;
        let mut subjects = HashMap::new();
        // Reserve a deterministic subject for this node.
        let subject = SubjectId::from_bytes(rand_bytes32(rng));
        subjects.insert(subject, (0, None));
        Ok(Self {
            dir,
            env,
            store,
            identity,
            schema_id,
            contract_id,
            subjects,
        })
    }

    fn root(&self) -> &Path {
        self.dir.path()
    }

    fn new_subject(&mut self, rng: &mut ChaCha20Rng) -> SubjectId {
        let subject = SubjectId::from_bytes(rand_bytes32(rng));
        self.subjects.insert(subject, (0, None));
        subject
    }

    fn write_set(&mut self, subject: SubjectId, value: i64) -> Result<AssertionId, String> {
        let (seq, prev) = self
            .subjects
            .get(&subject)
            .copied()
            .unwrap_or((0, None));
        let next_seq = seq + 1;
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: self.identity.root_public_key,
            seq: next_seq,
            prev,
            refs: vec![],
            ts: None,
            schema: self.schema_id,
            contract: self.contract_id,
            note: None,
            meta: add_signer_meta(None, &self.identity.subject_id),
        };
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("value".to_string()),
            ciborium::value::Value::Integer(value.into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &self.identity.root_signing_key)
            .map_err(|e| e.to_string())?;
        let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
        let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
        let envelope_id = crypto::envelope_id(&bytes);
        self.store
            .put_object(&envelope_id, &bytes)
            .map_err(|e| e.to_string())?;
        self.store
            .record_semantic(&assertion_id, &envelope_id)
            .map_err(|e| e.to_string())?;
        append_assertion(
            &self.env,
            &subject,
            next_seq,
            assertion_id,
            envelope_id,
            "Set",
            &bytes,
        )
        .map_err(|e| e.to_string())?;
        self.subjects.insert(subject, (next_seq, Some(assertion_id)));
        Ok(assertion_id)
    }

    fn start_relay(
        &self,
        port: u16,
        verbose: bool,
        trace: Option<Arc<Mutex<Vec<String>>>>,
        shutdown: Arc<AtomicBool>,
    ) -> thread::JoinHandle<()> {
        let addr = format!("127.0.0.1:{port}");
        let listener = TcpListener::bind(&addr).expect("bind relay");
        let identity = self.identity.clone();
        let store = self.store.clone();
        let options = server::ServerOptions {
            relay: true,
            verbose,
            trace,
            ..Default::default()
        };
        thread::spawn(move || {
            let _ = server::listen_with_shutdown(listener, identity, store, options, shutdown);
        })
    }

    fn connect_and_sync(
        &self,
        addr: &str,
        subject_override: Option<SubjectId>,
        trace: Option<Arc<Mutex<Vec<String>>>>,
    ) -> Result<(), String> {
        let identity = self.identity.clone();
        let store = self.store.clone();
        let addr = addr.to_string();
        let (tx, rx) = mpsc::channel();
        if let Some(trace) = &trace {
            if let Ok(mut guard) = trace.lock() {
                guard.push(format!("connect start addr={addr}"));
            }
        }
        let trace_for_thread = trace.clone();
        thread::spawn(move || {
            let result =
                connect_and_sync_once(identity, store, &addr, subject_override, trace_for_thread);
            let _ = tx.send(result);
        });
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(result) => result,
            Err(_) => {
                if let Some(trace) = &trace {
                    if let Ok(mut guard) = trace.lock() {
                        guard.push("sync timeout".to_string());
                    }
                }
                Err("sync timeout".to_string())
            }
        }
    }

    fn list_subjects(&self) -> Result<Vec<SubjectId>, String> {
        self.store.list_subjects().map_err(|e| e.to_string())
    }

    fn list_assertions(&self, subject: SubjectId) -> Result<Vec<AssertionId>, String> {
        self.store.scan_subject(&subject).map_err(|e| e.to_string())
    }

    fn frontiers(&self, subject: SubjectId) -> Result<Vec<AssertionId>, String> {
        let mut keys = HashMap::new();
        keys.insert(self.identity.subject_id, self.identity.subject_key);
        let index = FrontierIndex::build(&self.store, &keys).map_err(|e| e.to_string())?;
        Ok(index.get_tips(&subject))
    }

    fn pending(&self) -> Result<Vec<AssertionId>, String> {
        let mut keys = HashMap::new();
        keys.insert(self.identity.subject_id, self.identity.subject_key);
        let index = FrontierIndex::build(&self.store, &keys).map_err(|e| e.to_string())?;
        Ok(index.pending_objects())
    }
}

fn connect_and_sync_once(
    identity: IdentityState,
    store: Store,
    addr: &str,
    subject_override: Option<SubjectId>,
    trace: Option<Arc<Mutex<Vec<String>>>>,
) -> Result<(), String> {
    let mut stream = TcpStream::connect(addr).map_err(|e| e.to_string())?;
    if let Some(trace) = &trace {
        if let Ok(mut guard) = trace.lock() {
            guard.push("handshake start".to_string());
        }
    }
    let session = client_handshake(&mut stream, &identity).map_err(|e| e.to_string())?;
    if let Some(trace) = &trace {
        if let Ok(mut guard) = trace.lock() {
            guard.push("handshake ok".to_string());
        }
    }
    let mut keys = HashMap::new();
    keys.insert(identity.subject_id, identity.subject_key);
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let policy = OverlayPolicy::load(store.root());
    let claims = PeerClaims::default();
    let access = OverlayAccess::new(&policy, None, false, &claims);
    let local_subs = subject_override.map(|subject| Subscriptions {
        all: false,
        subjects: vec![subject],
        namespaces: Vec::new(),
    });
    let options = net::sync::SyncOptions {
        relay: false,
        ad_store: None,
        local_subs,
        verbose: false,
        exit_on_idle: true,
        trace,
    };
    net::sync::sync_loop_with(
        &mut stream,
        session,
        &store,
        &mut index,
        &keys,
        &identity,
        &access,
        options,
    )
    .map_err(|e| e.to_string())
}

fn prop_relay_baseline(rng: &mut ChaCha20Rng, trace: &mut Vec<String>) -> Result<(), String> {
    let mut relay = TestNode::new("relay", rng)?;
    let node_b = TestNode::new("node-b", rng)?;
    let node_c = TestNode::new("node-c", rng)?;
    let sync_trace = Arc::new(Mutex::new(Vec::new()));
    let relay_trace = Arc::new(Mutex::new(Vec::new()));
    let flush_traces = |trace: &mut Vec<String>| {
        if let Ok(mut guard) = sync_trace.lock() {
            trace.extend(guard.drain(..));
        }
        if let Ok(mut guard) = relay_trace.lock() {
            for line in guard.drain(..) {
                trace.push(format!("relay: {line}"));
            }
        }
    };

    trace.push(format!("node_b_root {}", node_b.root().display()));
    trace.push(format!("node_c_root {}", node_c.root().display()));

    let port = free_port()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let handle = relay.start_relay(port, false, Some(relay_trace.clone()), shutdown.clone());
    let addr = format!("127.0.0.1:{port}");
    trace.push(format!("relay_started addr={addr}"));

    let result = (|| -> Result<(), String> {
        let subject = relay.new_subject(rng);
        trace.push(format!("subject {}", subject.to_hex()));
        relay.write_set(subject, 10)?;
        relay.write_set(subject, 11)?;
        relay.write_set(subject, 12)?;
        trace.push("relay_writes seq=3".to_string());

        node_b.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        node_c.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        trace.push("nodes_synced".to_string());

        assert_converged(&[&relay, &node_b, &node_c], trace)?;

        let subject_b = relay.new_subject(rng);
        relay.write_set(subject_b, 21)?;
        relay.write_set(subject_b, 22)?;
        trace.push(format!("second_subject {}", subject_b.to_hex()));
        node_b.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        node_c.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        assert_converged(&[&relay, &node_b, &node_c], trace)?;

        // Interleaved update
        relay.write_set(subject, 13)?;
        node_b.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        node_c.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        trace.push("nodes_resynced".to_string());
        assert_converged(&[&relay, &node_b, &node_c], trace)?;

        Ok(())
    })();

    flush_traces(trace);
    shutdown.store(true, Ordering::SeqCst);
    let _ = handle.join();
    result
}

fn assert_converged(nodes: &[&TestNode], trace: &mut Vec<String>) -> Result<(), String> {
    let Some(first) = nodes.first() else {
        return Ok(());
    };
    let baseline_subjects = first.list_subjects()?;
    let mut baseline_set: BTreeMap<SubjectId, Vec<AssertionId>> = BTreeMap::new();
    for subject in &baseline_subjects {
        let mut ids = first.list_assertions(*subject)?;
        ids.sort();
        baseline_set.insert(*subject, ids);
    }
    for node in nodes.iter().skip(1) {
        let subjects = node.list_subjects()?;
        if subjects.len() != baseline_subjects.len() {
            return Err("subject count mismatch".to_string());
        }
        for subject in &baseline_subjects {
            let mut ids = node.list_assertions(*subject)?;
            ids.sort();
            let baseline = baseline_set.get(subject).cloned().unwrap_or_default();
            if ids != baseline {
                return Err(format!("assertion mismatch for {}", subject.to_hex()));
            }
            let mut tips = node.frontiers(*subject)?;
            tips.sort();
            let mut baseline_tips = first.frontiers(*subject)?;
            baseline_tips.sort();
            if tips != baseline_tips {
                return Err(format!("frontier mismatch for {}", subject.to_hex()));
            }
        }
        let pending = node.pending()?;
        if !pending.is_empty() {
            trace.push(format!("pending={}", pending.len()));
            return Err("pending objects present".to_string());
        }
    }
    Ok(())
}

fn free_port() -> Result<u16, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    Ok(port)
}
