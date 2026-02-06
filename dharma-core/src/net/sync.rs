use crate::assertion::{is_overlay, AssertionPlaintext};
use crate::assertion_types::ACTION_PREFIX;
use crate::error::DharmaError;
use crate::fabric::types::AdStore;
use crate::identity::IdentityState;
use crate::keys;
use crate::keys::Keyring;
use crate::net::handshake::Session;
use crate::net::ingest::{
    ingest_object, ingest_object_relay, retry_pending, IngestError, IngestStatus, RelayIngestStatus,
};
use crate::net::io::ReadWrite;
use crate::net::policy::OverlayAccess;
use crate::net::subscriptions::load_subscriptions;
use crate::net::trust::PeerPolicy;
use crate::pdl::schema::CqrsSchema;
use crate::store::index::FrontierIndex;
use crate::store::state::{
    find_assertion_by_id, find_overlay_by_id, list_assertions, list_overlays, overlays_for_ref,
};
use crate::store::Store;
use crate::sync::{
    Ads, Get, GetAds, Hello, Inventory, Obj, ObjectRef, SubjectInventory, Subscriptions,
    SyncMessage,
};
use crate::types::{AssertionId, EnvelopeId, SchemaId, SubjectId};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn};

const TYPE_INV: u8 = 10;
const TYPE_GET: u8 = 11;
const TYPE_OBJ: u8 = 12;
const TYPE_ERR: u8 = 13;
const TYPE_AD: u8 = 14;
const TYPE_ADS: u8 = 15;
const TYPE_GET_ADS: u8 = 16;

#[derive(Clone, Debug)]
pub struct SyncOptions {
    pub relay: bool,
    pub ad_store: Option<Arc<Mutex<AdStore>>>,
    pub local_subs: Option<Subscriptions>,
    pub verbose: bool,
    pub exit_on_idle: bool,
    pub trace: Option<Arc<Mutex<Vec<String>>>>,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            relay: false,
            ad_store: None,
            local_subs: None,
            verbose: false,
            exit_on_idle: false,
            trace: None,
        }
    }
}

enum SyncIngestOutcome {
    Accepted {
        envelope_id: Option<EnvelopeId>,
        assertion_id: Option<AssertionId>,
    },
    Pending {
        assertion_id: AssertionId,
        reason: String,
    },
}

pub fn sync_loop(
    stream: &mut dyn ReadWrite,
    session: Session,
    store: &Store,
    index: &mut FrontierIndex,
    keys: &mut Keyring,
    identity: &IdentityState,
    overlay: &OverlayAccess,
) -> Result<(), DharmaError> {
    sync_loop_with(
        stream,
        session,
        store,
        index,
        keys,
        identity,
        overlay,
        SyncOptions::default(),
    )
}

pub fn sync_loop_with(
    stream: &mut dyn ReadWrite,
    mut session: Session,
    store: &Store,
    index: &mut FrontierIndex,
    keys: &mut Keyring,
    identity: &IdentityState,
    overlay: &OverlayAccess,
    options: SyncOptions,
) -> Result<(), DharmaError> {
    let mut saw_inventory = false;
    let local_subs = options
        .local_subs
        .clone()
        .unwrap_or_else(|| load_subscriptions(store.root()));
    let peer_hello = exchange_hello(stream, &mut session, identity, &local_subs)?;
    {
        let subject = peer_hello
            .subject
            .map(|s| s.to_hex())
            .unwrap_or_else(|| "-".to_string());
        let subs_all = peer_hello.subs.as_ref().map(|s| s.all).unwrap_or(true);
        if options.verbose {
            info!(
                peer_id = %peer_hello.peer_id.to_hex(),
                subject_id = %subject,
                subs_all,
                "sync hello"
            );
        }
        log_sync(
            &options,
            format!(
                "sync: hello peer_id={} subject={} subs_all={}",
                peer_hello.peer_id.to_hex(),
                subject,
                subs_all
            ),
        );
    }
    if let (Some(expected), Some(actual)) = (overlay.peer(), peer_hello.subject) {
        if expected != actual {
            return Err(DharmaError::Validation("peer subject mismatch".to_string()));
        }
    }
    if let Some(peer_subject) = peer_hello.subject.or_else(|| overlay.peer()) {
        let policy = PeerPolicy::load(store.root());
        if !policy.allows(peer_subject, peer_hello.peer_id) {
            return Err(DharmaError::Validation("peer denied by policy".to_string()));
        }
    }
    let peer_subs = peer_hello.subs.unwrap_or_else(Subscriptions::all);
    if options.relay {
        send_inv(stream, &mut session, store, index, overlay, &peer_subs)?;
        log_sync(&options, "sync: sent inv subjects");
        send_inv_objects(stream, &mut session, store)?;
        log_sync(&options, "sync: sent inv objects");
    } else {
        send_inv(stream, &mut session, store, index, overlay, &peer_subs)?;
        log_sync(&options, "sync: sent inv subjects");
    }
    if options.ad_store.is_some() {
        let _ = send_msg(stream, &mut session, &SyncMessage::GetAds(GetAds));
    }

    let mut pending_get: HashSet<ObjectRef> = HashSet::new();
    let mut pending_subjects: HashMap<ObjectRef, SubjectId> = HashMap::new();
    loop {
        if options.exit_on_idle && saw_inventory && pending_get.is_empty() {
            log_sync(&options, "sync: complete (no pending)");
            return Ok(());
        }
        let frame = match crate::net::codec::read_frame_optional(stream) {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(DharmaError::Network(msg)) => {
                if is_graceful_close(&msg) {
                    log_sync(&options, format!("sync: peer closed ({msg})"));
                    return Ok(());
                }
                if is_idle_network_error(&msg) {
                    log_sync(&options, format!("sync: idle ({msg})"));
                    if options.exit_on_idle {
                        return Ok(());
                    }
                    std::thread::sleep(Duration::from_millis(50));
                    continue;
                }
                log_sync(&options, format!("sync: network glitch ({msg})"));
                return Err(DharmaError::Network(msg));
            }
            Err(err) => return Err(err),
        };
        let (t, payload) = session.decrypt(&frame)?;
        let msg = SyncMessage::from_cbor(&payload)?;
        match msg {
            SyncMessage::Inv(inv) => {
                saw_inventory = true;
                match &inv {
                    Inventory::Subjects(subjects) => {
                        log_sync(
                            &options,
                            format!("sync: recv inv subjects={}", subjects.len()),
                        );
                    }
                    Inventory::Objects(objects) => {
                        log_sync(
                            &options,
                            format!("sync: recv inv objects={}", objects.len()),
                        );
                    }
                }
                if let Some(delta) =
                    delta_inventory_from_peer(&inv, store, index, overlay, &peer_subs)?
                {
                    log_sync(&options, "sync: sending delta inv");
                    send_msg(stream, &mut session, &SyncMessage::Inv(delta))?;
                }
                let mut subject_map: HashMap<AssertionId, SubjectId> = HashMap::new();
                if let Inventory::Subjects(subjects) = &inv {
                    for subject in subjects {
                        for tip in &subject.frontier {
                            subject_map.insert(*tip, subject.sub);
                        }
                        for tip in &subject.overlay {
                            subject_map.insert(*tip, subject.sub);
                        }
                    }
                }
                let missing =
                    handle_inv(&inv, store, index, &mut pending_get, overlay, &local_subs);
                if options.relay {
                    for obj in &missing {
                        if let ObjectRef::Assertion(assertion_id) = obj {
                            if let Some(subject) = subject_map.get(assertion_id) {
                                pending_subjects.insert(obj.clone(), *subject);
                            }
                        }
                    }
                }
                if !missing.is_empty() {
                    log_sync(&options, format!("sync: send get ids={}", missing.len()));
                    send_get(stream, &mut session, missing)?;
                }
            }
            SyncMessage::Get(get) => {
                log_sync(&options, format!("sync: recv get ids={}", get.ids.len()));
                for obj in handle_get(store, get, overlay, &peer_subs)? {
                    log_sync(
                        &options,
                        format!("sync: send obj {}", object_ref_hex(&obj.id)),
                    );
                    send_obj(stream, &mut session, obj)?;
                }
            }
            SyncMessage::Obj(obj) => {
                log_sync(
                    &options,
                    format!("sync: recv obj {}", object_ref_hex(&obj.id)),
                );
                let envelope_id = crate::crypto::envelope_id(&obj.bytes);
                if crate::store::looks_like_wasm(&obj.bytes) {
                    store.verify_contract_bytes(&envelope_id, &obj.bytes)?;
                    let _ = store.put_object(&envelope_id, &obj.bytes);
                    pending_get.remove(&obj.id);
                    pending_get.remove(&ObjectRef::Envelope(envelope_id));
                    let _ = retry_pending(store, index, keys);
                } else {
                    let subject_hint = if options.relay {
                        pending_subjects.remove(&obj.id)
                    } else {
                        None
                    };
                    let result = if options.relay {
                        ingest_sync_object_relay(
                            store,
                            index,
                            identity,
                            envelope_id,
                            &obj.bytes,
                            subject_hint.clone(),
                        )
                    } else {
                        ingest_sync_object_normal(store, index, keys, envelope_id, &obj.bytes)
                    };
                    handle_sync_object_ingest_result(
                        stream,
                        &mut session,
                        store,
                        index,
                        keys,
                        &options,
                        &obj,
                        &mut pending_get,
                        &mut pending_subjects,
                        subject_hint,
                        options.relay,
                        result,
                    )?;
                }
            }
            SyncMessage::Err(err) => {
                log_sync(&options, format!("sync: recv err {}", err.message));
                warn!(message = %err.message, "peer error");
            }
            SyncMessage::Ad(ad) => {
                log_sync(
                    &options,
                    format!(
                        "sync: recv ad provider={} domain={}",
                        ad.provider_id.to_hex(),
                        ad.domain
                    ),
                );
                if let Some(store) = &options.ad_store {
                    if ad.verify()? {
                        let mut guard = store.lock().map_err(|_| {
                            DharmaError::Validation("ad store lock poisoned".to_string())
                        })?;
                        guard.insert(ad);
                    }
                }
            }
            SyncMessage::Ads(ads) => {
                log_sync(&options, format!("sync: recv ads count={}", ads.ads.len()));
                if let Some(store) = &options.ad_store {
                    let mut guard = store.lock().map_err(|_| {
                        DharmaError::Validation("ad store lock poisoned".to_string())
                    })?;
                    for ad in ads.ads {
                        if ad.verify()? {
                            guard.insert(ad);
                        }
                    }
                }
            }
            SyncMessage::GetAds(_) => {
                log_sync(&options, "sync: recv getads");
                if let Some(store) = &options.ad_store {
                    let guard = store.lock().map_err(|_| {
                        DharmaError::Validation("ad store lock poisoned".to_string())
                    })?;
                    let ads = guard.get_all().into_iter().map(|(_, ad)| ad).collect();
                    let msg = SyncMessage::Ads(Ads { ads });
                    send_msg(stream, &mut session, &msg)?;
                }
            }
            SyncMessage::Hello(_) => {}
        }
        if options.exit_on_idle && saw_inventory && pending_get.is_empty() {
            log_sync(&options, "sync: complete (no pending)");
            return Ok(());
        }
        let _ = t; // reserved for future type checks
    }
}

fn ingest_sync_object_relay(
    store: &Store,
    index: &mut FrontierIndex,
    identity: &IdentityState,
    envelope_id: EnvelopeId,
    bytes: &[u8],
    subject_hint: Option<SubjectId>,
) -> Result<SyncIngestOutcome, IngestError> {
    match ingest_object_relay(store, index, identity, envelope_id, bytes, subject_hint) {
        Ok(RelayIngestStatus::Accepted(env_id, assertion_id)) => Ok(SyncIngestOutcome::Accepted {
            envelope_id: Some(env_id),
            assertion_id: Some(assertion_id),
        }),
        Ok(RelayIngestStatus::Opaque(env_id)) => Ok(SyncIngestOutcome::Accepted {
            envelope_id: Some(env_id),
            assertion_id: None,
        }),
        Ok(RelayIngestStatus::Pending(assertion_id, reason)) => Ok(SyncIngestOutcome::Pending {
            assertion_id,
            reason,
        }),
        Err(err) => Err(err),
    }
}

fn ingest_sync_object_normal(
    store: &Store,
    index: &mut FrontierIndex,
    keys: &mut Keyring,
    envelope_id: EnvelopeId,
    bytes: &[u8],
) -> Result<SyncIngestOutcome, IngestError> {
    match ingest_object(store, index, bytes, keys) {
        Ok(IngestStatus::Accepted(assertion_id)) => Ok(SyncIngestOutcome::Accepted {
            envelope_id: None,
            assertion_id: Some(assertion_id),
        }),
        Ok(IngestStatus::Pending(assertion_id, reason)) => Ok(SyncIngestOutcome::Pending {
            assertion_id,
            reason,
        }),
        Err(IngestError::Validation(_reason)) => {
            // If it's not an assertion, it might be a schema or other object.
            store.put_object(&envelope_id, bytes)?;
            Ok(SyncIngestOutcome::Accepted {
                envelope_id: Some(envelope_id),
                assertion_id: None,
            })
        }
        Err(err) => Err(err),
    }
}

fn handle_sync_object_ingest_result(
    stream: &mut dyn ReadWrite,
    session: &mut Session,
    store: &Store,
    index: &mut FrontierIndex,
    keys: &mut Keyring,
    options: &SyncOptions,
    obj: &Obj,
    pending_get: &mut HashSet<ObjectRef>,
    pending_subjects: &mut HashMap<ObjectRef, SubjectId>,
    subject_hint: Option<SubjectId>,
    is_relay_mode: bool,
    result: Result<SyncIngestOutcome, IngestError>,
) -> Result<(), DharmaError> {
    match result {
        Ok(SyncIngestOutcome::Accepted {
            envelope_id,
            assertion_id,
        }) => {
            pending_get.remove(&obj.id);
            if let Some(envelope_id) = envelope_id {
                pending_get.remove(&ObjectRef::Envelope(envelope_id));
            }
            if let Some(assertion_id) = assertion_id {
                pending_get.remove(&ObjectRef::Assertion(assertion_id));
            }
            let _ = retry_pending(store, index, keys);
        }
        Ok(SyncIngestOutcome::Pending {
            assertion_id,
            reason,
        }) => {
            pending_get.remove(&obj.id);
            pending_get.remove(&ObjectRef::Assertion(assertion_id));
            log_sync(
                options,
                format!("sync: pending {} ({reason})", assertion_id.to_hex()),
            );
            if is_relay_mode {
                warn!(
                    assertion_id = %assertion_id.to_hex(),
                    reason = %reason,
                    "pending object"
                );
            } else {
                warn!(
                    object_id = %object_ref_hex(&obj.id),
                    reason = %reason,
                    "pending object"
                );
            }
        }
        Err(IngestError::MissingDependency {
            assertion_id,
            missing,
        }) => {
            pending_get.remove(&obj.id);
            pending_get.remove(&ObjectRef::Assertion(assertion_id));
            if let Some(subject) = subject_hint {
                pending_subjects.insert(ObjectRef::Assertion(missing), subject);
            }
            if pending_get.insert(ObjectRef::Assertion(missing)) {
                send_get(stream, session, vec![ObjectRef::Assertion(missing)])?;
            }
        }
        Err(IngestError::Validation(reason)) => {
            let envelope_id = crate::crypto::envelope_id(&obj.bytes);
            pending_get.remove(&obj.id);
            pending_get.remove(&ObjectRef::Envelope(envelope_id));
            pending_subjects.remove(&obj.id);
            warn!(
                object_id = %object_ref_hex(&obj.id),
                envelope_id = %envelope_id.to_hex(),
                reason = %reason,
                "peer sent invalid object"
            );
            if is_relay_mode && is_relay_admission_rejection(&reason) {
                log_sync(
                    options,
                    format!(
                        "sync: relay rejected obj {} ({reason})",
                        object_ref_hex(&obj.id)
                    ),
                );
                return Ok(());
            }
            return Err(DharmaError::Validation(
                "peer sent invalid object".to_string(),
            ));
        }
        Err(IngestError::Dharma(err)) => return Err(err),
        Err(IngestError::Pending(reason)) => {
            warn!(reason = %reason, "pending object");
        }
    }
    Ok(())
}

fn send_inv(
    stream: &mut dyn ReadWrite,
    session: &mut Session,
    store: &Store,
    index: &FrontierIndex,
    overlay: &OverlayAccess,
    peer_subs: &Subscriptions,
) -> Result<(), DharmaError> {
    let subjects = build_inventory_subjects(store, index, overlay, None, Some(peer_subs))?;
    let msg = SyncMessage::Inv(Inventory::Subjects(subjects));
    send_msg(stream, session, &msg)
}

fn send_get(
    stream: &mut dyn ReadWrite,
    session: &mut Session,
    ids: Vec<ObjectRef>,
) -> Result<(), DharmaError> {
    let msg = SyncMessage::Get(Get { ids });
    send_msg(stream, session, &msg)
}

fn is_idle_network_error(msg: &str) -> bool {
    let msg = msg.to_lowercase();
    msg.contains("temporarily unavailable")
        || msg.contains("timed out")
        || msg.contains("would block")
}

fn is_graceful_close(msg: &str) -> bool {
    let msg = msg.to_lowercase();
    msg.contains("connection reset")
        || msg.contains("broken pipe")
        || msg.contains("not connected")
        || msg.contains("connection aborted")
}

fn is_relay_admission_rejection(reason: &str) -> bool {
    reason.starts_with("relay policy:")
        || reason.starts_with("relay auth:")
        || reason == "relay identity not authorized for relay domain"
        || reason == "relay quota exceeded"
        || reason == "relay object quota exceeded"
}

fn log_sync(options: &SyncOptions, msg: impl Into<String>) {
    let msg = msg.into();
    if options.verbose {
        info!(message = %msg, "sync event");
    }
    if let Some(trace) = &options.trace {
        if let Ok(mut guard) = trace.lock() {
            guard.push(msg);
        }
    }
}

fn object_ref_hex(id: &ObjectRef) -> String {
    match id {
        ObjectRef::Assertion(id) => id.to_hex(),
        ObjectRef::Envelope(id) => id.to_hex(),
    }
}

fn send_obj(
    stream: &mut dyn ReadWrite,
    session: &mut Session,
    obj: Obj,
) -> Result<(), DharmaError> {
    let msg = SyncMessage::Obj(obj);
    send_msg(stream, session, &msg)
}

fn send_msg(
    stream: &mut dyn ReadWrite,
    session: &mut Session,
    msg: &SyncMessage,
) -> Result<(), DharmaError> {
    let (t, payload) = match msg {
        SyncMessage::Inv(_) => (TYPE_INV, msg.to_cbor()?),
        SyncMessage::Get(_) => (TYPE_GET, msg.to_cbor()?),
        SyncMessage::Obj(_) => (TYPE_OBJ, msg.to_cbor()?),
        SyncMessage::Ad(_) => (TYPE_AD, msg.to_cbor()?),
        SyncMessage::Ads(_) => (TYPE_ADS, msg.to_cbor()?),
        SyncMessage::GetAds(_) => (TYPE_GET_ADS, msg.to_cbor()?),
        SyncMessage::Err(_) => (TYPE_ERR, msg.to_cbor()?),
        SyncMessage::Hello(_) => (TYPE_INV, msg.to_cbor()?),
    };
    let frame = session.encrypt(t, &payload)?;
    match crate::net::codec::write_frame(stream, &frame) {
        Ok(()) => Ok(()),
        Err(DharmaError::Network(msg)) => {
            std::thread::sleep(Duration::from_millis(50));
            crate::net::codec::write_frame(stream, &frame).map_err(|err| match err {
                DharmaError::Network(_) => DharmaError::Network(msg),
                other => other,
            })
        }
        Err(err) => Err(err),
    }
}

fn send_inv_objects(
    stream: &mut dyn ReadWrite,
    session: &mut Session,
    store: &Store,
) -> Result<(), DharmaError> {
    let objects = list_opaque_objects(store)?;
    let msg = SyncMessage::Inv(Inventory::Objects(objects));
    send_msg(stream, session, &msg)
}

fn list_opaque_objects(store: &Store) -> Result<Vec<EnvelopeId>, DharmaError> {
    let asserted = list_semantic_envelopes(store)?;
    let mut out = Vec::new();
    for obj in store.list_objects()? {
        if !asserted.contains(&obj) {
            out.push(obj);
        }
    }
    Ok(out)
}

fn list_semantic_envelopes(store: &Store) -> Result<HashSet<EnvelopeId>, DharmaError> {
    let mut out = HashSet::new();
    let path = store.indexes_dir().join("semantic_v2.idx");
    if !store.env().exists(&path) {
        return Ok(out);
    }
    let buf = store.env().read(&path)?;
    let usable_len = (buf.len() / 64) * 64;
    if usable_len == 0 {
        return Ok(out);
    }
    for chunk in buf[..usable_len].chunks_exact(64) {
        let env_id = EnvelopeId::from_slice(&chunk[32..64])?;
        out.insert(env_id);
    }
    Ok(out)
}

fn exchange_hello(
    stream: &mut dyn ReadWrite,
    session: &mut Session,
    identity: &IdentityState,
    subs: &Subscriptions,
) -> Result<Hello, DharmaError> {
    let hello = Hello {
        v: 1,
        peer_id: identity.public_key,
        hpke_pk: keys::hpke_public_key_from_secret(&identity.noise_sk),
        suites: vec![1],
        caps: vec!["sync.range".to_string(), "overlay.acl".to_string()],
        subs: Some(subs.clone()),
        subject: Some(identity.subject_id),
        note: None,
    };
    send_msg(stream, session, &SyncMessage::Hello(hello))?;
    let frame = crate::net::codec::read_frame(stream)?;
    let (_t, payload) = session.decrypt(&frame)?;
    let msg = SyncMessage::from_cbor(&payload)?;
    match msg {
        SyncMessage::Hello(peer) => Ok(peer),
        _ => Err(DharmaError::Validation("expected hello".to_string())),
    }
}

fn handle_inv(
    inv: &Inventory,
    store: &Store,
    index: &FrontierIndex,
    pending: &mut HashSet<ObjectRef>,
    overlay: &OverlayAccess,
    subs: &Subscriptions,
) -> Vec<ObjectRef> {
    let mut missing = Vec::new();
    match inv {
        Inventory::Subjects(subjects) => {
            for subject in subjects {
                let namespace = subject_namespace(store, &subject.sub);
                if !subs.allows_subject(&subject.sub, namespace.as_deref()) {
                    continue;
                }
                for tip in &subject.frontier {
                    if !has_assertion(store, index, tip)
                        && pending.insert(ObjectRef::Assertion(*tip))
                    {
                        missing.push(ObjectRef::Assertion(*tip));
                    }
                }
                let allow_overlay = allow_overlay_for_subject(overlay, store, &subject.sub);
                if allow_overlay {
                    for tip in &subject.overlay {
                        if !has_assertion(store, index, tip)
                            && pending.insert(ObjectRef::Assertion(*tip))
                        {
                            missing.push(ObjectRef::Assertion(*tip));
                        }
                    }
                }
            }
        }
        Inventory::Objects(objects) => {
            for obj in objects {
                if !index.has_envelope(&obj) && pending.insert(ObjectRef::Envelope(*obj)) {
                    let already = store.get_object_any(obj).ok().flatten();
                    if already.is_none() {
                        missing.push(ObjectRef::Envelope(*obj));
                    }
                }
            }
        }
    }
    missing
}

fn has_assertion(store: &Store, index: &FrontierIndex, assertion_id: &AssertionId) -> bool {
    if index.is_pending(assertion_id) {
        return true;
    }
    if let Ok(Some(env_id)) = store.lookup_envelope(assertion_id) {
        if index.has_envelope(&env_id) {
            return true;
        }
        if let Ok(Some(_)) = store.get_object_any(&env_id) {
            return true;
        }
    }
    false
}

fn delta_inventory_from_peer(
    inv: &Inventory,
    store: &Store,
    index: &FrontierIndex,
    overlay: &OverlayAccess,
    peer_subs: &Subscriptions,
) -> Result<Option<Inventory>, DharmaError> {
    let Inventory::Subjects(subjects) = inv else {
        return Ok(None);
    };
    let mut out = Vec::new();
    for subject in subjects {
        let Some(since) = subject.since_seq else {
            continue;
        };
        let namespace = subject_namespace(store, &subject.sub);
        if !peer_subs.allows_subject(&subject.sub, namespace.as_deref()) {
            continue;
        }
        if let Some(entry) =
            build_subject_inventory(store, index, overlay, subject.sub, Some(since))?
        {
            out.push(entry);
        }
    }
    if out.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Inventory::Subjects(out)))
    }
}

fn build_inventory_subjects(
    store: &Store,
    index: &FrontierIndex,
    overlay: &OverlayAccess,
    since: Option<&HashMap<SubjectId, u64>>,
    filter: Option<&Subscriptions>,
) -> Result<Vec<SubjectInventory>, DharmaError> {
    let mut subjects_set = std::collections::BTreeSet::new();
    for sub in index.subjects() {
        subjects_set.insert(sub);
    }
    for sub in store.list_subjects()? {
        subjects_set.insert(sub);
    }
    let mut subjects = Vec::new();
    for sub in subjects_set {
        if let Some(filter) = filter {
            let namespace = subject_namespace(store, &sub);
            if !filter.allows_subject(&sub, namespace.as_deref()) {
                continue;
            }
        }
        let since_seq = since.and_then(|map| map.get(&sub).copied());
        if let Some(entry) = build_subject_inventory(store, index, overlay, sub, since_seq)? {
            subjects.push(entry);
        }
    }
    Ok(subjects)
}

fn build_subject_inventory(
    store: &Store,
    index: &FrontierIndex,
    overlay: &OverlayAccess,
    sub: SubjectId,
    since_seq: Option<u64>,
) -> Result<Option<SubjectInventory>, DharmaError> {
    let allow_overlay = allow_overlay_for_subject(overlay, store, &sub);
    let mut frontier = index.get_tips(&sub);
    if let Some(since) = since_seq {
        frontier = filter_tips_since(index, &sub, frontier, since);
    }
    let overlay_assertions = if allow_overlay {
        overlay_frontier(store, &sub)?
    } else {
        Vec::new()
    };
    let overlay = overlay_assertions;
    if frontier.is_empty() && overlay.is_empty() {
        return Ok(None);
    }
    Ok(Some(SubjectInventory {
        sub,
        frontier,
        overlay,
        since_seq: index.max_seq_for_subject(&sub),
    }))
}

fn filter_tips_since(
    index: &FrontierIndex,
    subject: &SubjectId,
    tips: Vec<AssertionId>,
    since: u64,
) -> Vec<AssertionId> {
    tips.into_iter()
        .filter(|tip| {
            index
                .tip_seq(subject, tip)
                .map(|seq| seq > since)
                .unwrap_or(true)
        })
        .collect()
}

fn overlay_frontier(store: &Store, subject: &SubjectId) -> Result<Vec<AssertionId>, DharmaError> {
    let mut headers: HashMap<AssertionId, Option<AssertionId>> = HashMap::new();
    for record in crate::store::state::list_overlays(store.env(), subject)? {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(assertion) => assertion,
            Err(_) => continue,
        };
        headers.insert(record.assertion_id, assertion.header.prev);
    }
    let mut tips: HashSet<AssertionId> = headers.keys().copied().collect();
    for prev in headers.values().copied().flatten() {
        tips.remove(&prev);
    }
    Ok(tips.into_iter().collect())
}

fn handle_get(
    store: &Store,
    get: Get,
    overlay: &OverlayAccess,
    subs: &Subscriptions,
) -> Result<Vec<Obj>, DharmaError> {
    let mut out = Vec::new();
    let mut sent: HashSet<ObjectRef> = HashSet::new();
    for id in get.ids {
        match id {
            ObjectRef::Envelope(env_id) => {
                if let Some(bytes) = store.get_object_any(&env_id)? {
                    push_obj_with_overlays(
                        store,
                        overlay,
                        subs,
                        ObjectRef::Envelope(env_id),
                        bytes,
                        &mut out,
                        &mut sent,
                    )?;
                    continue;
                }
                if let Some((_kind, _subject, _assertion_id, bytes)) =
                    find_cqrs_object_by_envelope(store, &env_id)?
                {
                    push_obj_with_overlays(
                        store,
                        overlay,
                        subs,
                        ObjectRef::Envelope(env_id),
                        bytes,
                        &mut out,
                        &mut sent,
                    )?;
                }
            }
            ObjectRef::Assertion(assertion_id) => {
                let mut bytes_opt = None;
                if let Some(env_id) = store.lookup_envelope(&assertion_id)? {
                    if let Some(bytes) = store.get_object_any(&env_id)? {
                        bytes_opt = Some(bytes);
                    }
                }
                if bytes_opt.is_none() {
                    if let Some((_kind, _subject, bytes)) = find_cqrs_object(store, &assertion_id)?
                    {
                        bytes_opt = Some(bytes);
                    }
                }
                if let Some(bytes) = bytes_opt {
                    push_obj_with_overlays(
                        store,
                        overlay,
                        subs,
                        ObjectRef::Assertion(assertion_id),
                        bytes,
                        &mut out,
                        &mut sent,
                    )?;
                }
            }
        }
    }
    Ok(out)
}

fn push_obj_with_overlays(
    store: &Store,
    overlay: &OverlayAccess,
    subs: &Subscriptions,
    id: ObjectRef,
    bytes: Vec<u8>,
    out: &mut Vec<Obj>,
    sent: &mut HashSet<ObjectRef>,
) -> Result<(), DharmaError> {
    if let Ok(assertion) = AssertionPlaintext::from_cbor(&bytes) {
        let subject = assertion.header.sub;
        let namespace = subject_namespace(store, &subject);
        if !subs.allows_subject(&subject, namespace.as_deref()) {
            return Ok(());
        }
        let allow_overlay = allow_overlay_for_bytes(overlay, store, &subject, &bytes);
        let kind = classify_assertion(&bytes);
        if kind == CqrsKind::OverlayAction && !allow_overlay {
            return Ok(());
        }
        if sent.insert(id.clone()) {
            out.push(Obj {
                id: id.clone(),
                bytes: bytes.clone(),
            });
        }
        if allow_overlay && kind == CqrsKind::BaseAction {
            let assertion_id = assertion.assertion_id()?;
            append_overlays(store, &subject, &assertion_id, out, sent)?;
        }
        return Ok(());
    }
    if sent.insert(id.clone()) {
        out.push(Obj { id, bytes });
    }
    Ok(())
}

fn find_cqrs_object_by_envelope(
    store: &Store,
    envelope_id: &EnvelopeId,
) -> Result<Option<(CqrsKind, SubjectId, AssertionId, Vec<u8>)>, DharmaError> {
    if let Some(entry) = store.lookup_cqrs_by_envelope(envelope_id)? {
        if let Some(bytes) = load_bytes_for_cqrs_entry(store, &entry)? {
            let kind = if entry.is_overlay {
                CqrsKind::OverlayAction
            } else {
                classify_assertion(&bytes)
            };
            return Ok(Some((kind, entry.subject, entry.assertion_id, bytes)));
        }
    }
    for subject in store.list_subjects()? {
        for record in list_assertions(store.env(), &subject)? {
            if record.envelope_id == *envelope_id {
                let kind = classify_assertion(&record.bytes);
                return Ok(Some((kind, subject, record.assertion_id, record.bytes)));
            }
        }
        for record in list_overlays(store.env(), &subject)? {
            if record.envelope_id == *envelope_id {
                return Ok(Some((
                    CqrsKind::OverlayAction,
                    subject,
                    record.assertion_id,
                    record.bytes,
                )));
            }
        }
    }
    Ok(None)
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum CqrsKind {
    BaseAction,
    OverlayAction,
    Other,
}

fn load_bytes_for_cqrs_entry(
    store: &Store,
    entry: &crate::store::state::CqrsReverseEntry,
) -> Result<Option<Vec<u8>>, DharmaError> {
    if let Some(bytes) = store.get_object_any(&entry.envelope_id)? {
        return Ok(Some(bytes));
    }
    if entry.is_overlay {
        if let Some(bytes) = find_overlay_by_id(store.env(), &entry.subject, &entry.assertion_id)? {
            return Ok(Some(bytes));
        }
        return find_assertion_by_id(store.env(), &entry.subject, &entry.assertion_id);
    }
    if let Some(bytes) = find_assertion_by_id(store.env(), &entry.subject, &entry.assertion_id)? {
        return Ok(Some(bytes));
    }
    find_overlay_by_id(store.env(), &entry.subject, &entry.assertion_id)
}

fn find_cqrs_object(
    store: &Store,
    assertion_id: &AssertionId,
) -> Result<Option<(CqrsKind, SubjectId, Vec<u8>)>, DharmaError> {
    if let Some(entry) = store.lookup_cqrs_by_assertion(assertion_id)? {
        if let Some(bytes) = load_bytes_for_cqrs_entry(store, &entry)? {
            let kind = if entry.is_overlay {
                CqrsKind::OverlayAction
            } else {
                classify_assertion(&bytes)
            };
            return Ok(Some((kind, entry.subject, bytes)));
        }
    }
    for subject in store.list_subjects()? {
        if let Some(bytes) = find_assertion_by_id(store.env(), &subject, assertion_id)? {
            let kind = classify_assertion(&bytes);
            return Ok(Some((kind, subject, bytes)));
        }
        if let Some(bytes) = find_overlay_by_id(store.env(), &subject, assertion_id)? {
            return Ok(Some((CqrsKind::OverlayAction, subject, bytes)));
        }
    }
    Ok(None)
}

fn append_overlays(
    store: &Store,
    subject: &SubjectId,
    base_id: &AssertionId,
    out: &mut Vec<Obj>,
    sent: &mut HashSet<ObjectRef>,
) -> Result<(), DharmaError> {
    for bytes in overlays_for_ref(store.env(), subject, base_id)? {
        let overlay_assertion = match AssertionPlaintext::from_cbor(&bytes) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let overlay_id = overlay_assertion.assertion_id()?;
        let env_id = store
            .lookup_envelope(&overlay_id)?
            .unwrap_or_else(|| EnvelopeId::from_bytes(*overlay_id.as_bytes()));
        let payload = match store.get_object_any(&env_id)? {
            Some(payload) => payload,
            None => bytes,
        };
        let ref_id = ObjectRef::Assertion(overlay_id);
        if sent.insert(ref_id.clone()) {
            out.push(Obj {
                id: ref_id,
                bytes: payload,
            });
        }
    }
    Ok(())
}

fn classify_assertion(bytes: &[u8]) -> CqrsKind {
    let assertion = match AssertionPlaintext::from_cbor(bytes) {
        Ok(assertion) => assertion,
        Err(_) => return CqrsKind::Other,
    };
    if !assertion.header.typ.starts_with(ACTION_PREFIX) {
        return CqrsKind::Other;
    }
    if is_overlay(&assertion.header) {
        return CqrsKind::OverlayAction;
    }
    CqrsKind::BaseAction
}

fn allow_overlay_for_subject(overlay: &OverlayAccess, store: &Store, subject: &SubjectId) -> bool {
    let namespace = subject_namespace(store, subject);
    overlay.allows(subject, namespace.as_deref())
}

fn allow_overlay_for_bytes(
    overlay: &OverlayAccess,
    store: &Store,
    subject: &SubjectId,
    bytes: &[u8],
) -> bool {
    let namespace = namespace_for_assertion(store, bytes);
    overlay.allows(subject, namespace.as_deref())
}

fn subject_namespace(store: &Store, subject: &SubjectId) -> Option<String> {
    if let Ok(records) = list_assertions(store.env(), subject) {
        for record in records {
            if let Some(namespace) = namespace_for_assertion(store, &record.bytes) {
                return Some(namespace);
            }
        }
    }
    if let Ok(records) = list_overlays(store.env(), subject) {
        for record in records {
            if let Some(namespace) = namespace_for_assertion(store, &record.bytes) {
                return Some(namespace);
            }
        }
    }
    None
}

fn namespace_for_assertion(store: &Store, bytes: &[u8]) -> Option<String> {
    let assertion = AssertionPlaintext::from_cbor(bytes).ok()?;
    schema_namespace(store, &assertion.header.schema)
}

fn schema_namespace(store: &Store, schema_id: &SchemaId) -> Option<String> {
    let envelope_id = EnvelopeId::from_bytes(*schema_id.as_bytes());
    let bytes = store.get_object_any(&envelope_id).ok()??;
    let schema = CqrsSchema::from_cbor(&bytes).ok()?;
    Some(schema.namespace)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::assertion_types::META_OVERLAY;
    use crate::crypto;
    use crate::net::policy::OverlayPolicy;
    use crate::store::state::{append_assertion, append_overlay};
    use crate::types::{AssertionId, ContractId, EnvelopeId, IdentityKey, SchemaId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn action_assertion_bytes(
        subject: SubjectId,
        signing_key: &ed25519_dalek::SigningKey,
        seq: u64,
        prev: Option<AssertionId>,
        refs: Vec<AssertionId>,
        overlay: bool,
    ) -> Vec<u8> {
        let meta = if overlay {
            Some(Value::Map(vec![(
                Value::Text(META_OVERLAY.to_string()),
                Value::Bool(true),
            )]))
        } else {
            None
        };
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "action.Touch".to_string(),
            auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq,
            prev,
            refs,
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta,
        };
        let body = Value::Map(Vec::new());
        let assertion = AssertionPlaintext::sign(header, body, signing_key).unwrap();
        assertion.to_cbor().unwrap()
    }

    fn note_assertion_bytes(
        subject: SubjectId,
        signing_key: &ed25519_dalek::SigningKey,
    ) -> Vec<u8> {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![(
            Value::Text("text".to_string()),
            Value::Text("hello".to_string()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, signing_key).unwrap();
        assertion.to_cbor().unwrap()
    }

    #[test]
    fn handle_inv_requests_overlay_when_allowed() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::default();
        let subject = SubjectId::from_bytes([1u8; 32]);
        let base = AssertionId::from_bytes([2u8; 32]);
        let overlay = AssertionId::from_bytes([3u8; 32]);
        index.mark_known(base);

        let policy = OverlayPolicy::from_str(&format!(
            "default deny\nsubject {} allow\n",
            subject.to_hex()
        ));
        let claims = crate::net::policy::PeerClaims::default();
        let access = OverlayAccess::new(&policy, None, false, &claims);
        let inv = Inventory::Subjects(vec![SubjectInventory {
            sub: subject,
            frontier: Vec::new(),
            overlay: vec![overlay],
            since_seq: None,
        }]);
        let mut pending = HashSet::new();
        let missing = handle_inv(
            &inv,
            &store,
            &index,
            &mut pending,
            &access,
            &Subscriptions::all(),
        );
        assert_eq!(missing, vec![ObjectRef::Assertion(overlay)]);
    }

    #[test]
    fn relay_admission_rejection_classifies_expected_errors() {
        assert!(is_relay_admission_rejection(
            "relay policy: validation error: missing domain ownership"
        ));
        assert!(is_relay_admission_rejection("relay auth: unavailable"));
        assert!(is_relay_admission_rejection(
            "relay identity not authorized for relay domain"
        ));
        assert!(is_relay_admission_rejection("relay quota exceeded"));
        assert!(is_relay_admission_rejection("relay object quota exceeded"));
    }

    #[test]
    fn relay_admission_rejection_ignores_non_policy_errors() {
        assert!(!is_relay_admission_rejection("invalid signature"));
        assert!(!is_relay_admission_rejection("envelope hash mismatch"));
    }

    #[test]
    fn handle_inv_ignores_overlay_when_not_allowed() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::default();
        let subject = SubjectId::from_bytes([4u8; 32]);
        let overlay = AssertionId::from_bytes([5u8; 32]);
        index.mark_known(AssertionId::from_bytes([6u8; 32]));

        let policy = OverlayPolicy::from_str("default deny\n");
        let claims = crate::net::policy::PeerClaims::default();
        let access = OverlayAccess::new(&policy, None, false, &claims);
        let inv = Inventory::Subjects(vec![SubjectInventory {
            sub: subject,
            frontier: Vec::new(),
            overlay: vec![overlay],
            since_seq: None,
        }]);
        let mut pending = HashSet::new();
        let missing = handle_inv(
            &inv,
            &store,
            &index,
            &mut pending,
            &access,
            &Subscriptions::all(),
        );
        assert!(missing.is_empty());
    }

    #[test]
    fn handle_inv_deduplicates_replayed_and_reordered_tips() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let index = FrontierIndex::default();
        let subject = SubjectId::from_bytes([13u8; 32]);
        let first_tip = AssertionId::from_bytes([14u8; 32]);
        let second_tip = AssertionId::from_bytes([15u8; 32]);
        let policy = OverlayPolicy::from_str("default deny\n");
        let claims = crate::net::policy::PeerClaims::default();
        let access = OverlayAccess::new(&policy, None, false, &claims);
        let mut pending = HashSet::new();

        let first = Inventory::Subjects(vec![SubjectInventory {
            sub: subject,
            frontier: vec![first_tip, first_tip],
            overlay: Vec::new(),
            since_seq: None,
        }]);
        let first_missing = handle_inv(
            &first,
            &store,
            &index,
            &mut pending,
            &access,
            &Subscriptions::all(),
        );
        assert_eq!(first_missing, vec![ObjectRef::Assertion(first_tip)]);

        let replay_and_reorder = Inventory::Subjects(vec![SubjectInventory {
            sub: subject,
            frontier: vec![second_tip, first_tip],
            overlay: Vec::new(),
            since_seq: None,
        }]);
        let second_missing = handle_inv(
            &replay_and_reorder,
            &store,
            &index,
            &mut pending,
            &access,
            &Subscriptions::all(),
        );
        assert_eq!(second_missing, vec![ObjectRef::Assertion(second_tip)]);
    }

    #[test]
    fn classify_assertion_distinguishes_base_and_overlay() {
        let mut rng = StdRng::seed_from_u64(3);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([7u8; 32]);

        let base = action_assertion_bytes(subject, &signing_key, 1, None, Vec::new(), false);
        let overlay = action_assertion_bytes(subject, &signing_key, 1, None, Vec::new(), true);
        let other = note_assertion_bytes(subject, &signing_key);

        assert_eq!(classify_assertion(&base), CqrsKind::BaseAction);
        assert_eq!(classify_assertion(&overlay), CqrsKind::OverlayAction);
        assert_eq!(classify_assertion(&other), CqrsKind::Other);
    }

    #[test]
    fn delta_inventory_filters_by_since_seq() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::default();
        let subject = SubjectId::from_bytes([11u8; 32]);
        let header1 = AssertionHeader {
            v: 1,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: IdentityKey::from_bytes([2u8; 32]),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let id1 = AssertionId::from_bytes([6u8; 32]);
        let header2 = AssertionHeader {
            seq: 2,
            prev: Some(id1),
            ..header1.clone()
        };
        let id2 = AssertionId::from_bytes([7u8; 32]);
        index
            .update(id1, EnvelopeId::from_bytes(*id1.as_bytes()), &header1)
            .unwrap();
        index
            .update(id2, EnvelopeId::from_bytes(*id2.as_bytes()), &header2)
            .unwrap();

        let policy = OverlayPolicy::from_str("default deny\n");
        let claims = crate::net::policy::PeerClaims::default();
        let access = OverlayAccess::new(&policy, None, false, &claims);
        let inv = Inventory::Subjects(vec![SubjectInventory {
            sub: subject,
            frontier: Vec::new(),
            overlay: Vec::new(),
            since_seq: Some(1),
        }]);
        let delta = delta_inventory_from_peer(&inv, &store, &index, &access, &Subscriptions::all())
            .unwrap()
            .unwrap();
        match delta {
            Inventory::Subjects(subjects) => {
                assert_eq!(subjects.len(), 1);
                assert_eq!(subjects[0].frontier, vec![id2]);
                assert_eq!(subjects[0].since_seq, Some(2));
            }
            _ => panic!("expected subjects inventory"),
        }
    }

    #[test]
    fn handle_get_attaches_overlays_for_base_action_only() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut rng = StdRng::seed_from_u64(9);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([8u8; 32]);

        let base_bytes = action_assertion_bytes(subject, &signing_key, 1, None, Vec::new(), false);
        let base_assertion = AssertionPlaintext::from_cbor(&base_bytes).unwrap();
        let base_id = base_assertion.assertion_id().unwrap();
        let base_env = crypto::envelope_id(&base_bytes);
        append_assertion(
            store.env(),
            &subject,
            1,
            base_id,
            base_env,
            "Touch",
            &base_bytes,
        )
        .unwrap();
        store.record_semantic(&base_id, &base_env).unwrap();

        let overlay_bytes =
            action_assertion_bytes(subject, &signing_key, 1, None, vec![base_id], true);
        let overlay_assertion = AssertionPlaintext::from_cbor(&overlay_bytes).unwrap();
        let overlay_id = overlay_assertion.assertion_id().unwrap();
        let overlay_env = crypto::envelope_id(&overlay_bytes);
        append_overlay(
            store.env(),
            &subject,
            1,
            overlay_id,
            overlay_env,
            "Touch",
            &overlay_bytes,
        )
        .unwrap();
        store.record_semantic(&overlay_id, &overlay_env).unwrap();

        let policy = OverlayPolicy::from_str(&format!(
            "default deny\nsubject {} allow\n",
            subject.to_hex()
        ));
        let claims = crate::net::policy::PeerClaims::default();
        let access = OverlayAccess::new(&policy, None, false, &claims);
        let get = Get {
            ids: vec![ObjectRef::Assertion(base_id)],
        };
        let out = handle_get(&store, get, &access, &Subscriptions::all()).unwrap();
        let ids = out.iter().map(|o| o.id.clone()).collect::<HashSet<_>>();
        assert!(ids.contains(&ObjectRef::Assertion(base_id)));
        assert!(ids.contains(&ObjectRef::Assertion(overlay_id)));

        let note_subject = SubjectId::from_bytes([9u8; 32]);
        let note_bytes = note_assertion_bytes(note_subject, &signing_key);
        let note_assertion = AssertionPlaintext::from_cbor(&note_bytes).unwrap();
        let note_id = note_assertion.assertion_id().unwrap();
        let note_env = crypto::envelope_id(&note_bytes);
        store
            .put_assertion(&note_subject, &note_env, &note_bytes)
            .unwrap();
        append_assertion(
            store.env(),
            &note_subject,
            1,
            note_id,
            note_env,
            "note.text",
            &note_bytes,
        )
        .unwrap();
        store.record_semantic(&note_id, &note_env).unwrap();

        let get = Get {
            ids: vec![ObjectRef::Assertion(note_id)],
        };
        let out = handle_get(&store, get, &access, &Subscriptions::all()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, ObjectRef::Assertion(note_id));
    }

    #[test]
    fn find_cqrs_object_uses_reverse_index_when_object_missing() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut rng = StdRng::seed_from_u64(17);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([33u8; 32]);

        let bytes = action_assertion_bytes(subject, &signing_key, 1, None, Vec::new(), false);
        let assertion = AssertionPlaintext::from_cbor(&bytes).unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        let envelope_id = crypto::envelope_id(&bytes);

        store.put_assertion(&subject, &envelope_id, &bytes).unwrap();
        append_assertion(
            store.env(),
            &subject,
            1,
            assertion_id,
            envelope_id,
            "Touch",
            &bytes,
        )
        .unwrap();
        store.record_semantic(&assertion_id, &envelope_id).unwrap();

        let object_path = store
            .objects_dir()
            .join(format!("{}.obj", envelope_id.to_hex()));
        store.env().remove_file(&object_path).unwrap();

        let by_envelope = find_cqrs_object_by_envelope(&store, &envelope_id)
            .unwrap()
            .unwrap();
        assert_eq!(by_envelope.1, subject);
        assert_eq!(by_envelope.2, assertion_id);

        let by_assertion = find_cqrs_object(&store, &assertion_id).unwrap().unwrap();
        assert_eq!(by_assertion.1, subject);
        assert_eq!(by_assertion.2, bytes);
    }
}
