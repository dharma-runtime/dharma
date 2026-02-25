use ciborium::value::Value;
use dharma_core::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
use dharma_core::contract::PermissionSummary;
use dharma_core::crypto;
use dharma_core::error::DharmaError;
use dharma_core::store::spi::{StorageCommit, StorageIndex, StorageQuery, StorageRead};
use dharma_core::store::sqlite::SqliteEmbeddedAdapter;
use dharma_core::store::state;
use dharma_core::store::Store;
use dharma_core::types::{ContractId, EnvelopeId, IdentityKey, SchemaId, SubjectId};
use rand::rngs::StdRng;
use rand::SeedableRng;
use rusqlite::Connection;

fn signed_assertion_bytes(
    subject: SubjectId,
    seq: u64,
) -> (dharma_core::AssertionId, dharma_core::EnvelopeId, Vec<u8>) {
    let mut rng = StdRng::seed_from_u64(100 + seq);
    let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: "note.text".to_string(),
        auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
        seq,
        prev: None,
        refs: Vec::new(),
        ts: None,
        schema: SchemaId::from_bytes([2u8; 32]),
        contract: ContractId::from_bytes([3u8; 32]),
        note: None,
        meta: None,
    };
    let assertion =
        AssertionPlaintext::sign(header, Value::Text("hello".to_string()), &signing_key).unwrap();
    let bytes = assertion.to_cbor().unwrap();
    let assertion_id = assertion.assertion_id().unwrap();
    let envelope_id = crypto::envelope_id(&bytes);
    (assertion_id, envelope_id, bytes)
}

#[test]
fn sqlite_embedded_adapter_creates_schema_and_indexes() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::from_root(temp.path());
    let adapter = SqliteEmbeddedAdapter::open(temp.path(), store).unwrap();

    assert!(adapter.db_path().exists());

    let conn = Connection::open(adapter.db_path()).unwrap();
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'index'")
        .unwrap();
    let names = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(names.iter().any(|name| name == "idx_semantic_assertion"));
    assert!(names.iter().any(|name| name == "idx_cqrs_envelope"));
    assert!(names.iter().any(|name| name == "idx_cqrs_assertion"));
    assert!(names
        .iter()
        .any(|name| name == "idx_subject_assertions_subject_seq"));
}

#[test]
fn sqlite_embedded_adapter_lookup_parity() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::from_root(temp.path());
    let adapter = SqliteEmbeddedAdapter::open(temp.path(), store.clone()).unwrap();

    let subject = SubjectId::from_bytes([7u8; 32]);
    let (assertion_id, envelope_id, bytes) = signed_assertion_bytes(subject, 1);

    adapter
        .put_assertion(&subject, &envelope_id, &bytes)
        .unwrap();
    adapter
        .record_semantic(&assertion_id, &envelope_id)
        .unwrap();
    state::append_assertion(
        store.env(),
        &subject,
        1,
        assertion_id,
        envelope_id,
        "Init",
        &bytes,
    )
    .unwrap();

    assert_eq!(adapter.get_object(&envelope_id).unwrap(), bytes);
    assert_eq!(
        adapter.lookup_envelope(&assertion_id).unwrap(),
        store.lookup_envelope(&assertion_id).unwrap()
    );

    let adapter_cqrs = adapter.lookup_cqrs_by_envelope(&envelope_id).unwrap();
    let legacy_cqrs = store.lookup_cqrs_by_envelope(&envelope_id).unwrap();
    assert_eq!(adapter_cqrs, legacy_cqrs);

    let adapter_cqrs_by_assertion = adapter.lookup_cqrs_by_assertion(&assertion_id).unwrap();
    let legacy_cqrs_by_assertion = store.lookup_cqrs_by_assertion(&assertion_id).unwrap();
    assert_eq!(adapter_cqrs_by_assertion, legacy_cqrs_by_assertion);

    let cqrs_path = temp.path().join("indexes").join("cqrs_reverse_v1.idx");
    std::fs::remove_file(cqrs_path).unwrap();

    assert_eq!(
        adapter.lookup_cqrs_by_envelope(&envelope_id).unwrap(),
        adapter_cqrs
    );
    assert_eq!(
        adapter.lookup_cqrs_by_assertion(&assertion_id).unwrap(),
        adapter_cqrs_by_assertion
    );

    let summary = PermissionSummary::empty(ContractId::from_bytes([11u8; 32]), 1);
    adapter.put_permission_summary(&summary).unwrap();
    assert_eq!(
        adapter.get_permission_summary(&summary.contract).unwrap(),
        Some(summary)
    );
}

#[test]
fn sqlite_embedded_adapter_uses_custom_indexes() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::from_root(temp.path());
    let adapter = SqliteEmbeddedAdapter::open(temp.path(), store.clone()).unwrap();

    let subject = SubjectId::from_bytes([9u8; 32]);
    let (assertion_id, envelope_id, bytes) = signed_assertion_bytes(subject, 1);

    adapter
        .put_assertion(&subject, &envelope_id, &bytes)
        .unwrap();
    adapter
        .record_semantic(&assertion_id, &envelope_id)
        .unwrap();
    state::append_assertion(
        store.env(),
        &subject,
        1,
        assertion_id,
        envelope_id,
        "Init",
        &bytes,
    )
    .unwrap();

    adapter.lookup_cqrs_by_envelope(&envelope_id).unwrap();

    let semantic_plan = adapter
        .explain_query_plan(
            "EXPLAIN QUERY PLAN SELECT envelope_id FROM semantic_index WHERE assertion_id = ?1 ORDER BY inserted_at DESC LIMIT 1",
            assertion_id.as_bytes().as_slice(),
        )
        .unwrap();
    assert!(semantic_plan
        .iter()
        .any(|line| line.contains("idx_semantic_assertion")));

    let cqrs_envelope_plan = adapter
        .explain_query_plan(
            "EXPLAIN QUERY PLAN SELECT envelope_id, assertion_id, subject_id, is_overlay FROM cqrs_reverse WHERE envelope_id = ?1 LIMIT 1",
            envelope_id.as_bytes().as_slice(),
        )
        .unwrap();
    assert!(cqrs_envelope_plan
        .iter()
        .any(|line| line.contains("idx_cqrs_envelope")));

    let cqrs_assertion_plan = adapter
        .explain_query_plan(
            "EXPLAIN QUERY PLAN SELECT envelope_id, assertion_id, subject_id, is_overlay FROM cqrs_reverse WHERE assertion_id = ?1 LIMIT 1",
            assertion_id.as_bytes().as_slice(),
        )
        .unwrap();
    assert!(cqrs_assertion_plan
        .iter()
        .any(|line| line.contains("idx_cqrs_assertion")));
}

#[test]
fn sqlite_embedded_adapter_rejects_malformed_assertion_cbor() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::from_root(temp.path());
    let adapter = SqliteEmbeddedAdapter::open(temp.path(), store.clone()).unwrap();

    let subject = SubjectId::from_bytes([13u8; 32]);
    let envelope_id = EnvelopeId::from_bytes([14u8; 32]);
    let err = adapter
        .put_assertion(&subject, &envelope_id, &[0xff])
        .unwrap_err();
    match err {
        DharmaError::Cbor(_) | DharmaError::Validation(_) => {}
        other => panic!("unexpected error: {other:?}"),
    }

    assert_eq!(store.get_object_any(&envelope_id).unwrap(), None);

    let conn = Connection::open(adapter.db_path()).unwrap();
    let objects_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM objects", [], |row| row.get(0))
        .unwrap();
    let assertions_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM subject_assertions", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(objects_count, 0);
    assert_eq!(assertions_count, 0);
}

#[test]
fn sqlite_embedded_adapter_reconciles_list_reads_after_partial_backfill() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::from_root(temp.path());
    let adapter = SqliteEmbeddedAdapter::open(temp.path(), store.clone()).unwrap();

    let subject_a = SubjectId::from_bytes([21u8; 32]);
    let subject_b = SubjectId::from_bytes([22u8; 32]);
    let (assertion_a, envelope_a, bytes_a) = signed_assertion_bytes(subject_a, 1);
    let (assertion_b, envelope_b, bytes_b) = signed_assertion_bytes(subject_b, 2);

    store.put_object(&envelope_a, &bytes_a).unwrap();
    state::append_assertion(
        store.env(),
        &subject_a,
        1,
        assertion_a,
        envelope_a,
        "Init",
        &bytes_a,
    )
    .unwrap();
    store.put_object(&envelope_b, &bytes_b).unwrap();
    state::append_assertion(
        store.env(),
        &subject_b,
        2,
        assertion_b,
        envelope_b,
        "Init",
        &bytes_b,
    )
    .unwrap();

    adapter.scan_subject(&subject_a).unwrap();

    let subjects = adapter.list_subjects().unwrap();
    assert!(subjects.contains(&subject_a));
    assert!(subjects.contains(&subject_b));

    let objects = adapter.list_objects().unwrap();
    assert!(objects.contains(&envelope_a));
    assert!(objects.contains(&envelope_b));
}

#[test]
fn sqlite_embedded_adapter_reconciles_scan_subject_after_partial_cache() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::from_root(temp.path());
    let adapter = SqliteEmbeddedAdapter::open(temp.path(), store.clone()).unwrap();

    let subject = SubjectId::from_bytes([23u8; 32]);
    let (assertion_a, envelope_a, bytes_a) = signed_assertion_bytes(subject, 1);
    let (assertion_b, envelope_b, bytes_b) = signed_assertion_bytes(subject, 2);

    store.put_object(&envelope_a, &bytes_a).unwrap();
    state::append_assertion(
        store.env(),
        &subject,
        1,
        assertion_a,
        envelope_a,
        "Init",
        &bytes_a,
    )
    .unwrap();

    assert_eq!(adapter.scan_subject(&subject).unwrap(), vec![assertion_a]);

    store.put_object(&envelope_b, &bytes_b).unwrap();
    state::append_assertion(
        store.env(),
        &subject,
        2,
        assertion_b,
        envelope_b,
        "Init",
        &bytes_b,
    )
    .unwrap();

    let ids = adapter.scan_subject(&subject).unwrap();
    assert_eq!(ids, vec![assertion_a, assertion_b]);

    let conn = Connection::open(adapter.db_path()).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM subject_assertions WHERE subject_id = ?1 AND is_overlay = 0",
            [subject.as_bytes().as_slice()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
}
