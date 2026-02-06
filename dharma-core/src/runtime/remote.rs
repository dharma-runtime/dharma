use crate::assertion::AssertionPlaintext;
use crate::env::Env;
use crate::error::DharmaError;
use crate::pdl::schema::{CqrsSchema, TypeSpec};
use crate::runtime::cqrs::{load_state_at_seq, read_value_at_path};
use crate::store::state::find_assertion_by_seq;
use crate::types::{EnvelopeId, SubjectId};
use ciborium::value::Value;

pub fn read_remote_field(
    env: &dyn Env,
    subject: &SubjectId,
    seq: u64,
    path: &str,
) -> Result<(TypeSpec, Value), DharmaError> {
    let assertion_bytes = find_assertion_by_seq(env, subject, seq)?
        .ok_or_else(|| DharmaError::Validation("missing assertion for seq".to_string()))?;
    let assertion = AssertionPlaintext::from_cbor(&assertion_bytes)?;
    let schema_id = assertion.header.schema;
    let contract_id = assertion.header.contract;
    let ver = assertion.header.ver;

    let schema_bytes = load_object_bytes(env, &EnvelopeId::from_bytes(*schema_id.as_bytes()))?;
    let schema = CqrsSchema::from_cbor(&schema_bytes)?;
    let contract_bytes = load_object_bytes(env, &EnvelopeId::from_bytes(*contract_id.as_bytes()))?;

    let state = load_state_at_seq(env, subject, &schema, &contract_bytes, ver, seq)?;
    read_value_at_path(&state.memory, &schema, path)
}

fn load_object_bytes(env: &dyn Env, envelope: &EnvelopeId) -> Result<Vec<u8>, DharmaError> {
    let path = env
        .root()
        .join("objects")
        .join(format!("{}.obj", envelope.to_hex()));
    env.read(&path)
}
