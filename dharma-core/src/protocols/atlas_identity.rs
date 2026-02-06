use super::{ProtocolId, ProtocolInterface};
use crate::env::Env;
use crate::error::DharmaError;
use crate::identity::{self, IdentityStatus};
use crate::types::SubjectId;

pub const PROTOCOL_NAME: &str = "std.protocol.atlas.identity";
pub const PROTOCOL_VERSION: u64 = 1;

pub const ASSERTION_GENESIS: &str = "atlas.identity.genesis";
pub const ASSERTION_ACTIVATE: &str = "atlas.identity.activate";
pub const ASSERTION_SUSPEND: &str = "atlas.identity.suspend";
pub const ASSERTION_REVOKE: &str = "atlas.identity.revoke";

const REQUIRED_FIELDS: &[&str] = &["atlas_name", "owner_key", "schema", "contract"];

const REQUIRED_ACTIONS: &[&str] = &[];

const REQUIRED_ASSERTIONS: &[&str] = &[
    ASSERTION_GENESIS,
    ASSERTION_ACTIVATE,
    ASSERTION_SUSPEND,
    ASSERTION_REVOKE,
];

pub fn interface() -> ProtocolInterface {
    ProtocolInterface {
        id: ProtocolId::new(PROTOCOL_NAME, PROTOCOL_VERSION),
        required_state_fields: REQUIRED_FIELDS,
        required_actions: REQUIRED_ACTIONS,
        required_assertions: REQUIRED_ASSERTIONS,
        required_enums: &[],
        private_fields: &[],
    }
}

pub fn genesis_required_fields() -> &'static [&'static str] {
    REQUIRED_FIELDS
}

pub fn identity_status_v1(
    env: &dyn Env,
    subject: &SubjectId,
) -> Result<IdentityStatus, DharmaError> {
    identity::identity_status(env, subject)
}

pub fn is_verified_v1(env: &dyn Env, subject: &SubjectId) -> Result<bool, DharmaError> {
    identity::is_verified_identity(env, subject)
}
