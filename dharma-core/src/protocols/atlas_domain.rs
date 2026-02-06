use super::{ProtocolId, ProtocolInterface};

pub const PROTOCOL_NAME: &str = "std.protocol.atlas.domain";
pub const PROTOCOL_VERSION: u64 = 1;

pub const ASSERTION_GENESIS: &str = "atlas.domain.genesis";
pub const ASSERTION_INVITE: &str = "atlas.domain.invite";
pub const ASSERTION_REQUEST: &str = "atlas.domain.request";
pub const ASSERTION_APPROVE: &str = "atlas.domain.approve";
pub const ASSERTION_REVOKE: &str = "atlas.domain.revoke";
pub const ASSERTION_LEAVE: &str = "atlas.domain.leave";
pub const ASSERTION_POLICY: &str = "atlas.domain.policy";
pub const ASSERTION_FREEZE: &str = "domain.freeze";
pub const ASSERTION_UNFREEZE: &str = "domain.unfreeze";
pub const ASSERTION_COMPROMISED: &str = "domain.compromised";

pub const DEFAULT_OWNERSHIP: &str = "domain";
pub const DEFAULT_TRANSFER_POLICY: &str = "forbidden";

const REQUIRED_FIELDS: &[&str] = &[
    "domain",
    "owner",
    "parent",
    "ownership_default",
    "transfer_policy",
    "backup_relay_domain",
    "backup_relay_plan",
    "roles",
    "scopes",
    "expires",
    "invites",
    "requests",
];

const REQUIRED_ACTIONS: &[&str] = &[
    "Genesis", "Invite", "Request", "Approve", "Revoke", "Leave", "Policy",
];

const REQUIRED_ASSERTIONS: &[&str] = &[
    ASSERTION_GENESIS,
    ASSERTION_INVITE,
    ASSERTION_REQUEST,
    ASSERTION_APPROVE,
    ASSERTION_REVOKE,
    ASSERTION_LEAVE,
    ASSERTION_POLICY,
    ASSERTION_FREEZE,
    ASSERTION_UNFREEZE,
    ASSERTION_COMPROMISED,
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

pub fn requires_parent_authorization(domain: &str) -> bool {
    domain.contains('.')
}
