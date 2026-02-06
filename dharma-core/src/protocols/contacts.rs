use super::{ProtocolEnumDef, ProtocolId, ProtocolInterface};

pub const PROTOCOL_NAME: &str = "std.protocol.contacts";
pub const PROTOCOL_VERSION: u64 = 1;

const REQUIRED_FIELDS: &[&str] = &[
    "owner",
    "contact",
    "relation",
    "requested_by",
    "blocked_by",
];

const REQUIRED_ACTIONS: &[&str] = &[
    "Create",
    "UpdateAlias",
    "Request",
    "Accept",
    "Decline",
    "Block",
    "Unblock",
    "Tag",
    "UpdateNotes",
];

const REQUIRED_ASSERTIONS: &[&str] = &[];

const RELATION_ENUM: ProtocolEnumDef = ProtocolEnumDef {
    name: "Relation",
    variants: &["None", "Pending", "Accepted", "Declined", "Blocked"],
};

pub fn interface() -> ProtocolInterface {
    ProtocolInterface {
        id: ProtocolId::new(PROTOCOL_NAME, PROTOCOL_VERSION),
        required_state_fields: REQUIRED_FIELDS,
        required_actions: REQUIRED_ACTIONS,
        required_assertions: REQUIRED_ASSERTIONS,
        required_enums: &[RELATION_ENUM],
        private_fields: &[],
    }
}
