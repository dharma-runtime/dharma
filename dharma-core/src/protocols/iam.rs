use super::{ProtocolId, ProtocolInterface};
use crate::pdl::schema::{CqrsSchema, Visibility};
use std::collections::BTreeSet;

pub const PROTOCOL_NAME: &str = "std.protocol.iam";
pub const PROTOCOL_VERSION: u64 = 1;

const REQUIRED_FIELDS: &[&str] = &[
    "display_name",
    "email",
    "phone",
    "handle",
    "keys",
    "profile",
    "delegates",
];

const REQUIRED_ACTIONS: &[&str] = &[
    "UpdateDisplayName",
    "UpdateEmail",
    "UpdatePhone",
    "UpdateProfile",
    "Delegate",
    "RevokeDelegate",
];

const REQUIRED_ASSERTIONS: &[&str] = &[];

const PRIVATE_FIELDS: &[&str] = &["display_name", "email", "phone"];

pub fn interface() -> ProtocolInterface {
    ProtocolInterface {
        id: ProtocolId::new(PROTOCOL_NAME, PROTOCOL_VERSION),
        required_state_fields: REQUIRED_FIELDS,
        required_actions: REQUIRED_ACTIONS,
        required_assertions: REQUIRED_ASSERTIONS,
        required_enums: &[],
        private_fields: PRIVATE_FIELDS,
    }
}

pub fn private_fields() -> &'static [&'static str] {
    PRIVATE_FIELDS
}

pub fn is_private_field(name: &str) -> bool {
    PRIVATE_FIELDS.iter().any(|field| *field == name)
}

pub fn public_fields(schema: &CqrsSchema) -> BTreeSet<String> {
    let mut fields = BTreeSet::new();
    for (name, field) in &schema.fields {
        if field.visibility == Visibility::Private {
            continue;
        }
        if is_private_field(name) {
            continue;
        }
        fields.insert(name.clone());
    }
    fields
}
