use crate::domain::DomainState;
use crate::error::DharmaError;
use crate::ownership::Owner;
use crate::relay::RelayState;
use crate::store::state::load_ownership;
use crate::store::Store;
use crate::types::SubjectId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackupPolicyStatus {
    Defined {
        domain: String,
        relay_domain: String,
        plan: String,
    },
    MissingDomainPolicy {
        domain: String,
    },
    MissingRelayDomain {
        domain: String,
        relay_domain: String,
    },
    MissingRelayPlan {
        domain: String,
        relay_domain: String,
        plan: String,
    },
    MissingRelayGrant {
        domain: String,
        relay_domain: String,
        plan: String,
    },
    OwnerIdentity,
    MissingOwnership,
    MissingDomainName,
}

pub fn backup_policy_status(
    store: &Store,
    subject: &SubjectId,
) -> Result<BackupPolicyStatus, DharmaError> {
    let Some(record) = load_ownership(store.env(), subject)? else {
        return Ok(BackupPolicyStatus::MissingOwnership);
    };
    let domain_subject = match record.owner {
        Owner::Domain(domain_subject) => domain_subject,
        Owner::Identity(_) => return Ok(BackupPolicyStatus::OwnerIdentity),
    };
    let domain_state = DomainState::load(store, &domain_subject)?;
    let Some(domain_name) = domain_state.domain else {
        return Ok(BackupPolicyStatus::MissingDomainName);
    };
    let Some(relay_domain) = domain_state.backup_relay_domain else {
        return Ok(BackupPolicyStatus::MissingDomainPolicy {
            domain: domain_name,
        });
    };
    let Some(relay_plan) = domain_state.backup_relay_plan else {
        return Ok(BackupPolicyStatus::MissingDomainPolicy {
            domain: domain_name,
        });
    };
    let Some(relay_state) = RelayState::load(store, &relay_domain)? else {
        return Ok(BackupPolicyStatus::MissingRelayDomain {
            domain: domain_name,
            relay_domain,
        });
    };
    if relay_state.plans.get(&relay_plan).is_none() {
        return Ok(BackupPolicyStatus::MissingRelayPlan {
            domain: domain_name,
            relay_domain,
            plan: relay_plan,
        });
    }
    let now = store.env().now();
    let Some(grant) = relay_state.grant_for_domain(&domain_name, now) else {
        return Ok(BackupPolicyStatus::MissingRelayGrant {
            domain: domain_name,
            relay_domain,
            plan: relay_plan,
        });
    };
    if grant.plan != relay_plan {
        return Ok(BackupPolicyStatus::MissingRelayGrant {
            domain: domain_name,
            relay_domain,
            plan: relay_plan,
        });
    }
    Ok(BackupPolicyStatus::Defined {
        domain: domain_name,
        relay_domain,
        plan: relay_plan,
    })
}
