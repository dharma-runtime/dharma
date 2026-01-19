use crate::types::SubjectId;
use std::fs;
use std::path::Path;

#[derive(Clone, Debug)]
pub struct OverlayPolicy {
    default_allow: bool,
    verified_allow: Option<bool>,
    rules: Vec<OverlayRule>,
}

#[derive(Clone, Debug)]
struct OverlayRule {
    peer: Option<SubjectId>,
    subject: Option<SubjectId>,
    namespace: Option<String>,
    org: Option<String>,
    role: Option<String>,
    allow: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct OverlayAccess<'a> {
    policy: &'a OverlayPolicy,
    peer: Option<SubjectId>,
    verified: bool,
    claims: &'a PeerClaims,
}

#[derive(Clone, Debug, Default)]
pub struct PeerClaims {
    pub org: Option<String>,
    pub roles: Vec<String>,
}

impl OverlayPolicy {
    pub fn load(root: &Path) -> Self {
        let policy_path = root.join("overlays.policy");
        if let Ok(contents) = fs::read_to_string(&policy_path) {
            return OverlayPolicy::from_str(&contents);
        }
        let allowlist_path = root.join("overlays.allow");
        if let Ok(contents) = fs::read_to_string(&allowlist_path) {
            return OverlayPolicy::from_allowlist(&contents);
        }
        OverlayPolicy::default()
    }

    pub fn from_str(contents: &str) -> Self {
        let mut policy = OverlayPolicy::default();
        for line in contents.lines() {
            let line = strip_comment(line).trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }
            match parts.as_slice() {
                ["default", decision] => {
                    if let Some(allow) = parse_allow(decision) {
                        policy.default_allow = allow;
                    }
                }
                ["verified", decision] => {
                    if let Some(allow) = parse_allow(decision) {
                        policy.verified_allow = Some(allow);
                    }
                }
                _ => {
                    if let Some(rule) = parse_rule(&parts) {
                        policy.rules.push(rule);
                    }
                }
            }
        }
        policy
    }

    fn from_allowlist(contents: &str) -> Self {
        let mut policy = OverlayPolicy::default();
        for line in contents.lines() {
            let line = strip_comment(line).trim();
            if line.is_empty() {
                continue;
            }
            if line == "*" {
                policy.default_allow = true;
                continue;
            }
            if let Ok(peer) = SubjectId::from_hex(line) {
                policy.rules.push(OverlayRule {
                    peer: Some(peer),
                    subject: None,
                    namespace: None,
                    org: None,
                    role: None,
                    allow: true,
                });
            }
        }
        policy
    }

    pub fn allows(
        &self,
        peer: Option<SubjectId>,
        verified: bool,
        subject: &SubjectId,
        namespace: Option<&str>,
        claims: &PeerClaims,
    ) -> bool {
        let mut allowed = self.default_allow;
        if verified {
            if let Some(verified_allow) = self.verified_allow {
                allowed = verified_allow;
            }
        }
        for rule in &self.rules {
            if rule.matches(peer, subject, namespace, claims) {
                allowed = rule.allow;
            }
        }
        allowed
    }
}

impl Default for OverlayPolicy {
    fn default() -> Self {
        OverlayPolicy {
            default_allow: false,
            verified_allow: Some(true),
            rules: Vec::new(),
        }
    }
}

impl OverlayRule {
    fn matches(
        &self,
        peer: Option<SubjectId>,
        subject: &SubjectId,
        namespace: Option<&str>,
        claims: &PeerClaims,
    ) -> bool {
        if let Some(rule_peer) = self.peer {
            if Some(rule_peer) != peer {
                return false;
            }
        }
        if let Some(rule_subject) = self.subject {
            if &rule_subject != subject {
                return false;
            }
        }
        if let Some(rule_namespace) = &self.namespace {
            if namespace != Some(rule_namespace.as_str()) {
                return false;
            }
        }
        if let Some(rule_org) = &self.org {
            if claims.org.as_deref() != Some(rule_org.as_str()) {
                return false;
            }
        }
        if let Some(rule_role) = &self.role {
            if !claims.roles.iter().any(|r| r == rule_role) {
                return false;
            }
        }
        true
    }
}

impl<'a> OverlayAccess<'a> {
    pub fn new(
        policy: &'a OverlayPolicy,
        peer: Option<SubjectId>,
        verified: bool,
        claims: &'a PeerClaims,
    ) -> Self {
        OverlayAccess {
            policy,
            peer,
            verified,
            claims,
        }
    }

    pub fn allows(
        &self,
        subject: &SubjectId,
        namespace: Option<&str>,
    ) -> bool {
        self.policy
            .allows(self.peer, self.verified, subject, namespace, self.claims)
    }

    pub fn peer(&self) -> Option<SubjectId> {
        self.peer
    }
}

fn parse_rule(parts: &[&str]) -> Option<OverlayRule> {
    if parts.len() < 2 {
        return None;
    }
    let allow = parse_allow(parts[parts.len() - 1])?;
    let mut peer = None;
    let mut subject = None;
    let mut namespace = None;
    let mut org = None;
    let mut role = None;
    let mut idx = 0;
    while idx + 1 < parts.len() {
        match parts[idx] {
            "peer" => {
                peer = SubjectId::from_hex(parts[idx + 1]).ok();
                idx += 2;
            }
            "subject" => {
                subject = SubjectId::from_hex(parts[idx + 1]).ok();
                idx += 2;
            }
            "namespace" => {
                namespace = Some(parts[idx + 1].to_string());
                idx += 2;
            }
            "org" => {
                org = Some(parts[idx + 1].to_string());
                idx += 2;
            }
            "role" => {
                role = Some(parts[idx + 1].to_string());
                idx += 2;
            }
            _ => break,
        }
    }
    if peer.is_none() && subject.is_none() && namespace.is_none() && org.is_none() && role.is_none() {
        return None;
    }
    Some(OverlayRule {
        peer,
        subject,
        namespace,
        org,
        role,
        allow,
    })
}

fn parse_allow(value: &str) -> Option<bool> {
    match value {
        "allow" | "true" | "yes" => Some(true),
        "deny" | "false" | "no" => Some(false),
        _ => None,
    }
}

fn strip_comment(line: &str) -> &str {
    match line.find('#') {
        Some(idx) => &line[..idx],
        None => line,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SubjectId;

    #[test]
    fn default_policy_allows_verified_only() {
        let policy = OverlayPolicy::default();
        let subject = SubjectId::from_bytes([1u8; 32]);
        let peer = SubjectId::from_bytes([2u8; 32]);
        let claims = PeerClaims::default();
        assert!(!policy.allows(Some(peer), false, &subject, None, &claims));
        assert!(policy.allows(Some(peer), true, &subject, None, &claims));
    }

    #[test]
    fn policy_allows_subject_rule() {
        let subject = SubjectId::from_bytes([3u8; 32]);
        let other = SubjectId::from_bytes([4u8; 32]);
        let policy = OverlayPolicy::from_str(&format!(
            "default deny\nsubject {} allow\n",
            subject.to_hex()
        ));
        let peer = SubjectId::from_bytes([5u8; 32]);
        let claims = PeerClaims::default();
        assert!(policy.allows(Some(peer), false, &subject, None, &claims));
        assert!(!policy.allows(Some(peer), false, &other, None, &claims));
    }

    #[test]
    fn policy_allows_namespace_rule() {
        let subject = SubjectId::from_bytes([6u8; 32]);
        let peer = SubjectId::from_bytes([7u8; 32]);
        let policy = OverlayPolicy::from_str("default deny\nnamespace com.test allow\n");
        let claims = PeerClaims::default();
        assert!(policy.allows(Some(peer), false, &subject, Some("com.test"), &claims));
        assert!(!policy.allows(Some(peer), false, &subject, Some("com.other"), &claims));
    }

    #[test]
    fn policy_allows_peer_scoped_rule() {
        let subject = SubjectId::from_bytes([8u8; 32]);
        let peer = SubjectId::from_bytes([9u8; 32]);
        let other_peer = SubjectId::from_bytes([10u8; 32]);
        let policy = OverlayPolicy::from_str(&format!(
            "default deny\npeer {} subject {} allow\n",
            peer.to_hex(),
            subject.to_hex()
        ));
        let claims = PeerClaims::default();
        assert!(policy.allows(Some(peer), false, &subject, None, &claims));
        assert!(!policy.allows(Some(other_peer), false, &subject, None, &claims));
    }

    #[test]
    fn allowlist_fallback_maps_to_peer_rules() {
        let peer = SubjectId::from_bytes([11u8; 32]);
        let subject = SubjectId::from_bytes([12u8; 32]);
        let policy = OverlayPolicy::from_allowlist(&format!("{}\n", peer.to_hex()));
        assert!(policy.allows(Some(peer), false, &subject, None, &PeerClaims::default()));
        let other_peer = SubjectId::from_bytes([13u8; 32]);
        assert!(!policy.allows(Some(other_peer), false, &subject, None, &PeerClaims::default()));
    }

    #[test]
    fn policy_allows_org_rule() {
        let subject = SubjectId::from_bytes([14u8; 32]);
        let peer = SubjectId::from_bytes([15u8; 32]);
        let policy = OverlayPolicy::from_str("default deny\norg cmdv allow\n");
        let claims = PeerClaims {
            org: Some("cmdv".to_string()),
            roles: Vec::new(),
        };
        assert!(policy.allows(Some(peer), false, &subject, None, &claims));
        let claims_other = PeerClaims {
            org: Some("other".to_string()),
            roles: Vec::new(),
        };
        assert!(!policy.allows(Some(peer), false, &subject, None, &claims_other));
    }

    #[test]
    fn policy_allows_role_rule() {
        let subject = SubjectId::from_bytes([16u8; 32]);
        let peer = SubjectId::from_bytes([17u8; 32]);
        let policy = OverlayPolicy::from_str("default deny\nrole Accountant allow\n");
        let claims = PeerClaims {
            org: None,
            roles: vec!["Accountant".to_string()],
        };
        assert!(policy.allows(Some(peer), false, &subject, None, &claims));
        let claims_other = PeerClaims {
            org: None,
            roles: vec!["Viewer".to_string()],
        };
        assert!(!policy.allows(Some(peer), false, &subject, None, &claims_other));
    }
}
