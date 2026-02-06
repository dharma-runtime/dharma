use crate::types::{IdentityKey, SubjectId};
use std::fs;
use std::path::Path;

#[derive(Clone, Debug)]
pub struct PeerPolicy {
    default_allow: bool,
    rules: Vec<PeerRule>,
}

#[derive(Clone, Debug)]
struct PeerRule {
    subject: Option<SubjectId>,
    key: Option<IdentityKey>,
    allow: bool,
}

impl PeerPolicy {
    pub fn load(root: &Path) -> Self {
        let policy_path = root.join("peers.policy");
        if let Ok(contents) = fs::read_to_string(&policy_path) {
            return PeerPolicy::from_str(&contents);
        }
        let allow_path = root.join("peers.allow");
        if let Ok(contents) = fs::read_to_string(&allow_path) {
            return PeerPolicy::from_allowlist(&contents);
        }
        PeerPolicy {
            default_allow: true,
            rules: Vec::new(),
        }
    }

    pub fn allows(&self, subject: SubjectId, key: IdentityKey) -> bool {
        let mut allowed = self.default_allow;
        for rule in &self.rules {
            if rule.matches(subject, key) {
                allowed = rule.allow;
            }
        }
        allowed
    }

    fn from_str(contents: &str) -> Self {
        let mut policy = PeerPolicy {
            default_allow: false,
            rules: Vec::new(),
        };
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
        let mut policy = PeerPolicy {
            default_allow: false,
            rules: Vec::new(),
        };
        for line in contents.lines() {
            let line = strip_comment(line).trim();
            if line.is_empty() {
                continue;
            }
            if line == "*" {
                policy.default_allow = true;
                continue;
            }
            if let Ok(subject) = SubjectId::from_hex(line) {
                policy.rules.push(PeerRule {
                    subject: Some(subject),
                    key: None,
                    allow: true,
                });
            }
        }
        policy
    }
}

impl PeerRule {
    fn matches(&self, subject: SubjectId, key: IdentityKey) -> bool {
        if let Some(rule_subject) = self.subject {
            if rule_subject != subject {
                return false;
            }
        }
        if let Some(rule_key) = &self.key {
            if rule_key.as_bytes() != key.as_bytes() {
                return false;
            }
        }
        true
    }
}

fn parse_rule(parts: &[&str]) -> Option<PeerRule> {
    if parts.len() < 2 {
        return None;
    }
    let allow = parse_allow(parts[parts.len() - 1])?;
    let mut subject = None;
    let mut key = None;
    let mut idx = 0;
    while idx + 1 < parts.len() {
        match parts[idx] {
            "peer" | "subject" => {
                subject = SubjectId::from_hex(parts[idx + 1]).ok();
                idx += 2;
            }
            "key" => {
                let bytes = crate::types::hex_decode(parts[idx + 1]).ok()?;
                if bytes.len() != 32 {
                    return None;
                }
                key = Some(IdentityKey::from_slice(&bytes).ok()?);
                idx += 2;
            }
            _ => break,
        }
    }
    if subject.is_none() && key.is_none() {
        return None;
    }
    Some(PeerRule {
        subject,
        key,
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

    #[test]
    fn policy_defaults_to_allow() {
        let temp = tempfile::tempdir().unwrap();
        let policy = PeerPolicy::load(temp.path());
        let subject = SubjectId::from_bytes([1u8; 32]);
        let key = IdentityKey::from_bytes([2u8; 32]);
        assert!(policy.allows(subject, key));
    }

    #[test]
    fn allowlist_respects_subject() {
        let temp = tempfile::tempdir().unwrap();
        let subject = SubjectId::from_bytes([3u8; 32]);
        fs::write(
            temp.path().join("peers.allow"),
            format!("{}\n", subject.to_hex()),
        )
        .unwrap();
        let policy = PeerPolicy::load(temp.path());
        let key = IdentityKey::from_bytes([4u8; 32]);
        assert!(policy.allows(subject, key));
        let other = SubjectId::from_bytes([5u8; 32]);
        assert!(!policy.allows(other, key));
    }
}
