use crate::sync::Subscriptions;
use crate::types::SubjectId;
use std::fs;
use std::path::Path;

#[derive(Clone, Debug)]
struct Rule {
    subject: Option<SubjectId>,
    namespace: Option<String>,
    allow: bool,
}

#[derive(Clone, Debug)]
struct SubscriptionPolicy {
    default_allow: bool,
    rules: Vec<Rule>,
}

impl SubscriptionPolicy {
    fn load(root: &Path) -> Self {
        let policy_path = root.join("subscriptions.policy");
        if let Ok(contents) = fs::read_to_string(&policy_path) {
            return SubscriptionPolicy::from_str(&contents);
        }
        let allow_path = root.join("subscriptions.allow");
        if let Ok(contents) = fs::read_to_string(&allow_path) {
            return SubscriptionPolicy::from_allowlist(&contents);
        }
        SubscriptionPolicy {
            default_allow: true,
            rules: Vec::new(),
        }
    }

    fn from_str(contents: &str) -> Self {
        let mut policy = SubscriptionPolicy {
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
        let mut policy = SubscriptionPolicy {
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
                policy.rules.push(Rule {
                    subject: Some(subject),
                    namespace: None,
                    allow: true,
                });
            } else {
                policy.rules.push(Rule {
                    subject: None,
                    namespace: Some(line.to_string()),
                    allow: true,
                });
            }
        }
        policy
    }

    fn to_subscriptions(&self) -> Subscriptions {
        if self.default_allow {
            return Subscriptions::all();
        }
        let mut subjects = Vec::new();
        let mut namespaces = Vec::new();
        for rule in &self.rules {
            if !rule.allow {
                continue;
            }
            if let Some(subject) = rule.subject {
                subjects.push(subject);
            }
            if let Some(ns) = &rule.namespace {
                namespaces.push(ns.clone());
            }
        }
        Subscriptions {
            all: false,
            subjects,
            namespaces,
        }
    }
}

pub fn load_subscriptions(root: &Path) -> Subscriptions {
    SubscriptionPolicy::load(root).to_subscriptions()
}

fn parse_rule(parts: &[&str]) -> Option<Rule> {
    if parts.len() < 2 {
        return None;
    }
    let allow = parse_allow(parts[parts.len() - 1])?;
    let mut subject = None;
    let mut namespace = None;
    let mut idx = 0;
    while idx + 1 < parts.len() {
        match parts[idx] {
            "subject" => {
                subject = SubjectId::from_hex(parts[idx + 1]).ok();
                idx += 2;
            }
            "namespace" => {
                namespace = Some(parts[idx + 1].to_string());
                idx += 2;
            }
            _ => break,
        }
    }
    if subject.is_none() && namespace.is_none() {
        return None;
    }
    Some(Rule {
        subject,
        namespace,
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
    fn policy_defaults_to_all() {
        let temp = tempfile::tempdir().unwrap();
        let subs = load_subscriptions(temp.path());
        assert!(subs.all);
    }

    #[test]
    fn allowlist_builds_subjects() {
        let temp = tempfile::tempdir().unwrap();
        let subject = SubjectId::from_bytes([1u8; 32]);
        fs::write(
            temp.path().join("subscriptions.allow"),
            format!("{}\n", subject.to_hex()),
        )
        .unwrap();
        let subs = load_subscriptions(temp.path());
        assert!(!subs.all);
        assert_eq!(subs.subjects, vec![subject]);
    }
}
