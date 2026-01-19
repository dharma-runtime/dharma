use crate::identity_store;
use crate::dharmaq_core::QueryRow;
use crate::repl::aliases::{alias_for_subject, load_aliases, AliasMap};
use crate::IdentityState;
use crate::types::SubjectId;
use crate::FrontierIndex;
use crate::store::state::load_latest_snapshot_for_ver;
use crossterm::style::{Color, Stylize};
use std::fs;
use std::path::PathBuf;

pub struct ReplContext {
    pub identity: Option<IdentityState>,
    pub current_subject: Option<SubjectId>,
    pub current_alias: Option<String>,
    pub current_lens: u64,
    pub profile: Profile,
    pub data_dir: PathBuf,
    pub json: bool,
    pub color: bool,
    pub confirmations: bool,
    pub aliases: AliasMap,
    pub last_results: Vec<QueryRow>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Profile {
    Home,
    Pro,
    HighSec,
}

impl ReplContext {
    pub fn new() -> Self {
        let (data_dir, profile) = match std::env::current_dir() {
            Ok(root) => match dharma::config::Config::load(&root) {
                Ok(cfg) => {
                    let profile = match cfg.profile.mode.as_str() {
                        "server" => Profile::Pro,
                        "highsec" => Profile::HighSec,
                        _ => Profile::Home,
                    };
                    (cfg.storage_path(&root), profile)
                }
                Err(_) => (PathBuf::from("data"), Profile::Home),
            },
            Err(_) => (PathBuf::from("data"), Profile::Home),
        };
        let aliases = load_aliases(&data_dir).unwrap_or_default();
        Self {
            identity: None,
            current_subject: None,
            current_alias: None,
            current_lens: 1,
            profile,
            data_dir,
            json: false,
            color: true,
            confirmations: true,
            aliases,
            last_results: Vec::new(),
        }
    }

    pub fn prompt(&self) -> String {
        let (identity_label, locked) = self.identity_label();
        let lock_icon = if locked { "🔒" } else { "🔓" };
        let lock_icon = self.paint(lock_icon, if locked { Color::Red } else { Color::Green });
        let id_label = self.paint(&identity_label, Color::Green);
        let id_seg = format!("[👤 {id_label} {lock_icon}]");

        let subject_label = self.subject_label();
        let dirty = self.subject_dirty();
        let dirty_mark = if dirty { self.paint("*", Color::Red) } else { "".to_string() };
        let sub_seg = format!("[📄 {}{}]", self.paint(&subject_label, Color::Blue), dirty_mark);

        let lens_seg = format!("[👓 v{}]", self.paint(&self.current_lens.to_string(), Color::Magenta));

        let (online, peers) = self.network_status();
        let net_label = if online { "Online" } else { "Offline" };
        let net_color = if online { Color::Green } else { Color::Red };
        let net_label = self.paint(net_label, net_color);
        let peer_label = self.paint(&format!("👥 {peers}"), Color::Cyan);
        let net_seg = format!("[📡 {net_label} {peer_label}]");

        let profile = self.profile_name();
        let profile_seg = format!("[🛡️ {}]", self.paint(profile, Color::Yellow));

        let line1 = format!("┌─{id_seg}─{sub_seg}─{lens_seg}───{net_seg}─{profile_seg}");
        let line2 = "└─> ".to_string();
        format!("{line1}\n{line2}")
    }

    pub fn refresh_identity(&mut self) {
        let env = dharma::env::StdEnv::new(&self.data_dir);
        if let Ok(id) = identity_store::load_identity_if_unlocked(&env) {
            self.current_subject = Some(id.subject_id);
            self.current_alias = self.alias_for_subject(&id.subject_id);
            self.identity = Some(id);
        }
    }

    pub fn alias_for_subject(&self, subject: &SubjectId) -> Option<String> {
        alias_for_subject(&self.aliases, subject)
    }

    fn paint(&self, text: &str, color: Color) -> String {
        if self.color {
            format!("{}", text.with(color))
        } else {
            text.to_string()
        }
    }

    fn identity_label(&self) -> (String, bool) {
        if let Some(id) = &self.identity {
            let alias = self.alias_for_subject(&id.subject_id);
            let label = alias.unwrap_or_else(|| short_hex(&id.public_key.to_hex(), 8));
            return (label, false);
        }
        let env = dharma::env::StdEnv::new(&self.data_dir);
        if identity_store::identity_exists(&env) {
            if let Ok(subject) = identity_store::read_identity_subject(&env) {
                let alias = self.alias_for_subject(&subject);
                let label = alias.unwrap_or_else(|| short_hex(&subject.to_hex(), 8));
                return (label, true);
            }
        }
        ("anon".to_string(), true)
    }

    fn subject_label(&self) -> String {
        if let Some(alias) = &self.current_alias {
            return alias.clone();
        }
        if let Some(subject) = &self.current_subject {
            return short_hex(&subject.to_hex(), 8);
        }
        "no-subject".to_string()
    }

    fn subject_dirty(&self) -> bool {
        let Some(subject) = &self.current_subject else {
            return false;
        };
        let index = FrontierIndex::new(&self.data_dir).ok();
        let max_seq = index
            .as_ref()
            .and_then(|idx| idx.max_seq_for_ver(subject, self.current_lens))
            .unwrap_or(0);
        if max_seq == 0 {
            return false;
        }
        let env = dharma::env::StdEnv::new(&self.data_dir);
        let snapshot = load_latest_snapshot_for_ver(&env, subject, self.current_lens)
            .ok()
            .flatten();
        let snap_seq = snapshot.map(|s| s.header.seq).unwrap_or(0);
        snap_seq < max_seq
    }

    fn network_status(&self) -> (bool, usize) {
        let peers = peer_count(&self.data_dir);
        let online = peers > 0 || discovery_enabled(&self.data_dir, self.profile);
        (online, peers)
    }

    fn profile_name(&self) -> &'static str {
        match self.profile {
            Profile::Home => "Home",
            Profile::Pro => "Pro",
            Profile::HighSec => "HighSec",
        }
    }
}

fn short_hex(hex: &str, take: usize) -> String {
    if hex.len() <= take {
        hex.to_string()
    } else {
        format!("{}...", &hex[..take])
    }
}

fn peer_count(root: &PathBuf) -> usize {
    let path = root.join("peers.list");
    let Ok(contents) = fs::read_to_string(path) else {
        return 0;
    };
    contents
        .lines()
        .filter(|line| {
            let line = line.split('#').next().unwrap_or("").trim();
            !line.is_empty()
        })
        .count()
}

fn discovery_enabled(root: &PathBuf, profile: Profile) -> bool {
    let path = root.join("discovery.enabled");
    if let Ok(contents) = fs::read_to_string(path) {
        return contents.trim() != "off";
    }
    !matches!(profile, Profile::HighSec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn peer_count_ignores_empty_lines() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_path_buf();
        fs::write(root.join("peers.list"), "127.0.0.1:1\n\n# comment\n").unwrap();
        assert_eq!(peer_count(&root), 1);
    }

    #[test]
    fn discovery_enabled_defaults_by_profile() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_path_buf();
        assert!(discovery_enabled(&root, Profile::Home));
        assert!(!discovery_enabled(&root, Profile::HighSec));
    }
}
