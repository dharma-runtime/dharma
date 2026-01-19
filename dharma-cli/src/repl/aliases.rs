use crate::error::DharmaError;
use crate::types::SubjectId;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

pub type AliasMap = BTreeMap<String, SubjectId>;

const ALIASES_FILE: &str = "aliases.toml";

pub fn aliases_path(data_dir: &Path) -> PathBuf {
    data_dir.join(ALIASES_FILE)
}

pub fn load_aliases(data_dir: &Path) -> Result<AliasMap, DharmaError> {
    let path = aliases_path(data_dir);
    if !path.exists() {
        return Ok(AliasMap::new());
    }
    let contents = fs::read_to_string(path)?;
    let mut map = AliasMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        let name = name.trim();
        let value = value.trim().trim_matches('"');
        if name.is_empty() {
            continue;
        }
        if let Ok(subject) = SubjectId::from_hex(value) {
            map.insert(name.to_string(), subject);
        }
    }
    Ok(map)
}

pub fn save_aliases(data_dir: &Path, aliases: &AliasMap) -> Result<(), DharmaError> {
    if !data_dir.exists() {
        fs::create_dir_all(data_dir)?;
    }
    let mut out = String::new();
    for (alias, subject) in aliases {
        out.push_str(&format!("{alias} = \"{}\"\n", subject.to_hex()));
    }
    fs::write(aliases_path(data_dir), out)?;
    Ok(())
}

pub fn alias_for_subject(aliases: &AliasMap, subject: &SubjectId) -> Option<String> {
    aliases
        .iter()
        .find(|(_, value)| *value == subject)
        .map(|(alias, _)| alias.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn aliases_roundtrip() {
        let dir = tempdir().unwrap();
        let subject = SubjectId::from_bytes([7u8; 32]);
        let mut aliases = AliasMap::new();
        aliases.insert("alice".to_string(), subject);
        save_aliases(dir.path(), &aliases).unwrap();
        let loaded = load_aliases(dir.path()).unwrap();
        assert_eq!(loaded.get("alice").unwrap().as_bytes(), subject.as_bytes());
    }

    #[test]
    fn alias_lookup_by_subject() {
        let subject = SubjectId::from_bytes([9u8; 32]);
        let mut aliases = AliasMap::new();
        aliases.insert("bob".to_string(), subject);
        assert_eq!(
            alias_for_subject(&aliases, &subject).as_deref(),
            Some("bob")
        );
    }
}
