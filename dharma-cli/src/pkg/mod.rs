use crate::assertion::AssertionPlaintext;
use crate::crypto;
use crate::error::DharmaError;
use crate::store::state::list_assertions;
use crate::store::Store;
use crate::types::{EnvelopeId, IdentityKey, SubjectId};
use crate::value::{expect_array, expect_bytes, expect_map, expect_text, map_get};
use ciborium::value::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct PackageVersion {
    pub ver: u64,
    pub schema: EnvelopeId,
    pub contract: EnvelopeId,
    pub reactor: Option<EnvelopeId>,
    pub deps: Vec<PackageDep>,
}

#[derive(Clone, Debug)]
pub struct PackageDep {
    pub name: String,
    pub ver: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct PackageManifest {
    pub name: String,
    pub versions: BTreeMap<u64, PackageVersion>,
    pub pinned: Option<u64>,
    pub registry_subject: Option<SubjectId>,
    pub registry_object: Option<EnvelopeId>,
    pub publisher: Option<IdentityKey>,
}

#[derive(Clone, Debug)]
pub struct RegistryPackage {
    pub name: String,
    pub subject: SubjectId,
    pub object_id: EnvelopeId,
    pub publisher: IdentityKey,
    pub versions: BTreeMap<u64, PackageVersion>,
}

pub fn packages_root(root: &Path) -> PathBuf {
    root.join("packages")
}

pub fn manifest_path(root: &Path, name: &str) -> PathBuf {
    packages_root(root).join(name).join("manifest.cbor")
}

pub fn load_manifest(root: &Path, name: &str) -> Result<Option<PackageManifest>, DharmaError> {
    let path = manifest_path(root, name);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    let value = crate::cbor::ensure_canonical(&bytes)?;
    let manifest = parse_manifest(&value)?;
    Ok(Some(manifest))
}

pub fn save_manifest(root: &Path, manifest: &PackageManifest) -> Result<(), DharmaError> {
    let path = manifest_path(root, &manifest.name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let value = manifest_to_value(manifest);
    let bytes = crate::cbor::encode_canonical_value(&value)?;
    fs::write(path, bytes)?;
    Ok(())
}

pub fn list_installed(root: &Path) -> Result<Vec<PackageManifest>, DharmaError> {
    let mut out = Vec::new();
    let root_dir = packages_root(root);
    if !root_dir.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(root_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(manifest) = load_manifest(root, &name)? {
            out.push(manifest);
        }
    }
    Ok(out)
}

pub fn find_registry_packages(
    root: &Path,
    name: &str,
    registry_subject: Option<SubjectId>,
) -> Result<Vec<RegistryPackage>, DharmaError> {
    let store = Store::from_root(root);
    let subjects = if let Some(subject) = registry_subject {
        vec![subject]
    } else {
        store.list_subjects()?
    };
    let mut out = Vec::new();
    for subject in subjects {
        let records = list_assertions(store.env(), &subject)?;
        for record in records {
            let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
                Ok(a) => a,
                Err(_) => continue,
            };
            if !assertion.header.typ.starts_with("sys.package") {
                continue;
            }
            if let Some(pkg) = parse_registry_assertion(&assertion, record.envelope_id) {
                if pkg.name == name {
                    out.push(pkg);
                }
            }
        }
    }
    Ok(out)
}

pub fn select_best_version(versions: &BTreeMap<u64, PackageVersion>) -> Option<PackageVersion> {
    versions.values().max_by_key(|v| v.ver).cloned()
}

pub fn install_from_registry(
    root: &Path,
    registry: &RegistryPackage,
    preferred_ver: Option<u64>,
) -> Result<PackageManifest, DharmaError> {
    install_from_registry_with_fetch(root, registry, preferred_ver, &mut noop_fetch)
}

pub fn install_from_registry_with_fetch<F>(
    root: &Path,
    registry: &RegistryPackage,
    preferred_ver: Option<u64>,
    fetch: &mut F,
) -> Result<PackageManifest, DharmaError>
where
    F: FnMut(&[EnvelopeId]) -> Result<(), DharmaError>,
{
    let selected = preferred_ver
        .and_then(|v| registry.versions.get(&v).cloned())
        .or_else(|| select_best_version(&registry.versions))
        .ok_or_else(|| DharmaError::Validation("no package versions found".to_string()))?;
    let missing = missing_artifacts(root, std::iter::once(&selected))?;
    if !missing.is_empty() {
        fetch(&missing)?;
    }
    ensure_artifacts_present(root, std::iter::once(&selected))?;
    let manifest = PackageManifest {
        name: registry.name.clone(),
        versions: registry.versions.clone(),
        pinned: Some(selected.ver),
        registry_subject: Some(registry.subject),
        registry_object: Some(registry.object_id),
        publisher: Some(registry.publisher),
    };
    save_manifest(root, &manifest)?;
    Ok(manifest)
}

pub fn verify_manifest(root: &Path, manifest: &PackageManifest) -> Result<VerifyReport, DharmaError> {
    let mut missing = Vec::new();
    let mut mismatched = Vec::new();
    for version in manifest.versions.values() {
        verify_object(root, &version.schema, &mut missing, &mut mismatched)?;
        verify_object(root, &version.contract, &mut missing, &mut mismatched)?;
        if let Some(reactor) = version.reactor {
            verify_object(root, &reactor, &mut missing, &mut mismatched)?;
        }
    }

    let mut registry_sig_ok = None;
    if let (Some(subject), Some(object_id)) =
        (manifest.registry_subject, manifest.registry_object)
    {
        let store = Store::from_root(root);
        if let Ok(bytes) = store.get_object(&object_id) {
            if let Ok(assertion) = AssertionPlaintext::from_cbor(&bytes) {
                registry_sig_ok = Some(assertion.verify_signature()?);
                if assertion.header.sub != subject {
                    registry_sig_ok = Some(false);
                }
            }
        }
    }
    Ok(VerifyReport {
        missing,
        mismatched,
        registry_sig_ok,
    })
}

#[derive(Clone, Debug)]
pub struct VerifyReport {
    pub missing: Vec<EnvelopeId>,
    pub mismatched: Vec<EnvelopeId>,
    pub registry_sig_ok: Option<bool>,
}

pub fn update_config_for_manifest(root: &Path, manifest: &PackageManifest) -> Result<(), DharmaError> {
    let path = root.join("dharma.toml");
    let mut lines = Vec::new();
    if path.exists() {
        let contents = fs::read_to_string(&path)?;
        for line in contents.lines() {
            let trimmed = line.trim();
            let key = trimmed.split('=').next().unwrap_or("").trim();
            if should_filter_key(key, manifest) {
                continue;
            }
            if !trimmed.is_empty() {
                lines.push(line.to_string());
            }
        }
    }
    for version in manifest.versions.values() {
        let schema_key = format!("schema_v{}", version.ver);
        let contract_key = format!("contract_v{}", version.ver);
        let reactor_key = format!("reactor_v{}", version.ver);
        lines.push(format!("{schema_key} = \"{}\"", version.schema.to_hex()));
        lines.push(format!("{contract_key} = \"{}\"", version.contract.to_hex()));
        if let Some(reactor) = version.reactor {
            lines.push(format!("{reactor_key} = \"{}\"", reactor.to_hex()));
        }
        if version.ver == crate::assertion::DEFAULT_DATA_VERSION {
            lines.push(format!("schema = \"{}\"", version.schema.to_hex()));
            lines.push(format!("contract = \"{}\"", version.contract.to_hex()));
            if let Some(reactor) = version.reactor {
                lines.push(format!("reactor = \"{}\"", reactor.to_hex()));
            }
        }
    }
    fs::write(path, lines.join("\n") + "\n")?;
    Ok(())
}

pub fn remove_manifest(root: &Path, manifest: &PackageManifest, keep_cache: bool) -> Result<(), DharmaError> {
    remove_config_entries(root, manifest)?;
    if !keep_cache {
        let path = packages_root(root).join(&manifest.name);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
    }
    Ok(())
}

pub fn pin_manifest(
    root: &Path,
    manifest: &mut PackageManifest,
    artifact: &EnvelopeId,
) -> Result<Option<u64>, DharmaError> {
    let mut matched = None;
    for version in manifest.versions.values() {
        if &version.schema == artifact || &version.contract == artifact {
            matched = Some(version.ver);
            break;
        }
    }
    if let Some(ver) = matched {
        manifest.pinned = Some(ver);
        save_manifest(root, manifest)?;
        update_config_for_manifest(root, manifest)?;
    }
    Ok(matched)
}

fn parse_registry_assertion(
    assertion: &AssertionPlaintext,
    object_id: EnvelopeId,
) -> Option<RegistryPackage> {
    let map = expect_map(&assertion.body).ok()?;
    let name = map_get(map, "name").and_then(|v| expect_text(v).ok())?;
    let versions_value = map_get(map, "versions")?;
    let versions_map = expect_map(versions_value).ok()?;
    let mut versions = BTreeMap::new();
    for (k, v) in versions_map {
        let key = expect_text(k).ok()?;
        let ver = parse_version_key(&key)?;
        let mut entry = parse_version_entry(v)?;
        entry.ver = ver;
        versions.insert(ver, entry);
    }
    if versions.is_empty() {
        return None;
    }
    Some(RegistryPackage {
        name,
        subject: assertion.header.sub,
        object_id,
        publisher: assertion.header.auth,
        versions,
    })
}

fn parse_version_entry(value: &Value) -> Option<PackageVersion> {
    let map = expect_map(value).ok()?;
    let schema = map_get(map, "schema")
        .or_else(|| map_get(map, "base_schema"))
        .and_then(|v| expect_bytes(v).ok())
        .and_then(|b| EnvelopeId::from_slice(&b).ok())?;
    let contract = map_get(map, "contract")
        .and_then(|v| expect_bytes(v).ok())
        .and_then(|b| EnvelopeId::from_slice(&b).ok())?;
    let reactor = map_get(map, "reactor")
        .and_then(|v| expect_bytes(v).ok())
        .and_then(|b| EnvelopeId::from_slice(&b).ok());
    let deps = map_get(map, "deps")
        .and_then(|v| parse_deps(v).ok())
        .unwrap_or_default();
    let ver = map_get(map, "ver")
        .and_then(|v| v.as_integer())
        .and_then(|i| i64::try_from(i).ok())
        .map(|i| i as u64)
        .unwrap_or(0);
    Some(PackageVersion {
        ver: if ver == 0 { 0 } else { ver },
        schema,
        contract,
        reactor,
        deps,
    })
}

fn parse_deps(value: &Value) -> Result<Vec<PackageDep>, DharmaError> {
    let list = expect_array(value)?;
    let mut deps = Vec::new();
    for item in list {
        let map = expect_map(item)?;
        let name = map_get(map, "name")
            .and_then(|v| expect_text(v).ok())
            .unwrap_or_default();
        let ver = map_get(map, "ver")
            .and_then(|v| v.as_integer())
            .and_then(|i| i64::try_from(i).ok())
            .map(|i| i as u64);
        if !name.is_empty() {
            deps.push(PackageDep { name, ver });
        }
    }
    Ok(deps)
}

fn parse_manifest(value: &Value) -> Result<PackageManifest, DharmaError> {
    let map = expect_map(value)?;
    let name = map_get(map, "name")
        .and_then(|v| expect_text(v).ok())
        .ok_or_else(|| DharmaError::Validation("missing package name".to_string()))?;
    let versions_value = map_get(map, "versions")
        .ok_or_else(|| DharmaError::Validation("missing versions".to_string()))?;
    let versions_map = expect_map(versions_value)?;
    let mut versions = BTreeMap::new();
    for (k, v) in versions_map {
        let key = expect_text(k)?;
        let ver = parse_version_key(&key).ok_or_else(|| DharmaError::Validation("bad version".to_string()))?;
        let entry = parse_version_entry(v).ok_or_else(|| DharmaError::Validation("bad version entry".to_string()))?;
        let mut entry = entry;
        entry.ver = ver;
        versions.insert(ver, entry);
    }
    let pinned = map_get(map, "pinned")
        .and_then(|v| v.as_integer())
        .and_then(|i| i64::try_from(i).ok())
        .map(|i| i as u64);
    let registry_subject = map_get(map, "registry_subject")
        .and_then(|v| expect_bytes(v).ok())
        .and_then(|b| SubjectId::from_slice(&b).ok());
    let registry_object = map_get(map, "registry_object")
        .and_then(|v| expect_bytes(v).ok())
        .and_then(|b| EnvelopeId::from_slice(&b).ok());
    let publisher = map_get(map, "publisher")
        .and_then(|v| expect_bytes(v).ok())
        .and_then(|b| IdentityKey::from_slice(&b).ok());
    Ok(PackageManifest {
        name,
        versions,
        pinned,
        registry_subject,
        registry_object,
        publisher,
    })
}

fn manifest_to_value(manifest: &PackageManifest) -> Value {
    let mut versions = Vec::new();
    for (ver, entry) in &manifest.versions {
        versions.push((
            Value::Text(ver.to_string()),
            version_to_value(entry),
        ));
    }
    let mut map = Vec::new();
    map.push((Value::Text("name".to_string()), Value::Text(manifest.name.clone())));
    map.push((Value::Text("versions".to_string()), Value::Map(versions)));
    if let Some(pinned) = manifest.pinned {
        map.push((Value::Text("pinned".to_string()), Value::Integer((pinned as i64).into())));
    }
    if let Some(subject) = manifest.registry_subject {
        map.push((Value::Text("registry_subject".to_string()), Value::Bytes(subject.as_bytes().to_vec())));
    }
    if let Some(object) = manifest.registry_object {
        map.push((Value::Text("registry_object".to_string()), Value::Bytes(object.as_bytes().to_vec())));
    }
    if let Some(publisher) = manifest.publisher {
        map.push((Value::Text("publisher".to_string()), Value::Bytes(publisher.as_bytes().to_vec())));
    }
    Value::Map(map)
}

fn version_to_value(version: &PackageVersion) -> Value {
    let mut map = Vec::new();
    map.push((Value::Text("schema".to_string()), Value::Bytes(version.schema.as_bytes().to_vec())));
    map.push((Value::Text("contract".to_string()), Value::Bytes(version.contract.as_bytes().to_vec())));
    if let Some(reactor) = version.reactor {
        map.push((Value::Text("reactor".to_string()), Value::Bytes(reactor.as_bytes().to_vec())));
    }
    if !version.deps.is_empty() {
        let deps = version.deps.iter().map(|dep| {
            let mut dep_map = Vec::new();
            dep_map.push((Value::Text("name".to_string()), Value::Text(dep.name.clone())));
            if let Some(ver) = dep.ver {
                dep_map.push((Value::Text("ver".to_string()), Value::Integer((ver as i64).into())));
            }
            Value::Map(dep_map)
        }).collect();
        map.push((Value::Text("deps".to_string()), Value::Array(deps)));
    }
    Value::Map(map)
}

fn parse_version_key(key: &str) -> Option<u64> {
    if let Ok(ver) = key.parse::<u64>() {
        return Some(ver);
    }
    let major = key.split('.').next()?;
    major.parse::<u64>().ok()
}

fn ensure_artifacts_present<'a, I>(root: &Path, versions: I) -> Result<(), DharmaError>
where
    I: IntoIterator<Item = &'a PackageVersion>,
{
    let missing = missing_artifacts(root, versions)?;
    if missing.is_empty() {
        Ok(())
    } else {
        Err(DharmaError::Validation(format!(
            "missing {} artifact(s)",
            missing.len()
        )))
    }
}

pub fn missing_artifacts<'a, I>(
    root: &Path,
    versions: I,
) -> Result<Vec<EnvelopeId>, DharmaError>
where
    I: IntoIterator<Item = &'a PackageVersion>,
{
    let store = Store::from_root(root);
    let mut missing = BTreeSet::new();
    for version in versions {
        if !object_exists(&store, &version.schema) {
            missing.insert(version.schema);
        }
        if !object_exists(&store, &version.contract) {
            missing.insert(version.contract);
        }
        if let Some(reactor) = version.reactor {
            if !object_exists(&store, &reactor) {
                missing.insert(reactor);
            }
        }
    }
    Ok(missing.into_iter().collect())
}

fn object_exists(store: &Store, envelope_id: &EnvelopeId) -> bool {
    let path = store.objects_dir().join(format!("{}.obj", envelope_id.to_hex()));
    store.env().exists(&path)
}

fn verify_object(
    root: &Path,
    object_id: &EnvelopeId,
    missing: &mut Vec<EnvelopeId>,
    mismatched: &mut Vec<EnvelopeId>,
) -> Result<(), DharmaError> {
    let store = Store::from_root(root);
    let Some(bytes) = store.get_object_any(object_id)? else {
        missing.push(*object_id);
        return Ok(());
    };
    let computed = crypto::envelope_id(&bytes);
    if &computed != object_id {
        mismatched.push(*object_id);
    }
    Ok(())
}

fn should_filter_key(key: &str, manifest: &PackageManifest) -> bool {
    if key == "schema" || key == "contract" {
        return manifest
            .versions
            .values()
            .any(|v| v.ver == crate::assertion::DEFAULT_DATA_VERSION);
    }
    for ver in manifest.versions.keys() {
        let schema_key = format!("schema_v{ver}");
        let contract_key = format!("contract_v{ver}");
        let reactor_key = format!("reactor_v{ver}");
        if key == schema_key || key == contract_key || key == reactor_key {
            return true;
        }
    }
    false
}

fn remove_config_entries(root: &Path, manifest: &PackageManifest) -> Result<(), DharmaError> {
    let path = root.join("dharma.toml");
    if !path.exists() {
        return Ok(());
    }
    let contents = fs::read_to_string(&path)?;
    let mut lines = Vec::new();
    for line in contents.lines() {
        let trimmed = line.trim();
        let key = trimmed.split('=').next().unwrap_or("").trim();
        if matches_config_entry(trimmed, key, manifest) {
            continue;
        }
        if !trimmed.is_empty() {
            lines.push(line.to_string());
        }
    }
    fs::write(path, lines.join("\n") + "\n")?;
    Ok(())
}

fn matches_config_entry(line: &str, key: &str, manifest: &PackageManifest) -> bool {
    let value = line.split('=').nth(1).map(|v| v.trim().trim_matches('"'));
    for version in manifest.versions.values() {
        let schema_key = format!("schema_v{}", version.ver);
        let contract_key = format!("contract_v{}", version.ver);
        let reactor_key = format!("reactor_v{}", version.ver);
        if key == schema_key && value == Some(version.schema.to_hex().as_str()) {
            return true;
        }
        if key == contract_key && value == Some(version.contract.to_hex().as_str()) {
            return true;
        }
        if let Some(reactor) = version.reactor {
            if key == reactor_key && value == Some(reactor.to_hex().as_str()) {
                return true;
            }
        }
        if version.ver == crate::assertion::DEFAULT_DATA_VERSION {
            if key == "schema" && value == Some(version.schema.to_hex().as_str()) {
                return true;
            }
            if key == "contract" && value == Some(version.contract.to_hex().as_str()) {
                return true;
            }
            if let Some(reactor) = version.reactor {
                if key == "reactor" && value == Some(reactor.to_hex().as_str()) {
                    return true;
                }
            }
        }
    }
    false
}

pub fn ensure_dependencies(
    root: &Path,
    deps: &[PackageDep],
    registry_subject: Option<SubjectId>,
    visited: &mut BTreeSet<String>,
) -> Result<(), DharmaError> {
    ensure_dependencies_with_fetch(root, deps, registry_subject, visited, &mut noop_fetch)
}

fn noop_fetch(_: &[EnvelopeId]) -> Result<(), DharmaError> {
    Ok(())
}

pub fn ensure_dependencies_with_fetch<F>(
    root: &Path,
    deps: &[PackageDep],
    registry_subject: Option<SubjectId>,
    visited: &mut BTreeSet<String>,
    fetch: &mut F,
) -> Result<(), DharmaError>
where
    F: FnMut(&[EnvelopeId]) -> Result<(), DharmaError>,
{
    for dep in deps {
        if visited.contains(&dep.name) {
            continue;
        }
        visited.insert(dep.name.clone());
        let installed = load_manifest(root, &dep.name)?;
        if installed.is_some() {
            continue;
        }
        let registries = find_registry_packages(root, &dep.name, registry_subject)?;
        let Some(registry) = registries.first() else {
            return Err(DharmaError::Validation(format!("missing dependency {}", dep.name)));
        };
        let manifest = install_from_registry_with_fetch(root, registry, dep.ver, fetch)?;
        update_config_for_manifest(root, &manifest)?;
        let mut child_deps = Vec::new();
        for version in manifest.versions.values() {
            child_deps.extend(version.deps.clone());
        }
        ensure_dependencies_with_fetch(root, &child_deps, registry_subject, visited, fetch)?;
    }
    Ok(())
}

pub fn manifest_to_json_value(manifest: &PackageManifest) -> Value {
    manifest_to_value(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, DEFAULT_DATA_VERSION};
    use crate::crypto;
    use crate::types::{ContractId, SchemaId};
    use ciborium::value::Value;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use tempfile::tempdir;

    #[test]
    fn manifest_roundtrip() {
        let temp = tempdir().unwrap();
        let version = PackageVersion {
            ver: 1,
            schema: EnvelopeId::from_bytes([1u8; 32]),
            contract: EnvelopeId::from_bytes([2u8; 32]),
            reactor: None,
            deps: Vec::new(),
        };
        let manifest = PackageManifest {
            name: "test.pkg".to_string(),
            versions: [(1, version)].into_iter().collect(),
            pinned: Some(1),
            registry_subject: None,
            registry_object: None,
            publisher: None,
        };
        save_manifest(temp.path(), &manifest).unwrap();
        let loaded = load_manifest(temp.path(), "test.pkg").unwrap().unwrap();
        assert_eq!(loaded.name, manifest.name);
        assert_eq!(loaded.pinned, Some(1));
    }

    #[test]
    fn parse_registry_package() {
        let mut rng = StdRng::seed_from_u64(2);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([9u8; 32]);
        let schema = EnvelopeId::from_bytes([1u8; 32]);
        let contract = EnvelopeId::from_bytes([2u8; 32]);
        let body = Value::Map(vec![
            (Value::Text("name".to_string()), Value::Text("std.demo".to_string())),
            (
                Value::Text("versions".to_string()),
                Value::Map(vec![(
                    Value::Text("1".to_string()),
                    Value::Map(vec![
                        (Value::Text("schema".to_string()), Value::Bytes(schema.as_bytes().to_vec())),
                        (Value::Text("contract".to_string()), Value::Bytes(contract.as_bytes().to_vec())),
                    ]),
                )]),
            ),
        ]);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "sys.package.add".to_string(),
            auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, body, &signing_key).unwrap();
        let pkg = parse_registry_assertion(&assertion, EnvelopeId::from_bytes([5u8; 32])).unwrap();
        assert_eq!(pkg.name, "std.demo");
        assert!(pkg.versions.contains_key(&1));
    }

    #[test]
    fn missing_artifacts_reports_missing_ids() {
        let temp = tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let schema_bytes = vec![1u8, 2, 3];
        let schema_id = crypto::envelope_id(&schema_bytes);
        store.put_object(&schema_id, &schema_bytes).unwrap();
        let contract_bytes = vec![9u8, 8, 7];
        let contract_id = crypto::envelope_id(&contract_bytes);
        let version = PackageVersion {
            ver: 1,
            schema: schema_id,
            contract: contract_id,
            reactor: None,
            deps: Vec::new(),
        };
        let missing = missing_artifacts(temp.path(), std::iter::once(&version)).unwrap();
        assert_eq!(missing, vec![contract_id]);
    }
}
