# Task: Registry Publisher ACL Checks

## Objective
Enforce registry-scope authorization when consuming `sys.package.*` assertions. A node should trust registry packages only if the publisher key is authorized for the registry scope.

## Scope
- Applies to **registry subjects** that publish package assertions (`sys.package.add`, `sys.package.update`, etc.).
- Enforced in `pkg install` and `pkg verify` flows.

## Requirements
1. **ACL Source**
   - Define a registry ACL assertion type (proposal): `sys.registry.acl`.
   - Fields:
     - `scope`: Text (e.g. `std`, `company`, `personal`, or a namespace prefix).
     - `publisher`: PubKey32 (IdentityKey).
     - `action`: Enum(`allow`, `revoke`).
     - `ts`: Int (optional for future revocation ordering).
   - ACL assertions live under the registry subject.

2. **ACL Resolution**
   - Build an ACL view for a registry subject by scanning its assertions:
     - Process `sys.registry.acl` entries in seq order (or timestamp if defined).
     - `allow` adds a publisher key to the scope.
     - `revoke` removes it.
   - If multiple scopes match, **most specific wins** (exact namespace > prefix > global).

3. **Verification Behavior**
   - `pkg verify <name>` must:
     - Validate registry signature (already present).
     - Validate that `assertion.header.auth` is **authorized for the registry scope**.
     - Report failure if unauthorized.
   - `pkg install <name>` must fail if registry publisher is unauthorized.

4. **Registry Scope Derivation**
   - Scope is derived from the registry subject policy:
     - Default: `std` for public registry.
     - If `dharma.toml` defines `registry_scope`, use it.
     - If `registry_subject` is provided with `--from`, use its scope.

5. **Caching**
   - Cache ACL resolution per registry subject in memory for the process.
   - Invalidate cache if registry subject is updated (best-effort by seq comparison).

## Implementation Notes
- Add parsing for `sys.registry.acl` in `dharma-cli/src/pkg/mod.rs` (or a new registry module).
- Add helper:
  - `fn registry_acl_allows(root, registry_subject, publisher_key) -> Result<bool>`
- Update `find_registry_packages` to return `RegistryPackage` with `authorized: bool` or enforce authorization at install/verify time.
- Keep outputs aligned with user_guide: show `registry_signature` and `publisher_acl` booleans.

## Acceptance Criteria
- `pkg install` fails when publisher is unauthorized.
- `pkg verify` reports unauthorized publisher in JSON + human output.
- Unit tests cover:
  - allow → authorized
  - revoke → unauthorized
  - scope specificity resolution

