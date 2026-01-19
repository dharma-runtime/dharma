# Task: README Storage Layout + Object Store

## Objective
Adopt the README storage model: a global append-only object store plus derived subject views.

## Requirements
- Directory layout:
  - data/objects/<object_id>.obj (raw envelope bytes for assertions and artifacts)
  - data/subjects/<subject_id>/assertions (derived, for base CQRS actions)
  - data/subjects/<subject_id>/overlays (derived, for overlay actions)
  - data/subjects/<subject_id>/snapshots (derived snapshots)
  - data/subjects/<subject_id>/indexes (derived, disposable)
- Object store is the single source of truth.
- Subject views are derived and can be rebuilt from object store.
- Identity files (identity.key, identity.dharma, dharma.toml) remain at data root.

## Implementation Details
- Add object-store write path for any replicated object (assertions, artifacts, schemas, contracts).
- Update Store API:
  - put_object(object_id, bytes) -> data/objects/<id>.obj
  - get_object(object_id) -> bytes
- Update subject-specific helpers to rebuild from object store rather than write directly.
- Update scan/list logic to read subject views from data/subjects.
- Ensure CQRS action logs are derived from object store when needed (index rebuild).

## Acceptance Criteria
- All new writes go to data/objects first.
- Subject assertions/overlays/snapshots are rebuildable from objects.
- Sync/ingest resolves objects by object id without scanning per-subject.
