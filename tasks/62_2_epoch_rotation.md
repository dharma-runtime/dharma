# Task 62.2: Key Rotation + Epoch Usage

## Goal
Add key rotation and ensure new facts use the latest epoch without rewriting old data.

## Dependencies
- Task 62.1 (key primitives)

## Scope
- `domain.key.rotate` handling.
- Subject epoch selection for new facts.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/net/ingest.rs`
  - Handle `domain.key.rotate` assertions.
- `dharma-core/src/store.rs`
  - Track latest epoch per domain/subject.
- `dharma-core/src/envelope.rs`
  - Stamp new facts with current epoch.

## Test Plan (Detailed)
### Unit Tests
- `rotation_does_not_rewrite_data`:
  - Old objects remain unchanged after rotate.
- `new_epoch_used_for_new_facts`:
  - New assertions use new epoch id.

