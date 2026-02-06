# Task 65.4: IAM Protocol Interface + Resolver

## Goal
Define a minimal IAM protocol interface and align runtime enforcement with it.

## Dependencies
- Task 65.1 (protocol registry)
- Task 65.2 (implements validation)

## Scope
- Define `std.protocol.iam@1` interface:
  - Required fields: `display_name`, `email`, `phone`, `handle`, `keys`, `profile`, `delegates` (or minimal subset)
  - Required actions: `UpdateDisplayName`, `UpdateEmail`, `UpdatePhone`, `UpdateProfile`, `Delegate`, `RevokeDelegate`
  - Privacy: `display_name/email/phone` treated as private for contact gating.
- Update `std.iam.dhl` to declare `implements: std.protocol.iam@1`.
- Provide resolver helpers for IAM profile + privacy field list.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/protocols/iam.rs`
  - Interface definition + privacy list helpers.
- `contracts/std/iam.dhl`
  - Add `implements` frontmatter.
- `dharma-core/src/fabric/router.rs` (or IAM read path)
  - Use IAM privacy list for field-level redaction.

## Test Plan (Detailed)
### Unit Tests
- `iam_interface_compatibility_ok`
- `iam_private_field_list_contains_display_name_email_phone`

### Integration Tests
- IAM visibility gating uses protocol privacy fields (contacts accepted => full view, others redacted).

