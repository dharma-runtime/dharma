# Task 65: Protocol Interfaces & Implementations

## Goal
Introduce a **protocol interface system** for core network contracts (contacts, IAM, Atlas identity, Atlas domain) so the runtime can rely on stable, minimal semantics while allowing extended implementations (e.g. `std.io.contacts`) to add fields/actions without breaking compatibility.

## Why
- Avoid hardcoding a specific contract as the only source of truth.
- Ensure runtime enforcement (visibility, membership, lifecycle) is stable and deterministic.
- Allow richer contracts to extend the base protocol while still passing validation.

## Principles
- A **protocol interface** defines the **minimum required fields, actions, and semantics**.
- A contract can declare `implements: <protocol>` and may extend it with extra fields/actions.
- The **compiler enforces** that implementations satisfy interface requirements.
- The **runtime uses interface semantics**, not the concrete contract implementation.

## Scope
- Define protocol interfaces for:
  - `std.protocol.contacts@1`
  - `std.protocol.iam@1`
  - `std.protocol.atlas.identity@1`
  - `std.protocol.atlas.domain@1`
- Support `implements` in DHL and enforce at compile time.
- Expose protocol metadata in contract artifacts for runtime inspection.
- Provide runtime resolvers for the protocols (deterministic folds).

## Dependencies
- Task 11 (Compiler + Runtime)
- Task 57 (IAM contact-gated visibility)
- Task 58/59 (Atlas identity + domain)

## Deliverables
1. Protocol interface registry (core).
2. DHL `implements` metadata + compiler validation.
3. Runtime resolvers (contacts/iam/atlas/atlas domain).
4. Contract updates to declare implementations.
5. Tests (unit + integration + SATS coverage).

