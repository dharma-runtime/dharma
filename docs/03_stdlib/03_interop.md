# Interoperability Guide

Interoperability in DHARMA means **two parties can independently verify and replay the same assertions** because they share the same schemas and contracts.

## Rules of Interop

1) **Namespace Stability**
   - Use stable namespaces: `std.task`, `com.acme.invoice`.

2) **Versioned Semantics**
   - Do not mutate old meaning in-place; publish new versions.

3) **Explicit Artifacts**
   - Schema/contract artifacts must be content-addressed and available.

4) **No Silent Extensions**
   - Avoid adding fields without version bumps.

## Current Limitations

- No registry or signed package directory yet.
- Artifact discovery is manual (`dh compile` or manual object import).

## Recommended Practice

- Pin schema/contract IDs in `dharma.toml`.
- Share the DHL source alongside artifacts for auditability.

