# Task 66.2: ECommerce Core Entity Contracts (Supplier + Warehouse)

## Goal
Define the canonical contracts for `std.commerce.inventory.supplier` and `std.commerce.logistics.warehouse`, or map them explicitly to existing `std.biz.*` entities.

## Why
`std.commerce.inventory.sellable` references supplier and warehouse. Without these contracts the system is incomplete and query semantics are ambiguous.

## Scope
- Decide one of:
  1) Create new `std.commerce.inventory.supplier` and `std.commerce.logistics.warehouse` contracts, or
  2) Map to `std.biz.suppliers` / `std.biz.logistics` with explicit aliasing and update all references.
- Update DHL contracts to align with the decision.
- Provide documentation and examples.

## Out of Scope
- Vendor portal UI.
- Advanced procurement logic.

## Specification

### Option A: Commerce‑native contracts
- `std.commerce.inventory.supplier`
  - minimal fields: name, status, contact, external_id, human_ref
- `std.commerce.logistics.warehouse`
  - fields: name, status, address_snapshot, timezone, external_id, human_ref

### Option B: Map to std.biz
- Define a shared interface or alias pattern.
- Update `Ref<>` fields in sellable + queries to point to `std.biz.*`.

### Required outputs
- New DHL contracts (if Option A), or
- Documented mapping + updated references (if Option B).

## Test Plan
- Contract compile tests.
- Creation + reference in sellable flows.
- DHARMA‑Q queries that join supplier/warehouse data.

## Acceptance Criteria
- There is a single canonical supplier and warehouse definition.
- All references compile and resolve correctly.
- Queries using supplier/warehouse fields function end‑to‑end.
