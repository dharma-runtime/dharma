# Task 66.1: ECommerce Projection Writers + Pipelines

## Goal
Populate all commerce projection tables referenced by DHARMA‑Q queries.

## Why
Queries such as `GetProductFacets`, `VariantAvailability`, `BatchLine`, and invoice/credit line analytics depend on derived tables. Without projection writers the system is unusable for real workflows.

## Scope
- Build projection writers (reactors or background jobs) that emit `Upsert/Clear` actions into:
  - `std.commerce.catalog.product_facet`
  - `std.commerce.catalog.variant_availability`
  - `std.commerce.order.line_stats`
  - `std.commerce.order.po_action_queue`
  - `std.commerce.logistics.batch_line`
  - `std.commerce.logistics.batch_route`
  - `std.commerce.invoice_line`
  - `std.commerce.credit_note_line`
- Add rebuild/backfill capability (full reindex) and incremental updates.
- Enforce `projection.writer` role for all writes.

## Out of Scope
- UI/reporting.
- Billing enforcement.

## Specification

### 1) Projection writer architecture
- Choose one of:
  - Reactor pipeline (event‑driven, deterministic), or
  - Background projector (incremental scan + idempotent writes).
- Must support:
  - full rebuild
  - incremental update on new events
  - idempotent Upsert

### 2) Triggers per projection
- **ProductFacet**: rebuild from all published products; update on product create/update/publish/archive.
- **VariantAvailability**: update on sellable receive/adjust/allocate/release; grouped by (variant_id, delivery_area?).
- **LineStats**: update on `SplitLine`, substitutions, fulfillment events.
- **POActionQueue**: update on PO status changes, line remaining qty changes, payment state, invoice state.
- **BatchLine**: update on `AttachLineToBatch` and batch state/schedule changes.
- **BatchRoute**: update on route assignments.
- **InvoiceLine**: update on invoice issue/post.
- **CreditNoteLine**: update on credit note issue/post.

### 3) Rebuild tooling
- Add a deterministic rebuild command:
  - `dh project rebuild --scope std.commerce.*` (or a scoped flag).
- Must wipe/rebuild projections atomically or in a consistent way.

## Test Plan
- Unit tests for projector logic (input events -> expected projection state).
- Integration tests that:
  1) create base entities,
  2) emit events,
  3) verify projection tables via DHARMA‑Q queries.

## Acceptance Criteria
- All listed projection tables are populated for the commerce flow.
- Full rebuild works and is deterministic.
- Incremental updates keep projections consistent.
