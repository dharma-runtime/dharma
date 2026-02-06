# Task 66.3: ECommerce Projection + Query Tests

## Goal
Provide deterministic tests that validate projection correctness and key DHARMA‑Q queries.

## Why
Commerce queries rely on projection tables. Without tests, regressions will silently corrupt analytics and ops.

## Scope
- Add fixtures for commerce flows (catalog → order → fulfillment → invoicing → credit).
- Add tests that validate projection tables + query outputs.

## Out of Scope
- Performance/benchmarking.

## Specification

### 1) Fixtures
- Build a deterministic fixture with:
  - 1 category, 2 products, 3 variants
  - 2 sellables with differing attributes
  - 1 PO with 2 lines
  - 1 batch attachment
  - 1 fulfillment
  - 1 invoice + 1 credit note

### 2) Projection validation
- Verify these projections:
  - `product_facet` counts
  - `variant_availability` qty + status
  - `line_stats` split/substitution counts
  - `po_action_queue` populated for expected states
  - `batch_line` and `batch_route` entries
  - `invoice_line` and `credit_note_line` entries

### 3) DHARMA‑Q query validation
- Execute key queries and assert expected rows:
  - `GetProductFacets`
  - `GetVariantAvailabilityHint`
  - `LinesNeedingAllocation`
  - `ListMyInvoices`
  - `ReturnsAndCreditsSummary`

## Test Plan
- Unit tests for individual projection builders.
- Integration tests in `dharma-test`:
  - run fixture pipeline
  - run queries
  - assert result sets

## Acceptance Criteria
- Tests run deterministically in CI.
- Projections and queries produce correct output for the fixture.
