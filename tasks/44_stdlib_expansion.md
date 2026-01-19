# Task 44: Standard Library Expansion

## Goal
Implement the comprehensive DHARMA Standard Library (`std.*`).
These contracts provide the "batteries included" data models for the Operating System of Business.

## The Library Map

### 1. Identity & Infrastructure
- [x] `std.iam`: Identity profiles, delegation, keys.
- [x] `std.atlas`: Namespace registry, URL routing.

### 2. Communication (`std.io`)
- [x] `std.io.mail`: Outbound email requests, inbound receipts.
- [x] `std.io.chat`: Rooms, membership, message log.
- [x] `std.io.contacts`: Personal CRM, vCard-style data.

### 3. Workflow (`std.wrk`)
- [x] `std.wrk.task`: Status workflow, assignment, priority.
- [x] `std.wrk.notes`: Markdown content, tagging, linking.

### 4. Commerce (`std.biz`)
- [x] `std.biz.inventory`: SKU, quantity, location.
- [x] `std.biz.orders`: Order lifecycle (Draft -> Paid -> Fulfilled).
- [x] `std.biz.suppliers`: Vendor relationships.
- [x] `std.biz.logistics`: Shipments, carriers, tracking.

### 5. Finance (`std.fin`)
- [x] `std.fin.ledger`: Double-entry accounting, accounts, transactions.

### 6. Web (`std.web`)
- [x] `std.web.page`: CMS pages, slugs, publishing workflow.
- [x] `std.web.com.product`: Product catalog, pricing, variants.
- [x] `std.web.com.cart`: Shopping cart state.

## Implementation Guide
-   Use **DHL**.
-   Place files in `contracts/std/`.
-   Use **Aspects** (Task 43) where appropriate (e.g. `mixin Versioned`).
-   Write comprehensive Doc Tests (Task 40) for each contract.

## Success Criteria
-   `dh compile contracts/std/` succeeds for all files.
-   The "Quickstart" tutorials can rely on these existing.
