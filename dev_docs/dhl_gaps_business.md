# DHL Gaps for Business & Legal Rules (Current Compiler)

This document summarizes *what real-world business rules, legal constraints, and regulatory requirements cannot be expressed today* in DHL, based on the current compiler/runtime behavior.

Scope: **current compiler** as implemented in `dharma-cli/src/pdl/*` and runtime codegen in `dharma-cli/src/pdl/codegen/*`. The broader design in `dev_docs/language.md` is intentionally *richer* than what is implemented today; this doc focuses on the **actual enforced subset**.

---

## 0) Quick Snapshot: What DHL Can Enforce Today

- Single aggregate with a flat `state` schema (no nested objects).
- Actions with `validate` expressions and `apply` assignments.
- Limited expression language: basic arithmetic, comparisons, booleans, `len`, `contains`, `in`, `index/get`, `sum`, `distance`, `now`, `has_role`.
- List push/remove and map set in `apply` only.
- Optional types are supported for storage, but *expression support for null/presence is limited*.
- Flows desugar to `state.status` transitions only.
- Reactors compile but are not executed in the runtime.
- External `roles/time` are declared; there is **no executable DHARMA‑Q query support** for external contracts in validation today (even though `external.datasets` exists as a declared-only mechanism).

---

## 1) Core Language Gaps (Why Business Rules Break)

### 1.1 Data Modeling Gaps
- **No nested objects/records** in `state` or `args`.
  - You cannot model line items with per-item fields (qty, price, tax, SKU) in a structured way.
- **No nested path access** (`state.lines[0].price` is not supported by the type checker).
- **No cross-subject joins** or foreign key validation for `Ref<T>`.
  - `Ref<T>` exists as a type but is not enforced by the runtime.
- **Fixed state size limits** (public/private blocks capped at 0x1000 bytes).
  - Large contracts, long lists, or large text fields are not representable.

### 1.2 Expression & Validation Gaps
- **No string functions** (regex, contains/startsWith, normalization, case handling, checksum).
- **No date/time arithmetic** (only `now()` returns a timestamp; no calendars/holidays/time zones).
- **No decimals with rounding modes** (financial math needs rounding rules, scale policies, and precision guarantees).
- **Limited optional/null logic** (null comparisons are rejected in codegen for most types).
- **No quantifiers** (`all`, `any`, `filter`, `map`, `reduce` over collections).
- **No conditionals** (`if/else`) inside `apply` for conditional mutation.

### 1.3 Process / Workflow Gaps
- **Flows only target `state.status`** (single state machine, single field).
- **No explicit multi-step approvals** (quorum/threshold signing not expressible in a single action).
- **No long-running timeouts** (reactors are not executed by runtime today).
- **No explicit event types** (actions are the only events; you can't emit side-effect events with schemas).

### 1.4 External Data / Oracles
- `external.datasets` is *declared* but not executable today.
- There is **no in-contract DHARMA‑Q query execution** over external contracts (FX rates, tax tables, sanctions lists, shipping carrier APIs).
- The proposed model is **“datasets as contracts” + DHARMA‑Q queries**, but determinism, snapshot binding, and provenance capture are not implemented in the runtime yet.
  - **Direction:** deprecate dataset primitives; treat every dataset as a first-class contract and query it via DHARMA‑Q with an auditable query hash + snapshot semantics.

### 1.5 Access Control & Segregation-of-Duties
- Only a single signer is available in context (`context.signer`).
- Multi-party approvals, countersignature requirements, or separation-of-duties (SoD) constraints can’t be enforced in a single action.
- Role checks rely on `has_role` but are limited by the available context and by role data availability.

---

## 2) Business Use Cases Not Expressible Today (Examples)

### Commerce & Invoicing
- **Line-item invoicing**: `sum(line.qty * line.price) == total`.
  - Requires nested objects, numeric transforms, and aggregates.
- **Tax logic**: VAT/GST rules, exemptions, thresholds, locale-specific tax rates.
  - Requires external contracts queried via DHARMA‑Q + conditional logic + date-sensitive rules.
- **Discount schedules**: tiered pricing, promotions, BOGO logic, coupons.
  - Requires conditional logic + collection operations.
- **Returns policies**: windowed eligibility, restocking fees, condition-based rules.
  - Requires time arithmetic + rule branching.

### Procurement & Finance
- **Approval thresholds**: “≥ $25k requires two approvers; ≥ $100k requires CFO.”
  - Requires multi-signer/quorum and conditional branching.
- **Spend controls**: monthly budgets, cumulative spend by cost center.
  - Requires historical aggregation, sums over many records, and time windows.
- **FX exposure rules**: validate invoices against live FX rates.
  - Requires external contracts queried via DHARMA‑Q and time-based locking.

### Logistics & Supply Chain
- **Hazmat compliance** (storage, routing, labeling constraints).
  - Requires detailed item classification and regulatory rule checks.
- **Incoterms / customs compliance** (exports, embargo lists, HS codes).
  - Requires external contracts queried via DHARMA‑Q, geolocation, and jurisdiction rules.
- **Carrier SLA enforcement** (late penalties, exception handling).
  - Requires time windows and conditional penalties.

### Healthcare & Privacy
- **HIPAA-style consent**: “this data may be used for treatment but not marketing.”
  - Requires policy-driven access control beyond `has_role`.
- **Break-glass access** with auditable reason codes and time-bound access.
  - Requires conditional rules, explicit reason enums, and strong policy enforcement.
- **Data retention** rules (e.g., delete after 7 years unless legal hold).
  - Requires time-aware lifecycle rules and purge semantics.

### HR / Employment Law
- **Overtime rules** (jurisdiction-specific thresholds and weekly caps).
  - Requires time windows, calendaring, and accumulation across records.
- **Scheduling constraints** (mandatory breaks, rest periods, union rules).
  - Requires cross-entity aggregation and calendar logic.

### Governance & Compliance
- **Board/committee voting** with quorum, weighted votes, or conflict-of-interest exclusion.
  - Requires multi-signer context, membership checks, and quorum counting.
- **Regulated audit trails** requiring immutable, signed evidence (documents, attestations).
  - Requires attachment hashing, structured evidence verification, and multi-party attestations.

---

## 3) Legal/Regulatory Constraints Not Expressible Today

**Examples (non-exhaustive):**
- **Consumer protection laws** requiring cooling-off windows or refund rights.
- **Export control / sanctions** rules (OFAC, EU sanctions lists, embargo restrictions).
- **Tax compliance** (VAT OSS, sales tax nexus, digital services tax).
- **Financial regulation** (AML/KYC risk scoring, transaction monitoring thresholds).
- **Privacy laws** (GDPR lawful basis, data minimization, right-to-erasure workflows).
- **Industry rules** (FDA 21 CFR Part 11, SOX audit requirements).

These all depend on combinations of:
- External data sources,
- Rich string & date/time functions,
- Conditional logic,
- Multi-party approvals,
- Cross-subject aggregation.

---

## 4) Prioritized DSL/RUNTIME Features to Unlock These Rules

### P0 — Expression & Data Model Parity (Foundational)
1. **Nested structs/records** in state + nested path access.
2. **Optional/presence checks** (explicit `is_null`, `exists`, or `has(field)`).
3. **String ops** (contains, regex, normalize, lowercase/uppercase, checksum/validation).
4. **Date/time arithmetic** (add/sub duration, compare with time windows, timezone handling).
5. **Decimal arithmetic** with explicit rounding modes and scale.
6. **Collection operators** (`any`, `all`, `filter`, `map`, `sum_by`, `count_if`).

### P1 — Cross-Subject & External Data
1. **Ref integrity**: enforce existence of referenced subjects and optional state constraints.
2. **DHARMA‑Q queries over external contracts** with deterministic snapshots and query hashing.
3. **Subject joins** in validations (read-only, deterministic).

### P1 — Process / Authorization
1. **Multi-signer validation** (quorum, weighted votes, SoD).
2. **Explicit multi-step workflows** with named states, guards, and sequencing.
3. **Event emission** with dedicated event schemas.

### P2 — Lifecycle & Governance
1. **Contract migrations** with explicit transformations (state evolution).
2. **Time-based automation** (reactors + scheduled triggers).
3. **Formal policy blocks** for access control (beyond `has_role`).

---

## 5) Implementation Touchpoints (Where Each Gap Lands)

- **Parser / AST**: `dharma-cli/src/pdl/parser.rs`, `dharma-cli/src/pdl/ast.rs`
  - Nested records, new expressions, flow syntax, policy blocks.
- **Type checking**: `dharma-cli/src/pdl/typecheck.rs`
  - Optional presence logic, type coercions, rich function typing.
- **Codegen**: `dharma-cli/src/pdl/codegen/wasm.rs`
  - Expression compilation, collection ops, string ops, date/time ops, optional checks.
- **Schema model**: `dharma-core/src/pdl/schema.rs`
  - Extend schema for record/struct and richer type metadata.
- **Runtime**: `dharma-core/src/runtime/vm.rs`
  - Host functions for DHARMA‑Q query execution, calendar, policy checks, quorum validation.

---

## 6) Near-Term Workarounds (Until DSL Expands)

- **Pre-compute** complex logic off-chain, store results as action args, and validate only simple invariants in DHL.
- **Use enums** for categorical validation in place of string parsing.
- **Flatten schemas** (encode structured objects into multiple primitive fields).
- **External validation layer** in the client/REPL to enforce domain rules before commit.
- **Manual multi-approval**: encode approvals as separate actions and validate sequencing.

---

## 7) Open Questions

- Do we need a **policy language** distinct from DHL expressions for access control?
- Should cross-subject reads be **explicit and versioned** (e.g., `ref.state.status`) to preserve determinism?
- How should DHARMA‑Q queries be **snapshotted, hashed, and bound** to validation to avoid nondeterminism?
- What is the acceptable boundary between **DHL** and **host-provided builtins**?

---

## 8) Summary

DHL today is strong as a **schema + simple validation** language, but it cannot yet express many of the rules that real businesses and legal regimes require. The biggest blockers are:

1. **No nested data / collection iteration** (blocks almost all real invoices, orders, claims).
2. **No robust string/time/decimal semantics** (blocks taxes, compliance, and legal rules).
3. **No cross-subject validation or external data feeds** (blocks regulated or real-world cross-entity enforcement).
4. **No multi-party approvals or explicit workflow semantics** (blocks governance and procurement rules).

Closing these gaps will determine whether DHL evolves from a solid IDL into a full **domain law language** for business and regulatory logic.

---

## 9) Proposed DHL Example (Full-Feature Syntax)

Below is a **single DHL file** showing a proposed syntax that would cover the gaps above. This is *illustrative* only; it is **not** valid in the current compiler.

````markdown
---
namespace: com.acme.procurement
version: 2.0.0
import:
  - std.iam.v1.dhl
  - std.finance.v1.dhl
concurrency: strict
---

# Procurement + Invoicing (Proposed DHL Syntax)

```dhl
package com.acme.procurement

external
    roles: [finance.approver, finance.cfo, procurement.buyer, compliance.officer]
    time: [block_time]
    queries: [sanctions.ofac, fx_rates, tax_table.us, holidays.us, promo.rules]

aggregate PurchaseOrder
    state
        public buyer: Identity
        public vendor: Ref<Vendor>
        public currency: Currency = "USD"
        public status: Enum(Draft, Submitted, Approved, Rejected, Ordered, Received, Invoiced, Paid, Cancelled) = 'Draft
        public created_at: Timestamp = now()
        public due_date: Timestamp?
        public total: Decimal(scale=2)
        public tax_total: Decimal(scale=2)
        public discount_total: Decimal(scale=2)
        public approvals: List<Approval>
        public lines: List<LineItem>
        public notes: Text(len=512)?

    record Approval
        signer: Identity
        role: Text(len=64)
        at: Timestamp

    record PaymentInstruction
        amount: Decimal(scale=2)
        currency: Currency
        method: Enum(Wire, Card, Check, ACH)
        reference: Text(len=128)?

    record LineItem
        sku: Text(len=64)
        description: Text(len=256)?
        qty: Int
        unit_price: Decimal(scale=2)
        tax_code: Text(len=32)
        tax_rate: Decimal(scale=4)
        total: Decimal(scale=2)

    invariant
        total >= 0
        tax_total >= 0
        discount_total >= 0
        all(lines, line => line.qty > 0)
        all(lines, line => line.unit_price >= 0)
        sum_by(lines, line => line.total) == total

flow Lifecycle
    'Draft -> [Submit] -> 'Submitted
    'Submitted -> [Approve] -> 'Approved
    'Submitted -> [Reject] -> 'Rejected
    'Approved -> [Order] -> 'Ordered
    'Ordered -> [Receive] -> 'Received
    'Received -> [Invoice] -> 'Invoiced
    'Invoiced -> [Pay] -> 'Paid
    'Draft -> [Cancel] -> 'Cancelled
    'Submitted -> [Cancel] -> 'Cancelled
    'Approved -> [Cancel] -> 'Cancelled

action Create(buyer: Identity, vendor: Ref<Vendor>, currency: Currency)
    validate
        has_role(context.signer, "procurement.buyer")
        buyer == context.signer
        ref_exists(vendor)
        ref_state(vendor, "status") == 'Active
    apply
        state.buyer = buyer
        state.vendor = vendor
        state.currency = currency
        state.created_at = now()
        state.status = 'Draft

action AddLine(line: LineItem)
    validate
        state.status == 'Draft
        line.qty > 0
        line.unit_price >= 0
        regex(line.sku, "^[A-Z0-9_-]{3,32}$")
        let sanctioned = query("
            sanctions.ofac
            | where tax_id == $vendor_tax_id
            | where status == 'Active
        ", { vendor_tax_id: ref_field(state.vendor, "tax_id") })
        len(sanctioned) == 0
    apply
        state.lines.push(line)
        state.total = sum_by(state.lines, l => l.qty * l.unit_price)

action ApplyDiscount(code: Text)
    validate
        state.status == 'Draft
        regex(code, "^[A-Z0-9]{6,10}$")
        let promo = query("
            promo.rules
            | where code == $code
            | where status == 'Active
            | order_by effective_at desc
            | take 1
        ", { code: code })
        present(promo[0])
    apply
        let d = promo[0].discount_amount
        state.discount_total = d
        state.total = state.total - d

action Submit()
    validate
        state.status == 'Draft
        len(state.lines) > 0
        state.total >= 0
    apply
        state.status = 'Submitted

action Approve()
    validate
        state.status == 'Submitted
        has_role(context.signer, "finance.approver")
        context.signer != state.buyer
        not exists(state.approvals, a => a.signer == context.signer)
        if state.total >= 100000.00 then has_role(context.signer, "finance.cfo")
        quorum(state.approvals, min=2)
    apply
        state.approvals.push({ signer: context.signer, role: "finance.approver", at: now() })
        if quorum(state.approvals, min=2) then state.status = 'Approved

action Reject(reason: Text)
    validate
        state.status == 'Submitted
        len(reason) >= 8
    apply
        state.status = 'Rejected
        state.notes = reason

action Invoice(invoice_id: Ref<Invoice>)
    validate
        state.status == 'Received
        ref_exists(invoice_id)
        ref_state(invoice_id, "status") == 'Issued
        ref_state(invoice_id, "total") == state.total
    apply
        state.status = 'Invoiced

action Pay(payment: PaymentInstruction)
    validate
        state.status == 'Invoiced
        payment.amount == state.total
        let fx = query("
            fx_rates
            | where from_currency == $from
            | where to_currency == $to
            | where effective_at <= now()
            | order_by effective_at desc
            | take 1
        ", { from: state.currency, to: payment.currency })
        present(fx[0])
        fx[0].rate > 0
        present(state.due_date)
        let holidays = query("
            holidays.us
            | where date >= $from
            | where date <= $to
            | where type in ['Federal, 'Bank]
        ", { from: now(), to: state.due_date })
        within_business_days(now(), state.due_date, holidays)
    apply
        state.status = 'Paid

reactor AutoLateFee
    trigger cron("0 0 * * *")
    validate
        state.status == 'Invoiced
        now() > state.due_date
    emit action.ApplyFee(amount = calculate_late_fee(state.total, now(), state.due_date))

view PurchaseOrderDetail
    # UI hints omitted
```
````

### Notes on Proposed Constructs

- **record**: nested typed objects inside an aggregate.
- **ref_exists / ref_state / ref_field**: cross-subject referential integrity and state read.
- **all / any / exists / sum_by**: collection quantifiers and aggregation (lambda syntax: `x => expr`).
- **regex / len / within_business_days**: richer string/time semantics.
- **quorum**: multi-signer approval checks.
- **query(...)**: DHARMA‑Q queries over external contracts (declared in `external.queries`).
- **query bindings**: query results are bound during `validate` and are reusable in `apply` with an attached query hash for auditability.
- **external.queries**: allowlist of contract namespaces permitted in validation.
- **cron(...)**: time-based reactor triggers.
- **if/then** in `validate` and `apply` blocks.
- **present(...)**: optional presence check for nullable fields.
- **let**: local binding in `apply` blocks (shown in `ApplyDiscount`).

This example intentionally spans the hardest gaps: nested data, cross-subject checks, contract queries (via DHARMA‑Q), approvals, time windows, and conditional state transitions.

---

## 10) Decision Tables (Proposed Syntax)

Decision tables are an optional authoring layer that compiles into ordinary DHL expressions. They help when rules are numerous, combinatorial, or reviewed by non-dev stakeholders.

### 10.1 Minimal Syntax

```dhl
decision ApprovePolicy
    inputs
        total: Decimal(scale=2)
        signer: Identity
        buyer: Identity
        approvals: List<Approval>
    table
        | rule | when                                                                 | then                 |
        | R1   | total < 25000 and has_role(signer, "finance.approver") and signer != buyer | allow               |
        | R2   | total >= 25000 and total < 100000 and has_role(signer, "finance.approver") and signer != buyer and quorum(approvals, min=2) | allow |
        | R3   | total >= 100000 and has_role(signer, "finance.cfo") and signer != buyer and quorum(approvals, min=2) | allow |
        | R4   | else                                                                   | deny("approval policy failed") |

action Approve()
    validate
        decision ApprovePolicy(
            total = state.total,
            signer = context.signer,
            buyer = state.buyer,
            approvals = state.approvals
        )
```

### 10.2 Desugaring (Conceptual)

The table compiles to a deterministic expression tree (first-match wins):

```dhl
validate
    (
        (total < 25000 and has_role(signer, "finance.approver") and signer != buyer)
        or
        (total >= 25000 and total < 100000 and has_role(signer, "finance.approver") and signer != buyer and quorum(approvals, min=2))
        or
        (total >= 100000 and has_role(signer, "finance.cfo") and signer != buyer and quorum(approvals, min=2))
    )
```

If a `deny("reason")` row fires, the runtime returns a validation error that includes the rule id and reason.

### 10.3 Interaction With DHARMA-Q Queries

Cells can call `query(...)` like any other DHL expression:

```dhl
| R5 | len(query("sanctions.ofac | where tax_id == $tax_id | where status == 'Active", { tax_id: vendor_tax_id })) == 0 | allow |
```

If a rule uses `query(...)`, the validation result should bind the query hash + snapshot for auditability.

### 10.4 Why This Is Not A New Primitive

Decision tables are a syntax convenience that compiles to existing DHL expressions. The runtime need only support:
- expression evaluation,
- optional rule-id metadata on failure,
- query binding (if used).
