# DHL Compiler Prover Specification

## Automatic Totality Verification Without Annotations

**Version:** 1.0  
**Status:** Draft  
**Target:** DHL Compiler v2.0+

---

## 1. Overview

### 1.1 Purpose

The DHL Compiler Prover automatically verifies that all DHL code is **total**—meaning every possible execution path is handled, no runtime panics can occur, and all business rules are exhaustively specified. Unlike traditional type systems or opt-in annotations, the prover operates **by default** on all code.

**Core Principle:**
> *"If it compiles, it is provably correct."*

### 1.2 Design Philosophy

| Traditional Approach | DHL Prover Approach |
|---------------------|---------------------|
| Optional `#[must_use]`, `#[non_exhaustive]` | All effects must be used, all patterns must be exhaustive |
| `unwrap()` allowed, `?` operator optional | `unwrap()` forbidden, `?` or explicit match required |
| Panics possible at runtime | Panics impossible by construction |
| Business logic gaps caught in testing | Business logic gaps caught at compile time |
| Annotation burden on developer | Zero annotation burden |

### 1.3 Key Properties Verified

1. **Exhaustiveness:** All pattern match arms cover all possible inputs
2. **Totality:** All functions return valid outputs for all valid inputs
3. **Reachability:** No dead code; all states reachable in state machines
4. **Consistency:** No contradictory rules in decision tables
5. **Effect Safety:** All side effects (IO, errors) are explicitly handled
6. **Arithmetic Safety:** No division by zero, no overflow
7. **Bounds Safety:** All array accesses within bounds or explicitly checked

---

## 2. Proof Obligations by Construct

### 2.1 Pattern Matching (`match`)

#### 2.1.1 Exhaustiveness Requirement

Every `match` expression must handle all possible variants of the matched type.

**Syntax:**
```dhl
match expression {
    pattern1 => expression1,
    pattern2 => expression2,
    // ... must cover all cases
}
```

**Proof Obligation:**
```
For type T with variants {V₁, V₂, ..., Vₙ},
match arms must cover {V₁, V₂, ..., Vₙ}.
```

**Example - ERROR (Missing Cases):**
```dhl
enum PaymentStatus {
    Pending,
    Authorized,
    Declined,
    Expired,
}

// COMPILE ERROR: Missing `Expired`
match payment.status {
    Pending     => queue_for_processing(),
    Authorized  => capture_funds(),
    Declined    => notify_customer(),
}
```

**Compiler Output:**
```
error[E0001]: non-exhaustive patterns: `Expired` not covered
  --> payments.dhl:42:3
   |
42 |   match payment.status {
   |   ^^^^^ pattern `Expired` not covered
   |
   = help: add a match arm for `Expired` or use an explicit wildcard `_`
   = note: financial compliance requires all payment statuses to be handled
```

**Example - VALID (Explicit Wildcard):**
```dhl
match payment.status {
    Authorized  => capture_funds(),
    Declined    => notify_customer(),
    _           => queue_for_retry(),  // Handles Pending + Expired
}
```

**Example - VALID (Complete Coverage):**
```dhl
match payment.status {
    Pending     => queue_for_processing(),
    Authorized  => capture_funds(),
    Declined    => notify_customer(),
    Expired     => void_transaction(),
}
```

#### 2.1.2 Guard Mutual Exclusivity

When patterns have guards (`if` conditions), the prover checks for overlap.

**Example - ERROR (Overlapping Guards):**
```dhl
match order.value {
    v if v > 1000 => high_value_handler(),
    v if v > 500  => medium_value_handler(),  // Overlaps with first!
    v             => standard_handler(),
}
```

**Compiler Output:**
```
error[E0002]: overlapping match guards
  --> orders.dhl:23:5
   |
22 |     v if v > 1000 => high_value_handler(),
   |     ------------- this arm matches values > 1000
23 |     v if v > 500  => medium_value_handler(),
   |     ^^^^^^^^^^^^^ this arm also matches values > 1000
   |
   = help: reorder arms from most specific to least specific
   = note: overlapping guards make business logic ambiguous
```

#### 2.1.3 Nested Pattern Completeness

Deep destructuring must also be complete:

```dhl
enum Order {
    Standard { items: Vec<Item> },
    Custom { items: Vec<Item>, config: Config },
    Subscription { items: Vec<Item>, interval: Interval },
}

// COMPILE ERROR: Missing `Subscription` and `items` empty case
match order {
    Standard { items } if items.len() > 0 => process_standard(items),
    Custom { items, config } => process_custom(items, config),
}
```

**Required Fix:**
```dhl
match order {
    Standard { items: [] } => reject_empty_order(),
    Standard { items } => process_standard(items),
    Custom { items, config } => process_custom(items, config),
    Subscription { items, interval } => process_subscription(items, interval),
}
```

### 2.2 Conditional Expressions (`if` / `else if` / `else`)

#### 2.2.1 Else Requirement

All `if` chains must terminate in an `else` branch.

**Example - ERROR (Missing Else):**
```dhl
if order.value > 1000 {
    apply_discount(20%)
} else if order.value > 500 {
    apply_discount(10%)
}
// ERROR: What happens when value <= 500?
```

**Compiler Output:**
```
error[E0003]: missing else branch
  --> orders.dhl:15:1
   |
15 | / if order.value > 1000 {
16 | |     apply_discount(20%)
17 | | } else if order.value > 500 {
18 | |     apply_discount(10%)
19 | | }
   | |_^ expected else branch here
   |
   = help: add else branch or use match expression for exhaustive logic
```

**Example - VALID:**
```dhl
if order.value > 1000 {
    apply_discount(20%)
} else if order.value > 500 {
    apply_discount(10%)
} else {
    apply_discount(0%)  // Explicit: no discount
}
```

#### 2.2.2 Condition Reachability

The prover eliminates unreachable conditions.

**Example - ERROR (Unreachable Condition):**
```dhl
if order.value > 1000 {
    tier = Premium
} else if order.value > 2000 {  // IMPOSSIBLE: already handled by first
    tier = UltraPremium
} else {
    tier = Standard
}
```

**Compiler Output:**
```
error[E0004]: unreachable condition
  --> orders.dhl:18:12
   |
17 | } else if order.value > 1000 {
   |            --------------- values > 1000 handled here
18 | } else if order.value > 2000 {
   |            ^^^^^^^^^^^^^^^ this condition is always false
   |
   = help: remove this branch or adjust previous condition
```

#### 2.2.3 Boolean Exhaustiveness

Boolean conditions must cover `true` and `false`.

```dhl
// COMPILE ERROR: Missing `false` case for is_vip
if customer.is_vip && order.value > 100 {
    apply_vip_discount()
}
```

**Fix:**
```dhl
if customer.is_vip && order.value > 100 {
    apply_vip_discount()
} else {
    // Explicitly handle: not VIP OR value <= 100
    apply_standard_pricing()
}
```

### 2.3 State Machines

#### 2.3.1 State Definition Creates Obligations

Defining a state machine automatically generates proof obligations for all transitions.

**Syntax:**
```dhl
statemachine <Name> {
    <State1>, <State2>, ..., <StateN>
}
```

**Generated Proof Obligations:**

1. **Transition Totality:** For every state, all possible transitions must be defined or explicitly forbidden
2. **Reachability:** Every state must be reachable from the initial state
3. **Terminal Consistency:** Terminal states (no outgoing transitions) must be explicitly marked

#### 2.3.2 Transition Validity

**Example - State Machine Definition:**
```dhl
statemachine OrderStatus {
    Draft,           // Initial
    Submitted,
    Paid,
    Fulfilled,
    Shipped,
    Delivered,       // Terminal
    Cancelled,       // Terminal
    Refunded,        // Terminal
}

// Define valid transitions
transitions OrderStatus {
    Draft       -> [Submitted, Cancelled],
    Submitted   -> [Paid, Cancelled],
    Paid        -> [Fulfilled, Refunded, Cancelled],
    Fulfilled   -> [Shipped],
    Shipped     -> [Delivered],
    Delivered   -> [Refunded],
    Cancelled   -> [],  // Terminal
    Refunded    -> [],  // Terminal
}
```

#### 2.3.3 Transition Action Verification

Actions that transition states are automatically verified.

**Example - ERROR (Invalid Transition):**
```dhl
action process_shipment {
    requires OrderStatus == Draft;  // Current state
    
    // COMPILE ERROR: Draft -> Shipped is not a valid transition
    transition Draft -> Shipped;
    
    create_shipping_label();
}
```

**Compiler Output:**
```
error[E0005]: invalid state transition
  --> fulfillment.dhl:45:5
   |
45 |     transition Draft -> Shipped;
   |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^
   |
   = note: valid transitions from Draft are: [Submitted, Cancelled]
   = help: transition to Submitted first, then to Paid, then to Fulfilled, then to Shipped
```

#### 2.3.4 Unreachable State Detection

```dhl
statemachine Example {
    A, B, C, D
}

transitions Example {
    A -> [B],
    B -> [C],
    C -> [],    // Terminal
    D -> [A],   // ERROR: D unreachable from initial state A
}
```

**Compiler Output:**
```
error[E0006]: unreachable state
  --> example.dhl:10:5
   |
10 |     D -> [A],
   |     ^ unreachable from initial state A
   |
   = help: add transition from A, B, or C to D, or remove state D
```

#### 2.3.5 Terminal State Enforcement

Terminal states cannot have outgoing transitions.

```dhl
transitions OrderStatus {
    Delivered -> [Refunded],  // OK: Refunded is also terminal
    Refunded  -> [Shipped],   // ERROR: Terminal state has outgoing transition
}
```

### 2.4 Decision Tables

#### 2.4.1 Cartesian Completeness

Decision tables must cover all combinations of input dimensions.

**Syntax:**
```dhl
decide <name> {
    | <dim1> | <dim2> | ... | <dimN> | <output> |
    |--------|--------|-----|--------|----------|
    | val1   | valA   | ... | result | action1  |
    | ...    | ...    | ... | ...    | ...      |
}
```

**Proof Obligation:**
```
For dimensions D₁ × D₂ × ... × Dₙ,
rules must cover |D₁| × |D₂| × ... × |Dₙ| combinations.
```

**Example - ERROR (Incomplete Coverage):**
```dhl
decide shipping_method {
    | Region        | Weight   | Method    |
    |---------------|----------|-----------|
    | Domestic      | Light    | Standard  |
    | Domestic      | Heavy    | Freight   |
    | International | Light    | Air       |
    // Missing: International × Heavy
}
```

**Compiler Output:**
```
error[E0007]: incomplete decision table
  --> shipping.dhl:20:1
   |
20 | / decide shipping_method {
21 | |     | Region        | Weight   | Method    |
22 | |     |---------------|----------|-----------|
23 | |     | Domestic      | Light    | Standard  |
24 | |     | Domestic      | Heavy    | Freight   |
25 | |     | International | Light    | Air       |
26 | | }
   | |_^ missing 1 combination: [International, Heavy]
   |
   = help: add rule for [International, Heavy] or use wildcard `_`
   = note: decision tables must be exhaustive for audit compliance
```

**Example - VALID (Wildcard Default):**
```dhl
decide shipping_method {
    | Region        | Weight   | Method    |
    |---------------|----------|-----------|
    | Domestic      | _        | Standard  |
    | International | _        | Air       |
}
```

#### 2.4.2 Rule Consistency (No Overlaps)

Each input combination must map to exactly one output.

**Example - ERROR (Overlapping Rules):**
```dhl
decide discount_rate {
    | CustomerTier | OrderValue | Discount |
    |--------------|------------|----------|
    | VIP          | _          | 20%      |
    | _            | High       | 15%      |  // Overlaps with VIP + High!
    | _            | _          | 0%       |
}
```

**Compiler Output:**
```
error[E0008]: overlapping rules in decision table
  --> discounts.dhl:24:5
   |
23 |     | VIP          | _          | 20%      |
   |     |--------------|------------|----------|
24 |     | _            | High       | 15%      |
   |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
   |
   = note: input [VIP, High] matches both rules
   = help: make rules mutually exclusive by adding constraints
```

**Fix - Explicit Priority:**
```dhl
decide discount_rate {
    | CustomerTier | OrderValue | Discount | Priority |
    |--------------|------------|----------|----------|
    | VIP          | High       | 25%      | 1        |
    | VIP          | _          | 20%      | 2        |
    | _            | High       | 15%      | 3        |
    | _            | _          | 0%       | 4        |
}
```

### 2.5 Error Handling

#### 2.5.1 Result Type Obligations

Functions returning `Result<T, E>` must have their errors handled at call sites.

**Example - ERROR (Unhandled Result):**
```dhl
fn charge_payment(token: Token, amount: Money) -> Result<Receipt, PaymentError>;

// COMPILE ERROR: Result not handled
action process_order {
    charge_payment(order.payment_token, order.total);  // Error!
    mark_as_paid();
}
```

**Compiler Output:**
```
error[E0009]: unhandled Result type
  --> orders.dhl:56:5
   |
56 |     charge_payment(order.payment_token, order.total);
   |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
   |
   = note: function returns Result<Receipt, PaymentError>
   = help: use `?` to propagate, `match` to handle, or `.unwrap()` with proof
```

**Example - VALID (Error Propagation):**
```dhl
action process_order {
    let receipt = charge_payment(order.payment_token, order.total)?;
    // ? propagates error, compiler knows receipt is Receipt (not Result)
    mark_as_paid(receipt);
}
```

**Example - VALID (Explicit Match):**
```dhl
action process_order {
    match charge_payment(order.payment_token, order.total) {
        Ok(receipt) => {
            mark_as_paid(receipt);
            Ok(())
        }
        Err(PaymentError::InsufficientFunds) => {
            notify_customer("Insufficient funds");
            Err(FulfillmentError::PaymentFailed)
        }
        Err(PaymentError::CardDeclined) => {
            notify_customer("Card declined");
            Err(FulfillmentError::PaymentFailed)
        }
        Err(e) => {
            log_error(e);
            queue_for_manual_review();
            Ok(())
        }
    }
}
```

#### 2.5.2 Option Type Obligations

Option types must be explicitly unwrapped with proof of non-nullity.

**Example - ERROR (Unverified Unwrap):**
```dhl
action apply_discount {
    let customer = find_customer(order.customer_id);  // Option<Customer>
    
    // COMPILE ERROR: Option not verified
    if customer.tier == VIP {  // Cannot access .tier on Option<Customer>
        apply_vip_discount();
    }
}
```

**Example - VALID (Pattern Match):**
```dhl
action apply_discount {
    match find_customer(order.customer_id) {
        Some(customer) if customer.tier == VIP => apply_vip_discount(),
        Some(_) => apply_standard_discount(),
        None => reject_order("Unknown customer"),
    }
}
```

**Example - VALID (Proof via Prior Check):**
```dhl
action apply_discount {
    let customer_opt = find_customer(order.customer_id);
    
    if customer_opt.is_none() {
        return reject_order("Unknown customer");
    }
    
    // Compiler knows customer_opt is Some(_) here
    let customer = customer_opt.unwrap();  // OK: proven safe
    
    if customer.tier == VIP {
        apply_vip_discount();
    }
}
```

#### 2.5.3 Forbidding `unwrap()` and `expect()`

By default, `unwrap()` and `expect()` are forbidden in production code.

**Example - ERROR:**
```dhl
let config = load_config().unwrap();  // COMPILE ERROR
```

**Compiler Output:**
```
error[E0010]: unwrap() not allowed without proof of safety
  --> config.dhl:12:25
   |
12 |     let config = load_config().unwrap();
   |                         ^^^^^ unwrap() may panic
   |
   = help: use pattern matching or ? operator instead
   = note: unsafe { ... } block required for explicit unwrap
```

**Escape Hatch (Explicit Unsafe):**
```dhl
// Only allowed with proof comment
let config = unsafe { 
    // SAFETY: Config was validated in prior bootstrap step
    load_config().unwrap() 
};
```

### 2.6 Arithmetic Safety

#### 2.6.1 Division by Zero

All division and modulo operations must have non-zero divisors.

**Example - ERROR (Possible Division by Zero):**
```dhl
fn calculate_average(total: Decimal, count: Int) -> Decimal {
    total / count  // COMPILE ERROR: count could be 0
}
```

**Compiler Output:**
```
error[E0011]: possible division by zero
  --> math.dhl:15:5
   |
15 |     total / count
   |           ^^^^^^^
   |
   = note: `count` has type Int which includes 0
   = help: check `count != 0` before dividing or use NonZeroInt type
```

**Example - VALID (Explicit Check):**
```dhl
fn calculate_average(total: Decimal, count: Int) -> Option<Decimal> {
    if count == 0 {
        None
    } else {
        Some(total / count)
    }
}
```

**Example - VALID (Non-Zero Type):**
```dhl
fn calculate_average(total: Decimal, count: NonZeroInt) -> Decimal {
    total / count  // OK: NonZeroInt proves count ≠ 0
}

// Caller must prove non-zero
let count = NonZeroInt::try_from(raw_count)?;
let avg = calculate_average(total, count);
```

#### 2.6.2 Integer Overflow

Arithmetic operations that can overflow must be checked or use saturating/wrapping semantics.

**Example - ERROR (Possible Overflow):**
```dhl
let total = price * quantity;  // Could overflow
```

**Compiler Output:**
```
error[E0012]: possible integer overflow
  --> math.dhl:23:17
   |
23 |     let total = price * quantity;
   |                 ^^^^^^^^^^^^^^^^
   |
   = note: use checked_mul, saturating_mul, or prove bounds
```

**Example - VALID (Checked Arithmetic):**
```dhl
let total = price.checked_mul(quantity)?;
```

**Example - VALID (Proven Bounds):**
```dhl
// Compiler can prove no overflow if:
// - price <= MAX_PRICE (const)
// - quantity <= MAX_QUANTITY (const)
// - MAX_PRICE * MAX_QUANTITY < Int::MAX
let total = price * quantity;  // OK: proven safe
```

### 2.7 Array and Slice Safety

#### 2.7.1 Bounds Checking

All indexing operations must be within bounds or explicitly checked.

**Example - ERROR (Unverified Index):**
```dhl
let items = get_order_items();
let first = items[0];  // COMPILE ERROR: items could be empty
```

**Compiler Output:**
```
error[E0013]: index out of bounds
  --> orders.dhl:34:15
   |
34 |     let first = items[0];
   |               ^^^^^^
   |
   = note: slice `items` may be empty
   = help: check `.len() > 0` before indexing or use `.get()`
```

**Example - VALID (Pattern Match):**
```dhl
match items {
    [] => handle_empty(),
    [first, ..] => process_first(first),
}
```

**Example - VALID (Checked Access):**
```dhl
if let Some(first) = items.get(0) {
    process_first(first);
} else {
    handle_empty();
}
```

#### 2.7.2 Iterator Safety

Iterators are always safe and preferred over indexing.

```dhl
// Always safe, no proof needed
for item in items {
    process(item);
}

// Safe with early exit
let found = items.iter().find(|i| i.id == target_id);
```

---

## 3. Advanced Proof Features

### 3.1 Proof-Carrying Code (PCC)

Compiled artifacts include proof certificates that can be verified independently.

**Compiled Contract Format:**
```json
{
  "contract": "order_processing",
  "version": "1.0.0",
  "proofs": {
    "exhaustiveness": {
      "status": "verified",
      "matches": [
        {
          "location": "orders.dhl:45:3",
          "type": "PaymentStatus",
          "variants_covered": 4,
          "variants_total": 4
        }
      ]
    },
    "arithmetic_safety": {
      "status": "verified",
      "divisions": [
        {
          "location": "math.dhl:23:5",
          "divisor_nonzero": "proven_by_guard"
        }
      ]
    },
    "state_machine": {
      "status": "verified",
      "states": 8,
      "transitions": 12,
      "unreachable_states": [],
      "deadlock_free": true
    }
  },
  "solver_time_ms": 145,
  "verified_by": "dhlc-prover-2.0"
}
```

### 3.2 SMT Solver Integration

For complex arithmetic and guard conditions, the prover uses SMT solvers (Z3, CVC5).

**Example - Complex Guard Proof:**
```dhl
match (a, b, c) {
    (x, y, z) if x > 0 && y > x && z > x + y => case1(),
    (x, y, z) if x < 0 && y < x && z < x + y => case2(),
    // Prover must prove: these guards are mutually exclusive
    // and cover all cases where the conditions apply
}
```

**SMT Encoding:**
```smt
(declare-fun x () Int)
(declare-fun y () Int)
(declare-fun z () Int)

; Guard 1: x > 0 && y > x && z > x + y
(define-fun guard1 () Bool
  (and (> x 0) (> y x) (> z (+ x y))))

; Guard 2: x < 0 && y < x && z < x + y
(define-fun guard2 () Bool
  (and (< x 0) (< y x) (< z (+ x y))))

; Prove mutual exclusion
(assert (not (and guard1 guard2)))
(check-sat)
; Expected: unsat (guards never true simultaneously)
```

### 3.3 Effect Tracking

The prover tracks all effects (IO, mutation, errors) to ensure they are handled.

**Effect Types:**
- `read<Subject>`: Reads from subject state
- `write<Subject>`: Writes to subject state
- `io<External>`: External IO (HTTP, files)
- `error<E>`: Can return error E
- `panic`: Can panic (forbidden in safe code)

**Effect Polymorphism:**
```dhl
// Function signature with effects
fn get_customer(id: CustomerId) -> Option<Customer>
  effects [read<Customer>, error<DatabaseError>];

// Caller must handle effects
action process {
  let customer = get_customer(id)?;  // Handles error effect
  // Option effect handled by pattern matching or ?
}
```

### 3.4 Termination Proofs

Recursive functions and loops must provably terminate.

**Example - Recursive Function:**
```dhl
fn factorial(n: Int) -> Int
  requires n >= 0  // Base case
{
    if n == 0 {
        1
    } else {
        n * factorial(n - 1)  // Decreasing measure: n
    }
}
// Prover verifies: n decreases, eventually reaches 0
```

**Example - Loop Termination:**
```dhl
while remaining > 0 {
    process_one();
    remaining = remaining - 1;  // Decreasing measure
}
// Prover verifies: remaining decreases, eventually 0
```

---

## 4. Error Messages and Diagnostics

### 4.1 Structured Error Format

All prover errors follow a consistent format:

```
error[<code>]: <short description>
  --> <file>:<line>:<col>
   |
<line> | <source code>
   |   <pointer to issue>
   |
   = <explanation>
   = help: <suggested fix>
   = note: <additional context>
```

### 4.2 Error Codes Reference

| Code | Category | Description |
|------|----------|-------------|
| E0001 | Exhaustiveness | Non-exhaustive pattern match |
| E0002 | Overlap | Overlapping match guards |
| E0003 | Completeness | Missing else branch |
| E0004 | Reachability | Unreachable condition |
| E0005 | State Machine | Invalid state transition |
| E0006 | State Machine | Unreachable state |
| E0007 | Decision Table | Incomplete coverage |
| E0008 | Decision Table | Overlapping rules |
| E0009 | Effects | Unhandled Result type |
| E0010 | Safety | unwrap() without proof |
| E0011 | Arithmetic | Possible division by zero |
| E0012 | Arithmetic | Possible integer overflow |
| E0013 | Bounds | Index out of bounds |
| W0001 | Style | Guard could be simplified |
| W0002 | Performance | Unnecessary match arm |

### 4.3 Interactive Fixes

The compiler suggests automated fixes:

```bash
$ dh compile orders.dhl
error[E0001]: non-exhaustive patterns: `Expired` not covered
  --> orders.dhl:42:3

$ dh fix orders.dhl  # Interactive fix application
Suggested fix: Add missing match arm for `Expired`?
[Y/n]: y
Applied: Added `Expired => todo!("handle expired"),`
```

---

## 5. Implementation Architecture

### 5.1 Compiler Pipeline Integration

```
DHL Source Code
    │
    ▼
Lexer ──► Tokens
    │
    ▼
Parser ──► AST
    │
    ▼
Type Checker ──► Typed AST
    │
    ▼
Proof Engine ──► Proof Obligations
    │
    ├──► Exhaustiveness Checker
    │       - Pattern coverage analysis
    │       - Enum variant tracking
    │
    ├──► Reachability Analyzer
    │       - Control flow graph
    │       - Dead code detection
    │
    ├──► Effect System
    │       - Effect inference
    │       - Effect checking
    │
    ├──► Arithmetic Prover
    │       - Range analysis
    │       - Division by zero check
    │
    ├──► Bounds Checker
    │       - Array length tracking
    │       - Slice bounds propagation
    │
    └──► SMT Solver Interface
            - Guard satisfiability
            - Mutual exclusion proofs
            - Complex constraint solving
    │
    ▼
Proof Certificates + Compiled Artifact
```

### 5.2 Proof Obligation Generation

```rust
// Pseudocode for proof obligation generation

enum ProofObligation {
    Exhaustive {
        location: Span,
        match_type: Type,
        uncovered_patterns: Vec<Pattern>,
    },
    NonZeroDivisor {
        location: Span,
        divisor: Expression,
        proof: Option<ProofTerm>,
    },
    ValidTransition {
        location: Span,
        from_state: State,
        to_state: State,
        valid_transitions: Vec<Transition>,
    },
    // ... etc
}

fn generate_proof_obligations(ast: &Ast) -> Vec<ProofObligation> {
    let mut obligations = vec![];
    
    for node in ast.walk() {
        match node {
            AstNode::Match { expr, arms } => {
                let match_type = expr.get_type();
                let covered = arms.iter().map(|a| &a.pattern).collect();
                let uncovered = match_type.variants().difference(&covered);
                
                if !uncovered.is_empty() {
                    obligations.push(ProofObligation::Exhaustive {
                        location: node.span(),
                        match_type,
                        uncovered_patterns: uncovered,
                    });
                }
                
                // Check guard mutual exclusivity
                obligations.extend(check_guard_overlap(arms));
            }
            
            AstNode::BinaryOp { op: Div, left, right } => {
                if !is_nonzero_proven(right) {
                    obligations.push(ProofObligation::NonZeroDivisor {
                        location: node.span(),
                        divisor: right.clone(),
                        proof: None,
                    });
                }
            }
            
            AstNode::StateTransition { from, to } => {
                if !is_valid_transition(from, to) {
                    obligations.push(ProofObligation::ValidTransition {
                        location: node.span(),
                        from_state: from.clone(),
                        to_state: to.clone(),
                        valid_transitions: get_valid_transitions(from),
                    });
                }
            }
            
            // ... other constructs
        }
    }
    
    obligations
}
```

### 5.3 Solver Integration

```rust
// SMT solver interface for complex proofs

struct SmtProver {
    solver: z3::Solver,
    context: z3::Context,
}

impl SmtProver {
    fn prove_guard_mutex(&self, guards: &[Guard]) -> Result<(), ProofError> {
        for (i, g1) in guards.iter().enumerate() {
            for g2 in guards.iter().skip(i + 1) {
                // Check if g1 AND g2 is satisfiable
                let overlap = self.solver.check_sat(&[g1, g2]);
                
                if overlap == SatResult::Sat {
                    return Err(ProofError::OverlappingGuards {
                        guard1: g1.clone(),
                        guard2: g2.clone(),
                    });
                }
            }
        }
        Ok(())
    }
    
    fn prove_decision_table_complete(
        &self,
        dimensions: &[Dimension],
        rules: &[Rule],
    ) -> Result<(), ProofError> {
        // Generate all combinations
        let combinations = cartesian_product(dimensions);
        
        for combo in combinations {
            let covered = rules.iter().any(|r| r.covers(&combo));
            if !covered {
                return Err(ProofError::IncompleteDecisionTable {
                    uncovered: combo,
                });
            }
        }
        
        Ok(())
    }
}
```

### 5.4 Performance Considerations

| Phase | Time Budget | Optimization |
|-------|-------------|--------------|
| Lex/Parse | <10ms | Cached incremental parsing |
| Type Check | <50ms | Parallel subtree checking |
| Exhaustiveness | <20ms | Bitmap representation for enums |
| SMT Queries | <100ms | Query caching, incremental solving |
| **Total** | **<200ms** | Per-file, parallel across modules |

---

## 6. Unsafe Escape Hatch

### 6.1 Unsafe Blocks

For interop with unverified code or intentionally skipping proofs:

```dhl
unsafe {
    // Code in here is not verified by the prover
    external_untrusted_function();
    risky_operation().unwrap();  // Allowed in unsafe
}
```

### 6.2 Unsafe Function Declarations

```dhl
unsafe fn parse_legacy_format(data: Bytes) -> Order {
    // Must be called within unsafe block or unsafe function
}

action process_legacy {
    unsafe {
        let order = parse_legacy_format(raw_data);
        // ...
    }
}
```

### 6.3 Audit Trail

All unsafe code is flagged in compiled artifacts:

```json
{
  "safety": {
    "verified": true,
    "unsafe_blocks": [
      {
        "location": "legacy.dhl:45:5",
        "reason": "interop with external payment processor",
        "reviewed_by": "security-team",
        "review_date": "2024-01-15"
      }
    ]
  }
}
```

---

## 7. Migration Strategy

### 7.1 Gradual Adoption

For existing DHL codebases:

**Phase 1: Warning Mode (Weeks 1-2)**
```bash
dh compile --prover=warn  # Warnings instead of errors
```

**Phase 2: Critical Paths Only (Weeks 3-4)**
```dhl
#[prove]  // Opt-in annotation for new code
action financial_transaction {
    // Prover enforced here
}

action logging {  // No enforcement yet
    // ...
}
```

**Phase 3: Full Enforcement (Week 5+)**
```bash
dh compile --prover=strict  # All code must pass
```

### 7.2 Automated Migration

```bash
# Generate fix suggestions
dh migrate --prover-suggestions > fixes.json

# Apply automated fixes
dh migrate --apply --input=fixes.json
```

---

## 8. Testing the Prover

### 8.1 Positive Tests (Should Compile)

```dhl
// tests/prover/should_compile/exhaustive_match.dhl
#[test_prover]
fn test_exhaustive_bool() {
    match true {
        true => 1,
        false => 0,  // Required
    }
}
```

### 8.2 Negative Tests (Should Fail)

```dhl
// tests/prover/should_fail/non_exhaustive.dhl
#[test_prover]
fn test_non_exhaustive() {
    match Some(42) {
        Some(x) => x,
        // ERROR: Missing None case
    }
}
```

### 8.3 Proof Certificate Validation

```bash
# Verify compiled proof certificates
dh verify-proof order.contract
# Output: All 47 proof obligations verified ✓
```

---

## 9. Future Extensions

### 9.1 Temporal Logic Proofs

```dhl
// Prove: After payment, eventually either delivered OR refunded
property payment_resolution {
    always (PaymentReceived => eventually (Delivered || Refunded))
}
```

### 9.2 Resource Linear Types

```dhl
// Ensure inventory is properly managed
fn allocate_inventory(qty: Quantity) -> InventoryToken {
    // Token must be consumed (shipped or returned)
}
```

### 9.3 Differential Privacy

```dhl
// Prove queries don't leak individual data
query aggregate_sales 
  requires proof: epsilon_differential_privacy(1.0)
{
    // ...
}
```

---

## 10. Summary

The DHL Compiler Prover provides **zero-annotation, automatic verification** of:

| Property | Mechanism | User Impact |
|----------|-----------|-------------|
| **Exhaustiveness** | Pattern analysis | No forgotten cases |
| **Totality** | Effect tracking | No runtime panics |
| **Consistency** | SMT solving | No contradictory rules |
| **Safety** | Type + bounds checking | No crashes |
| **Auditability** | Proof certificates | Compliance verification |

**Key Principle:** *Correctness is the default, not an opt-in.*

---

## Appendix A: Formal Grammar Extensions

```ebnf
MatchExpression ::= "match" Expression "{" MatchArm+ "}"

MatchArm ::= Pattern ("if" Guard)? "=>" Expression ","?

Pattern ::= 
    | "_"                                    (* Wildcard *)
    | Identifier                             (* Variable binding *)
    | Constructor ("{" FieldPattern* "}")?  (* Struct/enum *)
    | "[" Pattern* "]"                      (* Array/Slice *)
    | "(" Pattern ")"                       (* Grouping *)

Guard ::= Expression  (* Boolean expression *)

StateMachine ::= 
    "statemachine" Identifier "{" 
        State ("," State)* 
    "}"
    
Transitions ::= 
    "transitions" Identifier "{" 
        Transition+ 
    "}"

Transition ::= State "->" StateList

StateList ::= State | "[" State ("," State)* "]"

DecisionTable ::= 
    "decide" Identifier "{"
        "|" Column+ "|"
        Row+
    "}"
```

## Appendix B: Proof Certificate Schema

```json
{
  "$schema": "https://dharma.io/schemas/proof-certificate-v1.json",
  "format_version": "1.0",
  "contract": {
    "name": "string",
    "version": "string",
    "hash": "sha256:hex"
  },
  "prover": {
    "version": "string",
    "solver": "z3|cvc5|custom",
    "solver_version": "string"
  },
  "timestamp": "ISO8601",
  "proofs": {
    "exhaustiveness": [...],
    "arithmetic": [...],
    "state_machines": [...],
    "effects": [...]
  },
  "statistics": {
    "solver_time_ms": number,
    "obligations_total": number,
    "obligations_proven": number
  }
}
```
