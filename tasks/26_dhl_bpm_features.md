# Task 26: DHL Enhancements for BPM (Expressions, Collections, Reactors)

## Goal
Upgrade DHL from a "Data Definition Language" to a "Logic & Flow Language" capable of modeling complex Business Process Management (BPM) scenarios without hardcoded Rust logic.

## 1. Robust Expression Parser (The Logic Core)
**Problem:** `Expr::Raw(String)` delegates safety to Wasm at runtime. We need compile-time safety.
**Feature:** Typed Expression AST.

### Specification
```rust
enum Expr {
    // Values
    Literal(Literal),
    Path(Vec<String>), // state.status, args.amount, context.signer

    // Logic
    BinaryOp(Op, Box<Expr>, Box<Expr>), // ==, !=, >, <, &&, ||, +, -
    UnaryOp(Op, Box<Expr>),             // !

    // Functions (Built-in)
    Call(String, Vec<Expr>),            // len(x), contains(list, item), now()
}
```
**Validation:** The Compiler must type-check these expressions against the schema.

## 2. Collection Support (The Data Model)
**Problem:** BPM needs lists of approvers, line items, and attachments.
**Feature:** Generic `List<T>` and `Map<K,V>`.

### Specification
- **Syntax:** `items: List<Text>`, `meta: Map<Text, Int>`
- **AST:** Update `TypeSpec` to support generic nesting.
- **Ops:** `list.push()`, `list.remove()`, `map.get()`, `map.set()`.

## 3. Reactors (The Process Engine)
**Problem:** BPM requires automation (e.g., "If Invoice Approved, Send Email", "If Deadline Passed, Escalate").
**Feature:** `reactor` blocks in DHL.

### Specification (DHL Syntax)
```dhl
reactor OnOverdue
    trigger: Cron("0 0 * * *") // Or explicit Tick assertions
    scope: std.task
    
    validate
        state.status != Done
        now() > state.deadline

    emit action.Task.Escalate(reason = "Timeout")
```

```dhl
reactor OnApproval
    trigger: action.Approve
    emit action.Payment.Release(amount = trigger.state.amount)
```

**Implementation:**
- **Compiler:** Parse `reactor` blocks into `ReactorDef`.
- **Runtime:** `ReactorDaemon` subscribes to the Log + Clock.
- **Execution:** When trigger fires, run `validate`. If pass, generate `emit` assertion.
- **Auth:** Reactors run as a "Bot Identity" (signed by the node).

## 4. `flow` Blocks (BPM Sugar)
**Problem:** Defining complex state machines solely via `validate` blocks is hard to read and visualize.
**Feature:** `flow` syntax that desugars to state validation and transition logic.

### Specification
```dhl
flow Lifecycle
    'Draft -> [Send] -> 'Sent
    'Sent -> [Approve] -> 'Paid
```
- **Compiler:** 
    - Generates `validate state.status == 'Draft` for `action Send`.
    - Generates `apply state.status = 'Sent` for `action Send`.

## 5. RBAC & Identity Integration
**Feature:** `has_role` built-in function.
**Logic:**
- `has_role(subject, identity, role)` checks if the identity possesses the required role/capability within the context of the subject (resolved via Identity Subject or Fabric Capabilities).

## 6. `std.task` (The BPM Primitive)
**Problem:** Users need a standard way to assign work.
**Solution:** A standard library package, not a kernel keyword.

### Specification (`std/task.dharma`)
```dhl
aggregate Task
    state
        title: Text(len=128)
        assignee: Identity
        status: Enum(Open, InProgress, Done, Blocked)
        deadline: Int
        parent: Ref<Task>

action Assign(who: Identity) ...
action Complete() ...
```

## 7. DHARMA-Q Logic Integration (Unified CEL)
**Problem:** DHARMA-Q's query planner is flat (Vec<Filter> implies AND). It needs full boolean logic (OR, NOT).
**Solution:** Use the `Expr` AST (CEL) for queries too.

### Specification
- **Query Parser:** Parses `where (a > 10 or b < 5) and c == 'Active'` into `Expr` AST.
- **Query Executor:** 
    - Evaluates `Expr` against the Columnar Store.
    - `BinaryOp(Or, A, B)` -> `mask_a.or_inplace(mask_b)`.
    - `UnaryOp(Not, A)` -> `mask_a.negate()`.

## 8. Extended Type Logic Support (Wasm Compiler)
**Problem:** The current Wasm compiler throws explicit errors when trying to assign or calculate with new types (`GeoPoint`, `Currency`, `Timestamp`, `Duration`).
**Feature:** Implement Wasm lowering for these types in `codegen/wasm.rs`.

### Specification
- **Timestamp/Duration:** Lower to `i64` arithmetic in Wasm.
- **Currency:** Lower to `u32` (Symbol ID) comparison/assignment.
- **GeoPoint:** 
    - Lower to two `i32` operations (lat/lon).
    - Support `distance(p1, p2)` built-in using fixed-point approximation in Wasm.
- **Assignments:** Enable `state.geo = args.geo` and `state.ts = now()` logic paths.

## 9. Invariants & Externals (Business Physics)
**Goal:** Enforce structural correctness and explicit dependencies.

### Specification
-   **Invariant:** `invariant` block in aggregate.
    -   Compiler generates a `check_invariants()` function.
    -   Calls `check_invariants()` at the end of every `apply`.
    -   LSP uses it for symbolic verification.
-   **External:** `external` block listing dependencies.
    -   Compiler validates `has_role(..., 'role')` against declared roles.
    -   Compiler validates `now()` usage against declared `time` source.

## Implementation Roadmap
1.  **Refactor AST:** Add `Expr` tree and `TypeSpec::List/Map` in `src/pdl/ast.rs`.
2.  **Update Parser:** Implement expression parsing (precedence climbing) in `src/pdl/parser.rs`.
3.  **Language Update:** Parse `invariant`, `external`, `package` blocks.
4.  **DHARMA-Q Update:** Switch `QueryPlan` to use `Expr` instead of `Vec<Filter>`. Implement `evaluate_expr` for columnar bitsets.
5.  **Wasm Codegen Update:**
    -   Lower `Expr` AST to Wasm.
    -   Implement logic for `GeoPoint`, `Currency`, `Timestamp`, and `Duration`.
    -   Inject invariant checks.
6.  **Reactor AST:** Add parsing for `reactor` blocks.
7.  **Compiler:** Finalize `codegen/wasm.rs` for DHL v2.

## TODO (Gap Closure vs docs/language.md)
- [x] **CEL types:** add `Decimal` and `Ratio` to AST/schema/runtime/codegen.
- [x] **Builtins:** implement `distance(GeoPoint, GeoPoint)` and `sum(List<Int>)`.
- [x] **GeoPoint/Currency/Timestamp/Duration:** finish wasm lowering (GeoPoint assignment/ops; Currency comparisons).
- [x] **Type checker:** validate expression types against schema (no ad-hoc checks only).
- [x] **External enforcement:** compile-time validation of `external` (roles/time/datasets) vs usage (`has_role`, `now`, dataset refs).
- [x] **Reactors runtime:** emit reactor artifact + reactor daemon execution pipeline.
- [x] **DHARMA-Q CEL integration:** replace filter model with full CEL AST evaluation.
