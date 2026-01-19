# DHL (Literate DHARMA Domain Law) Review

## Overview

DHL is a domain-specific language embedded in Markdown for defining the data model and logic of DHARMA subjects. It aims to bridge the gap between human-readable legal/business contracts and machine-executable code.

The current implementation includes:
-   **AST (`src/pdl/ast.rs`):** Defines the structure of aggregates, fields, actions, and expressions.
-   **Parser (`src/pdl/parser.rs`):** A `nom`-based parser that extracts DHL blocks from Markdown and parses them into the AST.
-   **Schema (`src/pdl/schema.rs`):** A CBOR-serializable schema definition that represents the compiled contract interface.

## Strengths

1.  **Literate Programming First:** Embedding code within Markdown documents (` ```dhl ` blocks) is a strong design choice. It encourages documentation and makes the "contract" readable by non-developers.
2.  **Simple & Explicit:** The syntax is concise and declarative (`aggregate`, `state`, `action`, `validate`, `apply`). It forces a clear separation between state definition, validation logic, and state mutation.
3.  **Strict Typing:** The type system (`Int`, `Text`, `Bool`, `Enum`, `Identity`, `Ref`) is well-defined and serializable to CBOR. This ensures interoperability and determinism.
4.  **Privacy by Default:** The `Visibility` (Public/Private) modifier on fields is natively supported in the parser and schema, aligning with DHARMA's privacy-first architecture (Overlays).
5.  **Inheritance (`extends`):** The `aggregate ... extends ...` syntax allows for composition and reuse of standard schemas, which is crucial for a standard library.

## Weaknesses & Limitations

1.  **Rudimentary Expression Language:** The current `Expr` enum only supports `Raw(String)`. The parser extracts validation logic as raw strings (`state.status == Pending`), but there is no evidence of a proper expression parser or evaluator in the inspected code. This effectively delegates logic execution to an external (likely Wasm) runtime without providing a safe, strictly typed way to define that logic in DHL itself.
2.  **Limited Type System:**
    -   No collection types (`List`, `Map`) in the AST or Parser (though `schema.rs` has `TypeDesc::List/Map`, the parser `type_parser` doesn't seem to handle them genericly).
    -   `Text` length limits are optional but important for binary size control.
3.  **No Process/Flow Control:** BPM (Business Process Management) requires defining *flows* (state machines), not just atomic state transitions. DHL currently defines *actions* but not the allowed *sequences* of actions (except implicitly via `validate` clauses). Visualizing the lifecycle requires inspecting all `validate` blocks.
4.  **No Event Emission:** CQRS typically involves emitting events. DHL defines `apply` (state mutation), but explicit event definition is implicit (the Action *is* the event). This is a design choice, but it limits "side effects" to just state changes.
5.  **Hardcoded "Context":** The validation logic references `context.signer`, but the definition of `context` is implicit.

## Missing Features for Complete CQRS / BPM

To become a full-fledged system for business logic, DHL needs:

### 1. Robust Expression Parser
Instead of `Raw(String)`, DHL needs a structured expression tree:
-   Binary Ops: `==`, `!=`, `>`, `<`, `&&`, `||`
-   Accessors: `state.field`, `args.field`, `context.signer`
-   Literals: `123`, `"text"`, `true`, `Enum.Variant`
-   Functions: `len()`, `contains()`, `now()`

### 2. State Machine Definitions (BPM)
Explicitly defining valid transitions would allow generating UI diagrams and enforcing flow:
```dhl
flow Lifecycle {
    start -> Pending
    Pending -> (Approve) -> Approved
    Pending -> (Reject) -> Rejected
    Approved -> (Pay) -> Paid
}
```

### 3. Collection Support
Full support for `List<T>` and `Map<K,V>` in the parser and AST is essential for real-world models (e.g., "Line Items" in an invoice).

### 4. Role-Based Access Control (RBAC) Integration
While `context.signer` exists, a declarative way to check roles would be powerful:
```dhl
validate
    has_role(context.signer, "Approver")
```

### 5. Foreign Key / Ref Integrity
The `Ref<T>` type exists, but DHL doesn't yet specify how to validate that a reference exists or matches a specific state in another subject (Cross-Subject Validation).

## Conclusion

DHL is a solid **Interface Definition Language (IDL)** for DHARMA. It successfully defines "Shape" and "Intent". However, as a **Logic Definition Language**, it is currently incomplete due to the lack of a structured expression compiler. It relies on the Wasm generator to interpret the raw strings, which is fragile.

**Recommendation:** Prioritize implementing a proper Expression Parser (Task 11) to make the `validate` blocks safe and verifiable before compiling to Wasm.
