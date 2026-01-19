# Task 43: DHL Aspects (Mixins)

## Goal
Enable code reuse across Aggregates using compositional **Aspects** (Mixins) instead of rigid inheritance.
This allows defining behaviors like `SeoMetadata`, `Versioned`, or `Archivable` once and applying them to multiple types.

## Specification

### 1. The `aspect` Block
An aspect defines a partial state and associated logic. It cannot exist as a standalone Subject.

```dhl
aspect Seo
    state
        meta_title: Text
        meta_desc: Text
    
    action UpdateSeo(title: Text, desc: Text)
        apply
            state.meta_title = title
            state.meta_desc = desc
```

### 2. Usage (`use`)
Aggregates consume aspects.

```dhl
aggregate Product
    use Seo
    state
        price: Int
```

### 3. Compilation Strategy (Flattening)
The compiler flattens the aspect into the aggregate **at compile time**. The Kernel (Wasm/Schema) sees a single unified object.

-   **State:** Fields are injected. Naming collision strategy:
    -   Option A: Prefix (`Seo_meta_title`).
    -   Option B: Merge (Error on collision). **Decision: Error on collision.**
-   **Actions:** Actions are added to the dispatch table.
-   **Invariants:** Aspect invariants are AND-ed with Aggregate invariants.

## Implementation Steps
1.  **Parser:** Update `dharma-cli/src/pdl/parser.rs` to handle `aspect` and `use`.
2.  **AST:** Add `AspectDef` to the AST.
3.  **Compiler:** Implement the flattening logic in `codegen/schema.rs` and `codegen/wasm.rs` *before* Wasm generation.

## Success Criteria
-   A generic `Seo` aspect can be reused by `Page` and `Product`.
-   `dh compile` produces a single Wasm binary containing logic from both.
