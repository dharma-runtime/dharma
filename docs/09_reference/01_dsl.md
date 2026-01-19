# DHL Reference (Current Compiler)

This document describes the **DHL syntax that is currently implemented** in the `dh` compiler.

> DHL is embedded in Markdown. Only ` ```dhl ` blocks are compiled.

---

## 1) File Structure

An DHL file is Markdown with front-matter and one or more `dhl` blocks.

````markdown
---
namespace: com.acme.orders
version: 1.0.0
# optional
import:
  - ../stdlib/base.dhl
concurrency: strict
---

```dhl
aggregate Order
    state
        public amount: Int
        public status: Enum(Draft, Paid)
```
````

### Front-Matter Keys
- `namespace`: required, string
- `version`: required, string (data version = major component)
- `import`: optional list of relative paths
- `concurrency`: optional `strict | allow`

---

## 2) Top-Level Blocks

### `package`
Declares package namespace inside `dhl` blocks (optional if front-matter exists).

```dhl
package com.acme.orders
```

### `external`
Declared but currently **not enforced** by the runtime. Reserved for future integration.

```dhl
external
    roles: [admin, auditor]
    time: [block_time]
    datasets: [fx.rates]
```

### `aggregate`
Defines the state schema and invariants.

```dhl
aggregate Order
    state
        public amount: Int
        public status: Enum(Draft, Paid)
    invariant
        state.amount >= 0
```

### `flow`
Generates actions for state machines. **All flows target the `status` field.**

```dhl
flow Lifecycle
    'Draft -> [Pay] -> 'Paid
```

### `action`
Defines transitions.

```dhl
action Pay()
    validate
        state.status == 'Draft
    apply
        state.status = 'Paid
```

### `reactor`
Parsed and compiled, but **not yet executed** by the runtime.

```dhl
reactor AutoApprove
    trigger action.Pay
    validate
        state.amount < 100
    emit action.Approve()
```

### `view`
Parsed but not used by the runtime (reserved for Workspace UI).

---

## 3) State Fields & Visibility

```
public amount: Int
private secret_note: Text(len=256)
```

- Default visibility is **public**.
- If an aggregate `extends` another aggregate, default visibility becomes **private**.

---

## 4) Apply-Block Assignments

Assignments can only target `state.<field>`.

```dhl
state.status = 'Paid
```

> **Note:** Direct `state.field = "text"` is currently a stub (writes empty). Use action args or list/map mutations for text.

### List & Map Mutations

Supported in `apply`:

```dhl
state.tags.push("urgent")
state.tags.remove("urgent")
state.meta.set("source", "email")
```

These compile into the internal call forms:

- `push(state.tags, "urgent")`
- `remove(state.tags, "urgent")`
- `set(state.meta, "source", "email")`

---

## 5) Expression Language (Current Subset)

The current compiler is **strict** and supports only a subset of the full CEL design.

### Supported literals (by context)
- **Int** literals: `42`, `-1`
- **Bool** literals: `true`, `false`
- **Enum** literals: `'Pending` (valid in enum comparisons and enum assignments)
- **Text** literals: allowed in text equality checks (`==`/`!=`) and list/map mutations
- **List/Map** literals: allowed only in `len`, `in`, `contains`, `index/get`, and `sum`
- **null**: only allowed for optional assignments/defaults

### Supported operators
| Category | Operators | Notes |
| --- | --- | --- |
| Arithmetic | `+ - * / %` | Int only |
| Comparison | `== != > < >= <=` | Int, Bool, Enum, Identity (limited) |
| Logic | `and or not` | Bool only |
| Set | `in` | List literal or list path only |

### Supported functions
| Function | Status | Notes |
| --- | --- | --- |
| `len(x)` | yes | Text/List/Map path or literal list/map |
| `contains(list, item)` | yes | List literal or list/map path |
| `index(list, i)` / `get(map, k)` | yes | Literal index/key required; list/map literal or path |
| `has_role(...)` | stub | Parsed, currently always returns `true` in runtime |
| `now()` | yes | Returns context clock time |
| `distance(a, b)` | yes | GeoPoint paths only |
| `sum(list)` | yes | List of Ints (literal or path) |

## 6) Type Syntax (Quick Summary)

- `Text(len=64)`
- `Decimal(scale=2)`
- `List<Text>`
- `Map<Text, Int>`
- `Timestamp?` (optional)

Full details: [Type System](02_types.md)

---

## 7) Formal Grammar (Subset)

```ebnf
LpdlBlock   ::= PackageDef? ExternalDef? (AggregateDef | ActionDef | FlowDef | ReactorDef | ViewDef)*

AggregateDef::= "aggregate" Identifier ( "extends" Path )? 
                "state" ( FieldDef )*
                ( "invariant" Expr* )?

FieldDef    ::= ( "public" | "private" )? Identifier ":" TypeSpec ( "=" Expr )?

ActionDef   ::= "action" Identifier "(" ArgList? ")" 
                ( "validate" Expr* )? 
                ( "apply" Assignment* )?

Assignment  ::= Path "=" Expr
```

---

## 8) Known Gaps (Planned)

- General text operations in `validate` (beyond `==`/`!=`)
- Direct text assignment from literals (currently zeroes the field)
- Full CEL type coercions
- First-class `ObjectId`, `PubKey`, `delete(...)` in apply
- Reactor execution daemon
