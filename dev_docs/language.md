# DHARMA Language Specification: The Unified Logic Layer

This document specifies the three interlocking languages of the DHARMA ecosystem:
1.  **DHARMA CEL (Common Expression Language):** The shared core for logic, types, and values.
2.  **DHL (Literate DHARMA Domain Law):** The structure definition language (Contracts).
3.  **DHARMA-Q (Query Language):** The data projection and retrieval language.

---

## 1. DHARMA CEL (Common Expression Language)

CEL is the safe, deterministic, side-effect-free expression language used inside both DHL (validation/assignments) and DHARMA-Q (filters/projections).

### 1.1 Principles
-   **No Floats:** Only `Int` (i64), `Decimal` (fixed-point), or `Ratio`.
-   **No Loops:** Guaranteed termination.
-   **Strict Typing:** No implicit coercion.

### 1.2 Types
| Type | Keyword | Example |
| :--- | :--- | :--- |
| Integer | `Int` | `42`, `-100` |
| Boolean | `Bool` | `true`, `false` |
| Text | `Text` | `"hello world"` |
| Timestamp | `Timestamp` | `ts(1678886400)` |
| Duration | `Duration` | `dur(1h)` |
| Currency | `Currency` | `"USD"` |
| GeoPoint | `GeoPoint` | `geo(48.85, 2.35)` |
| Identity | `Identity` | `id(0xabc...)` |
| Reference | `Ref<T>` | `ref(0x123...)` |
| List | `List<T>` | `[1, 2, 3]` |
| Map | `Map<K,V>` | `{"a": 1, "b": 2}` |
| Enum | `Enum` | `'Pending`, `'Approved` |

### 1.3 Operators
| Category | Ops | Usage |
| :--- | :--- | :--- |
| Arithmetic | `+`, `-`, `*`, `/`, `%` | `a + b` |
| Comparison | `==`, `!=`, `>`, `<`, `>=`, `<=` | `a >= 10` |
| Logic | `and`, `or`, `not` | `a and (b or not c)` |
| Set | `in` | `'Pending in status_list` |
| Path | `.` | `invoice.lines[0].amount` |

### 1.4 Built-in Functions
-   `len(List|Text|Map) -> Int`
-   `contains(List|Map, Item) -> Bool`
-   `has_role(Identity, Text) -> Bool` (RBAC check)
-   `now() -> Timestamp` (Context-dependent: Block time in DHL, Query time in DHARMA-Q)
-   `distance(GeoPoint, GeoPoint) -> Int` (Meters)
-   `sum(List<Int>) -> Int`

---

## 2. DHL (Literate DHARMA Domain Law)

DHL defines **State**, **Transitions**, and **Invariants**. It is embedded in Markdown code blocks.

### 2.1 Structure
An DHL block can define:
-   `package`: Namespace declaration.
-   `external`: Dependency declarations (Roles, Time, Datasets).
-   `aggregate`: The data model.
-   `invariant`: Business physics (Always True).
-   `action`: A transaction type.
-   `flow`: (Optional) State machine visualizer and validator.
-   `reactor`: An event handler.
-   `view`: A UI projection.

### 2.2 Syntax Guide

#### Package & External
```dhl
package std.finance

external
    roles: [finance.approver, finance.viewer]
    time: [block_time]
    datasets: [fx_rates.v1]
```

#### Aggregate & Invariant
```dhl
aggregate Invoice extends std.finance.Base
    state
        public amount: Currency
        public status: Enum(Draft, Sent, Paid) = 'Draft
        public paid_at: Timestamp?

    invariant
        state.amount >= 0
        (state.status == 'Paid) -> (state.paid_at != null)
```

#### Flow (BPM)
```dhl
flow Lifecycle
    'Draft -> [Send] -> 'Sent
    'Sent -> [Approve] -> 'Paid
    'Sent -> [Reject] -> 'Draft
```
*Note: A flow block automatically generates the corresponding `validate` and `apply` logic for the referenced actions.*

#### Action
```dhl
action Send(recipient: Identity)
    validate
        state.status == 'Draft
        has_role(context.signer, 'finance.approver)
    
    apply
        state.status = 'Sent
        state.recipient = recipient
        state.sent_at = now()
```

#### Reactor
```dhl
reactor OnPayment
    trigger action.Payment.Receive
    validate trigger.state.invoice_id == state.id
    emit action.Invoice.MarkPaid()
```

#### View (UI)
```dhl
view InvoiceDetail
    layout Column
    card {
        text(state.amount, style=H1)
        badge(state.status)
    }
```

---

## 3. DHARMA-Q (Query Language)

DHARMA-Q is a **Pipeline Language**. It starts with a source (table) and flows data through operators (`|`).

### 3.1 Syntax Guide

**Basic Pipeline:**
```dhlq
invoice | where status == 'Paid' | sort -amount | take 10
```

**Joins:**
```dhlq
invoice 
| where status == 'Open'
| join customer on invoice.cust_id == customer.id
| select invoice.id, customer.name, invoice.amount
```

**Aggregations:**
```dhlq
invoice
| where date > '2023-01-01'
| by customer_id
| agg total = sum(amount), count = count()
| sort -total
```

**Search:**
```dhlq
search "cheese" or "wine" not "blue" | take 5
```

---

## 4. Formal Grammar (EBNF)

This grammar unifies CEL, DHL, and DHARMA-Q.

```ebnf
/* --- Top Level --- */
LpdlBlock   ::= PackageDef? ExternalDef? (AggregateDef | ActionDef | FlowDef | ReactorDef | ViewDef)*
Query       ::= TableSource ( "|" PipeOp )*

/* --- DHARMA CEL (Common Expression Language) --- */
Expr        ::= LogicOr
LogicOr     ::= LogicAnd ( "or" LogicAnd )*
LogicAnd    ::= Equality ( "and" Equality )*
Equality    ::= Comparison ( ( "==" | "!=" | "in" ) Comparison )*
Comparison  ::= Term ( ( ">" | "<" | ">=" | "<=" ) Term )*
Term        ::= Factor ( ( "+" | "-" ) Factor )*
Factor      ::= Unary ( ( "*" | "/" | "%" ) Unary )*
Unary       ::= ( "-" | "not" )? Atom
Atom        ::= Literal | Path | FunctionCall | "(" Expr ")"

Path        ::= Identifier ( "." Identifier | "[" Expr "]" )*
FunctionCall::= Identifier "(" ( Expr ( "," Expr )* )? ")"
Literal     ::= IntLit | StringLit | BoolLit | EnumLit | ListLit | MapLit | GeoLit

/* --- DHL Definitions --- */
PackageDef  ::= "package" Path
ExternalDef ::= "external" ( ExternalItem )*
ExternalItem::= Identifier ":" "[" ( Identifier ( "," Identifier )* )? "]"

AggregateDef::= "aggregate" Identifier ( "extends" Path )? 
                "state" ( FieldDef )*
                ( "invariant" Expr* )?

FieldDef    ::= ( "public" | "private" )? Identifier ":" TypeSpec ( "=" Expr )?

FlowDef     ::= "flow" Identifier ( Transition )*
Transition  ::= EnumLit "->" "[" Identifier "]" "->" EnumLit

ActionDef   ::= "action" Identifier "(" ArgList? ")" 
                ( "validate" Expr* )? 
                ( "apply" Assignment* )?
ArgList     ::= ArgDef ( "," ArgDef )*
ArgDef      ::= Identifier ":" TypeSpec
Assignment  ::= Path "=" Expr

ReactorDef  ::= "reactor" Identifier 
                "trigger" Path 
                ( "validate" Expr* )?
                "emit" Path "(" AssignmentList? ")"

ViewDef     ::= "view" Identifier ( ViewElement )*

TypeSpec    ::= "Int" | "Bool" | "Text" | "Identity" 
              | "Timestamp" | "Duration" | "Currency" | "GeoPoint"
              | "List" "<" TypeSpec ">" 
              | "Map" "<" TypeSpec "," TypeSpec ">"
              | "Ref" "<" Identifier ">"
              | "Enum" "(" Identifier ( "," Identifier )* ")"

/* --- DHARMA-Q Operators --- */
TableSource ::= Identifier
PipeOp      ::= WhereOp | SelectOp | SortOp | TakeOp | JoinOp | AggOp | SearchOp

WhereOp     ::= "where" Expr
SelectOp    ::= "select" ( Path ( "as" Identifier )? )+
SortOp      ::= "sort" ( "-"? Path )+
TakeOp      ::= "take" IntLit
JoinOp      ::= ( "join" | "lj" | "ij" ) Identifier "on" Expr
AggOp       ::= "by" ( Path )+ "agg" ( Identifier "=" FunctionCall )+
SearchOp    ::= "search" SearchTerm ( ( "or" | "and" ) SearchTerm )*
SearchTerm  ::= "not"? StringLit

/* --- Lexical --- */
Identifier  ::= [a-zA-Z_][a-zA-Z0-9_]*
IntLit      ::= [0-9]+
StringLit   ::= '"' [^"]* '"'
EnumLit     ::= "'" Identifier
```

---

## 5. Implementation Guide

### 5.1 The `dharma-expr` Crate
Create a shared crate that implements:
1.  **AST:** The `Expr` enum and `TypeSpec` enum.
2.  **Parser:** A `nom` implementation of the `Expr` grammar rules.
3.  **Evaluator:** A trait `EvalContext` that resolves `Path` lookups, and an implementation `eval(Expr, &Context) -> Value`.

### 5.2 DHL Integration
-   Use `dharma_expr::parser::parse_expr` inside `validate` blocks.
-   Compile `Expr` AST to Wasm instructions (Task 26).
-   **Invariants:** Compile `invariant` blocks into checks that run after every `apply`.

### 5.3 DHARMA-Q Integration
-   Use `dharma_expr::parser::parse_expr` inside `where` clauses.
-   Use the `Expr` AST to drive the Columnar Scan loop (Predicate Pushdown).

This unification ensures that **Logic is Logic**, everywhere in DHARMA.