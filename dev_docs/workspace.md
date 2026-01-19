# DHARMA Workspace: The Sovereign Operating Environment

**Goal:** Create the "Emacs of the 21st Century" for the DHARMA ecosystem.
**Stack:** Rust + GPUI (Zed Framework) + DHARMA Kernel.
**Philosophy:** Pure Speed. Pure Truth. Zero Distraction.

---

## 1. The Core Concept

Dharma Workspace is not an "App". It is a **Surface for Interaction with Truth**.
It organizes information into **Panes**, **Buffers**, and **Splits**.

*   **The Buffer:** The fundamental unit.
    *   It can be **Text** (Source Code, Markdown).
    *   It can be **Data** (A DHARMA Subject, A Query Result).
    *   It can be **Logic** (A visual representation of a Flow).
*   **The Pane:** A container for a Buffer. Panes can be split infinitely (Horizontally/Vertically).
*   **The Command Palette (`Cmd+K`):** The universal interface for action. No ribbons. No hidden menus.

---

## 2. Modes of Operation

The Workspace adapts to the user's intent without changing its fundamental mechanics.

### A. Consumer Mode (The "Worker")
*   **Intent:** "I need to approve invoices and write notes."
*   **DHL Contracts:** Rendered as **Active Forms**.
    *   You don't see `action Approve { ... }`.
    *   You see a **Button** labeled "Approve".
    *   You don't see `state.amount = 100`.
    *   You see a **Field** labeled "Amount" with value "100".
*   **Source Code:** Visible but Read-Only.
    *   Clicking "Source" splits the pane and shows the DHL contract text. This is the **Human Documentation**. You trust the form because you can read the law.

### B. Dev Mode (The "Architect")
*   **Intent:** "I am designing a new business process."
*   **DHL Contracts:** Editable.
    *   When you save the buffer, the Workspace **Hot-Reloads** the adjacent Data Pane.
    *   You write logic on the Left. You interact with the "Live Instance" on the Right.
*   **Feedback:** Real-time validation errors appear in the gutter (LSP-style).

---

## 3. The Navigation Model (Stack vs Tree)

We avoid the "Deeply Nested Form" trap by enforcing a **Stack-Based Navigation**.

*   **Scenario:** Order -> Line Item -> Product Variant.
*   **Interaction:**
    1.  Open `Order #123`. Main Pane shows the Order Summary.
    2.  User selects a Line Item.
    3.  **Action:** The UI **Splits Right** (or pushes to stack).
    4.  New Pane loads `Product #456`.
    5.  User edits Product.
    6.  User closes Pane (pop stack). Context returns to Order.

This keeps every view simple, flat, and focused.

---

## 4. The UI Engine (Dynamic DHL Rendering)

The Workspace does not have hardcoded screens for "Invoices" or "Tasks".
It has a **Generic DHL Renderer**.

### The `view` Block
DHL contracts can define a `view` block (UI Projection).

```dhl
view TaskSummary {
    layout: Row
    icon(state.status)
    text(state.title, style=Bold)
    badge(state.priority)
    link("Open", target=view.TaskDetail)
}
```

*   **Compiler:** The Workspace parses this `view` block.
*   **Renderer:** It maps `layout: Row` to a GPUI `Flex` container.
*   **Binding:** It binds `state.title` to the Text Element. Updates are reactive (Signals).

If no `view` is defined, the Workspace renders a **Default Inspector** (Key-Value table).

---

## 5. Auditability & Inspection

Every pixel on the screen must be accountable.

*   **"Why is this field red?"**
    *   Right-click -> "Inspect Provenance".
    *   Overlay shows the **Assertion** that set this value (`action.SetPriority`, signed by `Alice`, at `12:00`).
*   **"What happens if I click this?"**
    *   Hover over a button.
    *   Tooltip shows the **Simulation**: "This will sign `action.Approve` with arguments `...`."

---

--- 

## 8. Schema-Driven Widget System

The Workspace uses a **Smart Dispatcher** to map DHL `TypeSpec` definitions to rich GPUI components. This ensures a specialized UX for every data type while maintaining a consistent aesthetic.

### A. Core Type Mappings
| DHL Type | Default Widget | Description |
| :--- | :--- | :--- |
| `Text` | `TextField` | Single-line input. |
| `Text(max=65536)` | `MarkdownEditor` | Multi-line with syntax highlighting and preview. |
| `Timestamp` | `DatePicker` | Calendar selection. |
| `Duration` | `DurationPicker` | Human-readable time windows (e.g., "3 days"). |
| `Enum(...)` | `SelectMenu` | Searchable dropdown. |
| `Identity` | `IdentityPicker` | Searchable alias list with avatar/color. |
| `List<T>` | `EditableTable` | Dynamic rows with add/remove/sort. |

### B. Custom Widget Hints
DHL `view` blocks can provide hints to override defaults:
```dhl
view Detail {
    field(state.content, widget=RichText)
    field(state.deadline, widget=RelativeTime)
    field(state.location, widget=MapPoint)
}
```

### C. Compound Widgets (Forms)
Actions are automatically rendered as **Transaction Cards**. 
- Required arguments appear as high-priority fields.
- Optional arguments are collapsed behind an "Advanced" toggle.
- The "Sign & Commit" button remains disabled until all `validate` rules pass locally (Static Analysis).

--- 

## 9. Performance Targets

*   **Startup:** < 200ms.
*   **Input Latency:** < 16ms (60fps).
*   **Memory:** < 100MB (for standard usage).
*   **Search:** Instant (via embedded DHARMA-Q).

## 7. The "Emacs" Shortcuts (Default Keymap)

*   `Cmd+P`: Go to File / Subject (Fuzzy Search).
*   `Cmd+K`: Command Palette (Run Action).
*   `Cmd+B`: Switch Buffer.
*   `Cmd+\`: Split Pane.
*   `Cmd+Enter`: Commit / Sign.

---

## Summary

The DHARMA Workspace is a tool for **High-Velocity Truth**.
It respects your intelligence. It respects your time.
It is the cockpit for the Operating System of Reality.

```