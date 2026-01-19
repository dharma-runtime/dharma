# Task 47: Semantic Types & Rich Inputs

## Goal
Expand the DHL type system with **Semantic Wrappers** around primitive types.
These types enforce validation rules and hint the UI to provide **Rich Input Widgets**.

## The Type Catalog

| Type | Underlying | Validation | UI Widget |
| :--- | :--- | :--- | :--- |
| `Email` | `Text` | Regex | Text Input |
| `Phone` | `Text` | E.164 | Phone Input |
| `Url` | `Text` | URL Parse | Text Input |
| `Markdown` | `Text` | None | Open `$EDITOR` |
| `Color` | `Text` | Hex Regex | Color Picker |
| `Secret` | `Bytes` | None | Masked Input (****) |
| `File` | `Text` (CID) | IPFS CID | File Picker + IPFS Add |
| `Image` | `Text` (CID) | IPFS CID | File Picker + IPFS Add |
| `SemVer` | `Text` | SemVer Regex | Text Input |
| `Cron` | `Text` | Parser Check | Text Input (Cron Helper) |
| `IBAN` | `Text` | Mod 97 | Text Input |
| `Country`| `Text` | ISO 3166 | Dropdown (Flags) |
| `Language`|`Text` | ISO 639 | Dropdown |
| `Date` | `Text` | ISO 8601 | Date Picker |
| `Time` | `Text` | ISO 8601 | Time Picker |
| `DateTime`| `Text` | ISO 8601 | DateTime Picker |
| `Timezone`| `Text` | IANA TZ | Dropdown (Map) |

## Implementation Steps

### 1. Schema & AST
-   Update `TypeSpec` in `dharma-core` to include these variants.
-   Persist type metadata in `.schema` artifacts.

### 2. Validation Logic (Compiler)
-   Compiler injects Regex/Logic checks into Wasm.
-   `File/Image`: Validate that the string is a valid CID (Multihash).

### 3. Interactive REPL (The Wizard)
-   **Markdown:** Spawn `$EDITOR`.
-   **File/Image Handling:**
    1.  Prompt for local path.
    2.  **Blob Storage:**
        -   **Full Release:** Use built-in **`ipfs-embed`** node to `add` the file and generate a CID.
        -   **Micro Release:** Fallback to local filesystem storage in `~/.dharma/blobs/` or connect to external Kubo node via HTTP.
    3.  Return `ipfs://<CID>` as the value.

### 4. IPFS Integration
-   Add `ipfs-embed` dependency to `dharma-cli` under a feature flag (`ipfs`).
-   Implement a background worker that starts the IPFS node during REPL/Daemon initialization.
-   Provide a `dh ipfs` command group for status/gc/pins.

## Success Criteria
-   `dh compile` supports `email: Email` and `pic: Image`.
-   The REPL (Full Release) automatically adds files to the embedded IPFS node.

