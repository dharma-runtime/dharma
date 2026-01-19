# Canonical Constructs (Stdlib)

The standard library currently contains these draft contracts under `contracts/std/`:

| Contract | Namespace | Purpose | Status |
| --- | --- | --- | --- |
| Task | `std.task` | Work tracking | Draft (partial) |
| Note | `std.note` | Text documents | Draft (partial) |
| Identity | `std.iam` | Identity + keys | Draft (partial) |
| Atlas | `std.atlas` | Namespace registry | Draft (partial) |

> **Important:** Some constructs reference types not yet implemented (e.g., `ObjectId`, `PubKey`). Treat them as reference designs until the runtime catches up.

## Recommended Workflow

1) Start from the minimal examples in `docs/02_tutorial/`.
2) Gradually adopt stdlib constructs as they compile in your environment.
3) For interoperability, keep your namespaces aligned with stdlib naming conventions.

