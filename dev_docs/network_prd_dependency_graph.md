# Network PRD v1 — Dependency Graph (Tasks 58–63)

Legend:
- -> hard dependency
- ~> soft/optional dependency

## High-Level
58 (Atlas Identity) -> 59 (Domain Contract) -> 60 (Ownership/Sharing)
59 -> 62 (Key Hierarchy)
60 -> 61 (Permission Summaries)
59 -> 63 (Freeze/Compromise)

## Detailed DAG
58.1 -> 58.2 -> 58.3 -> 58.4

58.1,58.3 -> 59.1 -> 59.2 -> 59.3
59.1,59.2 -> 59.4

59.1,59.2 -> 60.1 -> 60.2 -> 60.3
60.1 -> 60.4

59.1,60.2 -> 61.1 -> 61.2 -> 61.3
60.3 ~> 61.2

59.1 -> 62.1 -> 62.2 -> 62.3
59.2 -> 62.3

59.1 -> 63.1
58.3 -> 63.2
59.1 -> 63.3
62.1 ~> 63.3

## Notes
- Task 58 is foundational (identity + genesis).
- Task 59 defines authoritative domains and membership.
- Task 62 key hierarchy must align with domain membership (no subject-level crypto sharing).
- Task 61 permission summaries are a latency guardrail; not required for correctness but required by PRD.

