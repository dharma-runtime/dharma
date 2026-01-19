# DHARMA Onboarding (REPL-First)

This document defines the intended onboarding experience for DHARMA with the REPL as the default mode. It is written to be implementable against the current codebase while staying explicit about trust boundaries and missing features.

---

## Principles

- **REPL-first by default**: running `dh` should open the REPL.
- **Identity is the first action**: no other workflow makes sense until identity is created/unlocked.
- **Trust is explicit**: every identity, feed, and domain must show its verification state.
- **Guided loop**: the first five commands should produce visible, provable state changes.
- **No silent failure**: every error should explain *why* and *how to fix*.

---

## Boot States (REPL Startup)

On startup, REPL inspects identity state and branches:

### A) UNINITIALIZED
```
Welcome to DHARMA.
You don’t have an identity yet. Let’s create one.
```
Wizard flow:
1) **Handle**  
   Prompt for a global handle.  
   Note: this handle is **UNVERIFIED** until registered.
2) **Password**  
   Prompt + confirm; used for keystore encryption (Argon2).
3) **Key generation**  
   Create root key + device key.
4) **Identity subject**  
   Create `core.genesis` assertion.
5) **Status summary**  
   - Identity: `handle@local` (UNVERIFIED)  
   - Subject ID  
   - Device key ID

Then:
```
Would you like to:
1) Install std contracts
2) Create your first subject
3) Subscribe to public feeds
```

### B) LOCKED
```
Identity exists. Unlock to continue.
Password:
```

### C) READY
```
DHARMA REPL vX
Identity: handle@local (UNVERIFIED)
Type 'help' for commands.
```

Prompt format (always visible):
```
dh [handle@local] (unverified) >
```

---

## Trust States (Always Visible)

All entities should display a trust state:

- **VERIFIED**: trust chain anchored to a known root.
- **UNVERIFIED**: locally created, not registered.
- **SIMULATED**: local fixtures or demo data.
- **UNKNOWN**: unrecognized publisher or missing verification data.

These states should be shown in:
- `identity status`
- `subscribe` output
- `domain list`
- `subject info`
- query results that include provenance

---

## The First Loop (Guided Flow)

### 1) Install standard contracts
```
> setup std
Installed: std.task, std.note, std.iam, std.atlas
```

### 2) Create first subject
```
> subject create std.task as household.tasks
Created: 7f3a...c012 (alias household.tasks)
```

### 3) Create first assertion
```
> use household.tasks
> commit action Create(title="Buy milk")
> state
```

### 4) Explainability
```
> why status
> tail 5
```

---

## Public Feeds (Verified Domains)

REPL should support subscribing to public verified feeds:

```
> subscribe fx.rates
Source: fx.rates
Publisher: <subject>
Trust: VERIFIED
```

If not implemented yet, it must say:
```
Trust: SIMULATED / UNVERIFIED
```

Expected public feeds (initial list):
- `fx.rates`
- `reuters.world`
- `us.bls.cpi`

---

## Sovereign Domains (User-Owned)

Users should be able to create sovereign domains they control:

```
> domain create household
Domain: household (SOVEREIGN)
Root: <your root key>
```

This should:
- create a domain root subject
- set namespace policy
- set overlay policy defaults

---

## First DHL Development Loop

The minimal path to custom laws:

```
> contract new household.task
> contract edit household.task
> contract compile household.task
> subject create household.task as household.tasks
```

Then:
```
> dryrun action Assign(who=@alice)
> commit action Assign(who=@alice)
```

Failures should reference the failing rule (ideally line-numbered).

---

## Required REPL Commands (Minimum Set)

These are required to support the onboarding flow:

- `identity status | init | unlock | lock | whoami`
- `setup std`
- `subject create <contract> as <alias>`
- `use <subject|alias>`
- `commit action <Action>(...)`
- `dryrun action <Action>(...)`
- `state`, `tail`, `why`, `prove`
- `subscribe <feed>`
- `domain create <name>`

---

## Implementation Notes (Current Gaps)

- Verified domains and feeds are **not implemented**. Use **simulated fixtures** until registry + capabilities exist.
- Contract compilation already exists via `dh compile`; it needs a REPL alias and tighter integration.
- Trust state display must be added to REPL prompt and status commands.
- REPL should become the default entrypoint (no-args `dh`).

---

## Success Criteria

On a fresh clone, a new developer should be able to:
1) Create identity (wizard)
2) Install std contracts
3) Create a subject and commit an action
4) Query and explain state
5) Subscribe to a public feed (or simulated)

Target time: **< 10 minutes**.
