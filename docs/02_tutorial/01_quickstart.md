# Quickstart: 15 Minutes to Truth (REPL-First)

This guide takes you from zero to a working DHARMA workflow **using the REPL** and a minimal, compilable DHL contract.

> Note: The REPL is launched via `dh`.

---

## 1) Verify the CLI

Assuming the `dh` binary is in your PATH, start the REPL:

`dh`

Inside the REPL:

```dh-repl
> version
DHARMA REPL v0.1.0
```

---

## 2) Initialize Your Identity

DHARMA requires a local identity before you can sign assertions.

```dh-repl
> identity init julien
Password: ********
Created identity.
```

Verify it:

```dh-repl
> identity whoami
```

---

## 3) Create a Minimal Contract

Create a file named `demo.task.dhl`:

````markdown
---
namespace: demo.task
version: 1.0.0
---

```dhl
aggregate Task
    state
        public title: Text(len=128)
        public status: Enum(Open, Done) = 'Open

action Create(title: Text)
    apply
        state.title = title

action Complete()
    validate
        state.status == 'Open
    apply
        state.status = 'Done
```
````

Compile it:

```bash
dh compile demo.task.dhl
```

This writes artifacts into `~/.dharma/data/objects/` (or your configured `storage.path`) and updates `dharma.toml` with the schema/contract IDs for version 1.

---

## 4) Create a New Subject

DHARMA does not yet have a "subject create" command. Generate a random subject ID and alias it:

```bash
python3 - <<'PY'
import os, binascii
print(binascii.hexlify(os.urandom(32)).decode())
PY
```

In the REPL:

```dh-repl
> alias set demo.task <SUBJECT_ID>
> use demo.task
```

---

## 5) Commit Your First Assertion

```dh-repl
> commit action Create title=Buy_milk
Committed assertions:
  <ASSERTION_ID>
```

> Arg parsing is whitespace-based today; values cannot contain spaces unless you encode them (e.g., `Buy_milk`).

View derived state:

```dh-repl
> state --json
```

---

## 6) Audit the History

```dh-repl
> tail 5
> show <ASSERTION_ID>
> prove <ASSERTION_ID>
```

---

## Next Step

Continue with [Your First Domain Package](02_first_package.md).
