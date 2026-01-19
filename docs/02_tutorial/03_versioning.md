# Versioning & Lenses

DHARMA treats **data and logic as versioned artifacts**. The same Subject can be replayed through different contract versions ("lenses").

---

## 1) Data Version = Major Version

The compiler derives the data version from the **major** component of the front-matter `version`.

```
version: 2.1.4
```

This yields `data_ver = 2`.

---

## 2) Compile Multiple Versions

Example:

- `demo.task v1` (status: Open/Done)
- `demo.task v2` (status: Open/InProgress/Done)

Compile both versions:

```bash
dh compile demo.task.v1.dhl
dh compile demo.task.v2.dhl
```

This writes artifacts for each version and updates `dharma.toml` with the schema/contract IDs for each data version.

---

## 3) Use Lenses in the REPL

There is no global `lens set` command yet. Instead, you pass `--lens` on commands.

```dh-repl
> state --lens 1 --json
> state --lens 2 --json
> commit action Create title=Alpha --lens=1
> commit action Create title=Beta --lens=2
```

Other commands also accept `--lens`:

- `why <path> --lens <ver>`
- `diff --at <idA> <idB> --lens <verA> <verB>`

---

## 4) Compatibility Rules (Current)

- Assertions are filtered by `header.ver` during replay.
- If a lens is missing its schema/contract artifacts, replay fails with `missing schema/contract`.

---

## 5) Planned Improvements

- `lens` command to set default lens in REPL.
- Lens negotiation during sync.
- Versioned migration helpers.
