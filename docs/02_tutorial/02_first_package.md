# Your First Domain Package

This guide builds a **Shipment Tracking** contract using *only* features supported by the current compiler/runtime.

---

## 1) Create the Package File

Create a file named `logistics.dhl`. In DHARMA, documentation **is** code.

````markdown
---
namespace: com.ph.logistics
version: 1.0.0
---

```dhl
aggregate Shipment
    state
        public status: Enum(Pending, Shipped, Delivered) = 'Pending
        public courier: Identity
        public history: List<Text(len=64)>

    invariant
        len(state.history) >= 0

action Dispatch(courier: Identity)
    validate
        state.status == 'Pending
    apply
        state.status = 'Shipped
        state.courier = courier
        state.history.push("Dispatched")

action MarkDelivered()
    validate
        state.status == 'Shipped
    apply
        state.status = 'Delivered
        state.history.push("Delivered")
```
````

> Notes on current limitations:
> - `len(...)` works on text, lists, and maps (paths or literal collections).
> - Text literals work in list/map mutations and text equality checks; direct `state.field = "text"` is currently a stub.
> - `has_role(...)` is parsed but currently stubbed in the runtime.
> - GeoPoint literals are not supported in DHL; action args accept `lat,lon` integers.

---

## 2) Compile the Package

```bash
dh compile logistics.dhl
```

This writes (under `.dharma/contracts/`):
- `.dharma/contracts/logistics.schema`
- `.dharma/contracts/logistics.contract`
- `.dharma/contracts/logistics.reactor`

And updates `dharma.toml` so the REPL uses version `1` for actions.

---

## 3) Use the Package in the REPL

Generate a subject ID and alias it:

```bash
python3 - <<'PY'
import os, binascii
print(binascii.hexlify(os.urandom(32)).decode())
PY
```

In the REPL:

```dh-repl
> alias set logistics.shipment <SUBJECT_ID>
> use logistics.shipment
> commit action Dispatch courier=<IDENTITY_HEX>
> state --json
```

> `Identity` arguments are 32-byte hex values. You can get your identity key from `identity whoami`.

---

## Summary

You have:
1) Defined a schema (`Shipment`).
2) Enforced invariants and validation rules.
3) Compiled the contract to Wasm.
4) Executed actions in the REPL.

Next: explore **versioning** and **lenses** in [Versioning & Iteration](03_versioning.md).
