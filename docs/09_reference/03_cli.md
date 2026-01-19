# CLI & REPL Reference (Current)

This document enumerates the **actual commands implemented** in the `dh` CLI.

---

## 1) CLI Commands (`dh`)

```
dh identity init <alias>
dh identity export
dh connect <addr:port>
dh config show
dh compile <file.dhl>
dh test [--deep] [--chaos] [--ci] [--replay SEED=<seed>]
dh serve
dh
```

Notes:
- `dh` with no args starts the REPL.

---

## 2) REPL Commands (`dh`)

```
identity [status|init|unlock|lock|whoami|export]
alias [set|rm|list]
subjects [recent|mine]
use <id|alias>
state [--json|--raw] [--at <id>] [--lens <ver>]
tail [n]
log [n]
show <id> [--json|--raw]
status [--verbose]
dryrun action <Action> [k=v...] [--lens <ver>] [--json]
commit action <Action> [k=v...] [--lens <ver>] [--json] [--force]
why <path> [--lens <ver>]
prove <id>
authority <Action> [k=v...]
diff --at <idA> <idB> [--lens <verA> <verB>]
pkg <list|show|install|verify|pin|remove>
overlay <status|list|enable|disable|show>
peers [--json|--verbose]
connect <addr>
sync now | sync subject [id]
discover [status|on|off]
find "<query>" [--limit n]
q <query pipeline>
pwd
:set <key> <val>
version
exit
```

---

## 3) Profiles

The REPL supports profiles via `:set profile`:

- `home`
- `pro`
- `highsec`

Highsec adds confirmation prompts and safety checks.
