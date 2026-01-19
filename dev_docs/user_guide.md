DHARMA REPL v1 — User Guide

A practical operator manual for home → enterprise → high assurance

1) The mental model (read this once)

DHARMA is not a server you query. It’s a truth machine you replay.
	•	Subjects are independent “truth spaces” (an invoice, a ledger, a case file).
	•	Assertions are append-only signed events: “I assert this happened.”
	•	Derived State is what you see: computed deterministically by replaying accepted assertions through a contract lens.
	•	Lenses (data_ver) are interpreters: the same subject may have multiple parallel interpretations (v1 vs v2).
	•	Pending means: “I can’t decide yet because something is missing.”
	•	Rejected means: “This is invalid; it will never be accepted under this lens.”

If you remember one thing:

Nothing is true unless it’s an accepted assertion under a chosen lens.

⸻

2) First launch: identity, safety, and readiness

2.1 Start the REPL

dh

You’ll see a banner. There are only three possible starting states:

A) UNINITIALIZED (fresh install)
You have no identity. You can browse public subjects if you have keys/links, but you can’t sign.

Do:

identity init julien

B) LOCKED (identity exists, key encrypted)
You can inspect some local decrypted subjects (if keys are stored), but you can’t sign new assertions.

Do:

identity unlock

C) UNLOCKED (ready)
You can sign actions and participate normally.

Check:

identity whoami
status

2.2 High-assurance default (recommended)

If you want the “CIA-grade” behavior even at home (safer):

:set profile highsec

This makes:
	•	dry-run the default
	•	commit requires explicit confirmation
	•	more verbose authority explanations
	•	fewer “helpful guesses”

⸻

3) Your first 10 commands (the core loop)

These ten are the heart of daily use:
	1.	subjects — see what you have locally
	2.	use <subject> — select the thing you’re working on
	3.	lens — see the active interpreter/version
	4.	state — show current derived truth
	5.	tail 20 — show recent accepted assertions
	6.	pending — show what can’t be decided yet
	7.	rejected — show what was invalid
	8.	why <field> — explain why a state value is true
	9.	dryrun action ... — simulate a mutation
	10.	commit action ... — actually append truth

If you only ever learn these, you’re functional.

⸻

4) Navigating subjects like a filesystem

4.1 List subjects

subjects
subjects recent
subjects mine

	•	subjects shows everything in your local store.
	•	recent is the default most people want.
	•	mine filters to subjects you authored or own.

4.2 Use a subject

use 7f3a...c012
pwd

pwd prints your current context (subject + lens + overlays).

4.3 Aliases (so normal humans don’t paste hex)

alias set home.ledger 7f3a...c012
alias set cmdv.ap.inbox 9b11...aa20
alias list
use home.ledger


⸻

5) Lenses (versioned interpreters)

5.1 What is a lens?

A lens is “which contract version interprets the data right now.”

Even if you don’t care about versioning today, a lens is how DHARMA stays stable for decades.

5.2 View and set lens

lens
lens list
lens set 1

5.3 When you change lens

You are not changing history. You are changing the interpretation.

Example:
	•	Lens 1 might compute a ledger total one way.
	•	Lens 2 might include a tax or new rules.

Use:

diff --lens 1 --lens 2


⸻

6) Inspecting truth

6.1 Derived state

state
state --json
state --at <tip_object_id>

	•	state uses snapshot + replay.
	•	--at time-travels (perfect for audits).

6.2 Timeline

tail 10
log 50
show <object_id>

	•	tail is compact summaries.
	•	log is verbose.
	•	show prints the full decoded assertion (header/body/signature status).

6.3 Status dashboard

status
status --verbose

Verbose should show:
	•	accepted/pending/rejected counts
	•	missing dependencies/artifacts
	•	missing lens versions
	•	frontier tips
	•	snapshot position

⸻

7) The “Explain” superpower (auditing & safety)

7.1 Why is this field true?

why status
why balance["Food"]
why invoice.total

A good why output includes:
	•	current value
	•	minimal proof chain of assertions
	•	authors + types
	•	links to inspect each assertion

7.2 Prove an assertion

prove <object_id>

This is your “truth debugger.” It must say:
	•	canonical CBOR OK?
	•	signature OK?
	•	dependencies present?
	•	schema validation OK?
	•	contract validation OK?
	•	accepted/pending/rejected and why

7.3 Diff state

diff --since "2026-01-01"
diff --at <tipA> <tipB>

Perfect for “what changed since last week?”

⸻

8) Acting safely: dry-run before commit

8.1 Dry-run an action

dryrun action Spend amount=4500 category="Groceries"

Dry-run MUST show:
	•	whether you’re authorized
	•	validate result (pass/fail + reason)
	•	a state diff preview
	•	any implied events (if the contract generates them)

8.2 Commit an action

commit action Spend amount=4500 category="Groceries"

In highsec profile you should see a transaction card:
	•	Subject
	•	Lens
	•	Action + args
	•	Authority proof (why you’re allowed)
	•	Expected state diff
	•	“Will write N assertions”
	•	Confirm: type yes

8.3 Emergency “I know what I’m doing”

commit --force action ...

Only available in pro profile.

⸻

9) Pending and rejected: what to do when things don’t work

9.1 Pending means “missing prerequisites”

Common causes:
	•	missing parent assertion (prev)
	•	missing referenced assertion (refs)
	•	missing schema artifact
	•	missing contract artifact
	•	missing lens version installed

Commands:

pending
prove <pending_id>
sync now

Workflow:
	1.	pending
	2.	prove <id> tells you what is missing
	3.	sync now tries to fetch it
	4.	pending again — it should resolve

9.2 Rejected means “never accepted under this lens”

Common causes:
	•	invalid signature
	•	schema mismatch (wrong field type)
	•	contract rule violated (e.g., paying invoice twice)

Commands:

rejected
prove <rejected_id>

Remedy depends:
	•	signature invalid: data is garbage/malicious/corrupt
	•	schema mismatch: you used wrong action args or wrong lens
	•	contract reject: you need a different sequence of actions

⸻

10) Overlays: public base + private extensions

This is how you can send a standard invoice to a third party without leaking internal PO IDs.

10.1 Overlay status

overlay status
overlay list

You might see:
	•	base-only: ✅
	•	overlays available but locked: 🔒
	•	overlays enabled: ✅ merged

10.2 A common workflow
	•	Outsiders see only: std.invoice.*
	•	Employees see: std.invoice.* + com.cmdv.invoice.*

10.3 Explaining overlay-derived state

why should annotate:
	•	value from base
	•	value from overlay
	•	merge rule

⸻

11) Peers and sync (day-to-day)

11.1 See peers

peers
peers --verbose

11.2 Sync now

sync now
sync subject
sync subject <id>

11.3 Discovery

discover status
discover on
discover off

Home profile: discovery ON by default.
Highsec: discovery OFF by default; manual connect or approved rendezvous only.

⸻

12) Packages (code) and installing the “rules of reality”

This matters once you start distributing stdlib and company logic.

12.1 List packages installed

pkg list
pkg show std.invoice

12.2 Install a package

pkg install std.invoice
pkg install com.ph.cmdv.invoice

12.3 Verify provenance

pkg verify std.invoice

This should show:
	•	publisher identity
	•	signature chain / trusted registry
	•	artifact hashes match

⸻

13) Search (once indexing exists)

Remember: indexes are derived, disposable.

13.1 Build index

index status
index build text
index build vector
index build graph

13.2 Search

find "invoice paid"
vfind "late deliveries last month"
gfind refs <object_id>

Every result should support:
	•	open
	•	why

⸻

14) Real scenario walkthroughs

Scenario A — Home finance: groceries + audit

use home.ledger
state
dryrun action Spend amount=4500 category="Groceries" note="S&R"
commit action Spend amount=4500 category="Groceries" note="S&R"
state
why balance["Groceries"]

Scenario B — Business: approve an invoice with strict rules

use cmdv.ap.invoice.2026.001
state
authority Approve
dryrun action Approve reason="Goods received"
commit action Approve reason="Goods received"
tail 10
why status

Scenario C — Incident response: why is something stuck pending?

use cmdv.case.incident.77
pending
prove <pending_id>
sync now
pending
status --verbose

Scenario D — Compare interpretations (lens 1 vs lens 2)

use home.ledger
diff --lens 1 --lens 2
lens set 2
state


⸻

15) High-assurance operating mode (CIA-style)

Turn it on:

:set profile highsec
:set confirmations on

Guidelines:
	•	Always dryrun before commit
	•	Use authority before actions
	•	Require dual control via contract (if enabled)
	•	Keep discovery off unless approved
	•	Use prove and why as standard steps

⸻

16) Help system

16.1 Built-in help

help
help action
help why
help profile

16.2 Command discovery

help should group commands by category and show examples.

⸻

17) Recommended onboarding path for users

For non-technical home users

Teach:
	•	use, state, dryrun action, commit action, why

Everything else is hidden behind menus/help.

For operators (enterprise)

Teach:
	•	plus prove, pending, diff, authority, pkg verify, sync subject


Appendix A — Full Command Reference (DHARMA REPL v1)

This appendix is the complete command reference for the interactive dh. Commands are grouped by category. For each command you’ll find:
	•	Syntax (with optional flags)
	•	Description
	•	Outputs (what it prints)
	•	Exit / error codes (where relevant)
	•	Examples

Conventions:
	•	<…> = required argument
	•	[…] = optional argument
	•	k=v = key/value argument (strings can be quoted)
	•	--json = machine output (canonical JSON)
	•	--raw = raw CBOR bytes shown as hex/base64 (implementation choice; must be stable)
	•	Object IDs / Subject IDs may be abbreviated (prefix), but REPL MUST disambiguate.

⸻

A.0 Global Meta-Commands

help

Syntax:
help
help <command>
help <category>

Description: Shows command list or detailed help.

Output (pretty):
	•	categories + brief summaries
	•	with <command>: syntax + examples

Examples:

help
help state
help identity


⸻

version

Syntax: version

Description: Prints REPL/runtime build info.

Output:
	•	REPL version
	•	runtime protocol version
	•	enabled features (compiler/repl/indexing)
	•	build hash

⸻

clear

Syntax: clear
Clears the screen.

⸻

exit / quit

Syntax: exit | quit
Exits REPL. If identity is unlocked, SHOULD lock/zeroize secrets.

⸻

:set

Syntax:
:set
:set <key> <value>

Keys (normative):
	•	profile = home|pro|highsec
	•	json = on|off
	•	color = on|off
	•	confirmations = on|off
	•	autosnapshot.every = <N> (integer)
	•	lens.default = <ver>
	•	pager = on|off
	•	time.format = iso|unix|human

Output: Current settings or “OK”.

Examples:

:set profile highsec
:set json on
:set autosnapshot.every 50


⸻

A.1 Identity Commands

identity status

Syntax: identity status [--json]

Description: Shows identity lifecycle status.

Output fields (normative):
	•	state: UNINITIALIZED|LOCKED|UNLOCKED|READONLY
	•	alias (if known)
	•	pubkey (hex)
	•	identity_subject (subject id if any)
	•	key_store_path

Example:

identity status


⸻

identity init

Syntax: identity init <alias> [--force]

Description: Creates a new identity keypair + identity subject; stores encrypted private key.

Interactive behavior (normative):
	•	prompt for passphrase twice
	•	confirm overwrite if identity exists unless --force

Output:
	•	new public key
	•	identity subject id
	•	storage locations

Errors:
	•	E_IDENTITY_EXISTS (unless --force)
	•	E_WEAK_PASSPHRASE (optional policy)

⸻

identity unlock

Syntax: identity unlock [--timeout <secs>] [--json]

Description: Unlocks identity private key into memory.

Interactive:
	•	prompts passphrase if not provided by env/agent

Output:
	•	OK + alias/pubkey
	•	optional timeout

Errors:
	•	E_BAD_PASSPHRASE
	•	E_UNINITIALIZED

⸻

identity lock

Syntax: identity lock

Description: Zeroizes and unloads private key.

Output: OK

⸻

identity whoami

Syntax: identity whoami [--json]

Description: Prints active identity details.

Output:
	•	alias
	•	pubkey
	•	identity subject id
	•	assurance level (optional): vouched|sovereign|hsm

⸻

identity export

Syntax: identity export [--format hex|mnemonic]

Description: Exports private key material (dangerous). MUST require:
	•	unlocked identity
	•	explicit confirmation prompt
	•	highsec profile MAY disable entirely

Output: Secret in selected format.

Errors: E_DISABLED_BY_POLICY

⸻

A.2 Subject Navigation

subjects

Syntax:
subjects [--json] [--limit <n>] [--sort recent|name|id]
subjects recent [--limit <n>]
subjects mine [--limit <n>]

Description: Lists known subjects in local store.

Output rows (pretty):
	•	subject id (short)
	•	alias (if any)
	•	last activity time
	•	accepted/pending counts (optional)
	•	keys available (yes/no)

⸻

use

Syntax: use <subject_id_or_alias> [--json]

Description: Sets current subject context.

Output: prints new context line (subject + lens).

Errors: E_SUBJECT_NOT_FOUND

⸻

pwd

Syntax: pwd [--json]

Description: Prints current context:
	•	subject id
	•	alias
	•	lens
	•	overlay status
	•	active package bindings

⸻

alias

Syntax:
alias set <name> <subject_id>
alias rm <name>
alias list [--json]

Output: OK or list.

⸻

subject create

Syntax: subject create [--type <typ>] [--title <text>] [--json]

Description: Creates a new subject with core.genesis using current default schema/contract (or a chosen pack). If your system requires explicit package choice, REPL SHOULD prompt.

Output:
	•	new subject id
	•	genesis object id

Errors: E_LOCKED if needs signing.

⸻

A.3 Lens / Versioning

lens

Syntax: lens [--json]

Description: Shows current lens settings for current subject.

Output fields:
	•	current lens id (data_ver)
	•	installed lenses available
	•	missing lenses referenced by data (if any)

⸻

lens list

Syntax: lens list [--json]

Description: Lists installed lenses for current subject/package.

⸻

lens set

Syntax: lens set <data_ver> [--json]

Description: Sets current lens for state/validation preview. MUST NOT rewrite history.

Errors:
	•	E_LENS_NOT_INSTALLED

⸻

A.4 State & History

state

Syntax:
state [--json] [--raw]
state --at <assertion_id>
state --lens <data_ver>

Description: Computes and displays derived state (snapshot + replay).

Output:
	•	pretty: formatted view of contract-defined state
	•	json: canonical JSON of state CBOR

Errors:
	•	E_NO_KEYS (cannot decrypt)
	•	E_LENS_NOT_INSTALLED
	•	E_CONTRACT_MISSING
	•	E_SCHEMA_MISSING

⸻

tail

Syntax: tail [n] [--json] [--accepted|--pending|--rejected]

Default: n=20, --accepted

Output columns (accepted):
	•	seq (author or logical)
	•	typ
	•	author (short)
	•	time claim
	•	object id (short)
	•	summary (contract-provided optional)

⸻

log

Syntax:
log [n] [--json] [--accepted|--pending|--rejected] [--since <time>] [--until <time>]

Verbose history with more header details.

⸻

show

Syntax: show <object_id> [--json] [--raw]

Displays decoded assertion (if decryptable):
	•	header fields
	•	body
	•	signature bytes (optional)
	•	schema/contract ids

If not decryptable:
	•	envelope metadata only.

Errors: E_OBJECT_NOT_FOUND

⸻

status

Syntax: status [--json] [--verbose]

Description: Subject health report.

Verbose includes:
	•	frontier tips
	•	snapshot status per lens
	•	missing deps/artifacts
	•	counts accepted/pending/rejected
	•	overlay status

⸻

A.5 Explain / Audit

why

Syntax: why <state_path> [--json]

Description: Explains why a state field has its value.

Output (normative):
	•	current value
	•	minimal proof chain: list of assertions that caused it
	•	each includes: object id, typ, author, refs
	•	optionally: derived rule (contract explanation string)

Errors: E_PATH_NOT_FOUND

Examples:

why status
why balance["Food"]


⸻

prove

Syntax: prove <object_id> [--json]

Description: Full validation report.

Output fields (normative):
	•	canonical decode: ok/fail
	•	signature verify: ok/fail
	•	deps present/missing
	•	schema validation: ok/fail + errors
	•	contract validation: accept/reject/pending + reason
	•	final status: ACCEPTED/PENDING/REJECTED

⸻

authority

Syntax:
authority <ActionName> [k=v ...] [--json]
authority typ <typ> [--json]

Description: Explains whether current identity is authorized, and why.

Output:
	•	allowed: true/false
	•	required capability/role/quorum
	•	evidence assertions (ids) proving grant/delegation
	•	failure reason if denied

⸻

diff

Syntax:
diff --at <tipA> <tipB> [--json]
diff --since <time> [--until <time>]
diff --lens <verA> --lens <verB>

Description: Shows state differences:
	•	between two tips
	•	between time ranges
	•	between lens interpretations

Output:
	•	pretty: added/removed/changed fields
	•	json: structured diff

⸻

A.6 Acting (Dry-run & Commit)

dryrun action

Syntax: dryrun action <ActionName> [k=v ...] [--json]

Description: Simulates validate+reduce without writing.

Output (normative):
	•	authority result
	•	validation result (pass/fail + reason)
	•	preview diff
	•	would-write count (0 or N if split assertions)
	•	notes about overlays/lens routing

Errors:
	•	E_UNAUTHORIZED
	•	E_SCHEMA_MISMATCH
	•	E_CONTRACT_REJECT

⸻

commit action

Syntax: commit action <ActionName> [k=v ...] [--json] [--force]

Description: Executes action and writes resulting assertion(s). --force bypasses confirmation (disabled in highsec).

Output:
	•	committed object_id(s)
	•	updated frontier
	•	snapshot update note (if created)

Errors:
	•	same as dryrun, plus storage errors:
	•	E_IO
	•	E_FSYNC (if policy requires)

⸻

dryrun emit

Syntax: dryrun emit <typ> <json_or_cbor> [--json]

Low-level simulation: bypasses action mapping and emits raw typ/body.

⸻

commit emit

Syntax: commit emit <typ> <json_or_cbor> [--json] [--force]

Writes a raw assertion.

⸻

tx

Syntax:
tx begin
tx show
tx commit
tx abort

Description: Optional transaction staging in REPL:
	•	stage multiple actions/emit
	•	then commit as a batch (still append-only, but can emit a “batch envelope” or sequential assertions)

Highsec profile MAY require tx for certain operations.

⸻

A.7 Overlays (Public/Base + Private Extensions)

overlay status

Syntax: overlay status [--json]

Shows:
	•	overlays present
	•	overlays enabled/disabled
	•	key availability (decryptable?)

⸻

overlay list

Syntax: overlay list [--json]

Lists overlay namespaces for current subject.

⸻

overlay enable

Syntax: overlay enable <namespace>

Enables merge of overlay state into derived state view.

Errors: E_NO_KEYS

⸻

overlay disable

Syntax: overlay disable <namespace>

⸻

overlay show

Syntax: overlay show <namespace> [--tail <n>]

Shows overlay assertion history for that namespace.

⸻

A.8 Peers & Sync

peers

Syntax: peers [--json] [--verbose]

Output fields:
	•	peer id (pubkey/subject)
	•	addr
	•	status (connected/disconnected)
	•	last_seen
	•	trust level (optional)
	•	relay role (optional)

⸻

peer trust

Syntax: peer trust <peer_id> <low|normal|high>

Stores local trust preference.

⸻

peer ban

Syntax: peer ban <peer_id> [--duration <secs>]

Client-side ban (stop syncing / drop connections).

⸻

sync now

Syntax: sync now [--json]

Triggers immediate INV exchange and sync cycle.

⸻

sync subject

Syntax: sync subject [<subject_id>]

If subject omitted, sync current subject only.

⸻

sync pause / sync resume

Pauses/resumes background sync loop (if daemon embedded).

⸻

connect

Syntax: connect <ip:port>

Manual connection from REPL (if not using dh connect outside).

⸻

listen

Syntax: listen [--port <p>]

Starts listening server (REPL embedded mode). Usually dh serve handles this; REPL may delegate.

⸻

discover

Syntax:
discover status
discover on
discover off

Controls UDP beacon discovery (LAN).

⸻

A.9 Packages / Code / Registry

pkg list

Syntax: pkg list [--json]

Lists installed packages:
	•	name
	•	available data versions
	•	artifact hashes
	•	trust status

⸻

pkg show

Syntax: pkg show <name> [--json]

Shows:
	•	all installed lenses (data_ver → schema+contract)
	•	dependencies pinned
	•	publishers/trust chain

⸻

pkg verify

Syntax: pkg verify <name> [--json]

Verifies:
	•	hashes
	•	publisher signatures
	•	dependency integrity

⸻

pkg install

Syntax: pkg install <name> [--from <registry_subject>]

Fetches from configured registry subject(s).

⸻

pkg pin

Syntax: pkg pin <name> <artifact_hash>

Pins to exact artifact hash (high assurance).

⸻

pkg remove

Syntax: pkg remove <name> [--keep-cache]

Removes active mapping; may keep artifacts cached.

⸻

A.10 Indexing & Search (Derived Views)

index status

Syntax: index status [--json]

Shows:
	•	enabled indexes (text/vector/graph)
	•	last build time
	•	size
	•	coverage (subjects indexed)

⸻

index build

Syntax:
index build text [--scope current|all]
index build vector [--scope current|all] [--model <name>]
index build graph [--scope current|all]

Builds derived indexes.

⸻

index drop

Syntax: index drop <text|vector|graph> [--scope current|all]

Drops derived index.

⸻

find

Syntax: find "<query>" [--limit <n>] [--json]

Full-text search across indexed data.

⸻

vfind

Syntax: vfind "<query>" [--limit <n>] [--json]

Vector semantic search.

⸻

gfind

Syntax:
gfind refs <object_id>
gfind deps <object_id>
gfind path <a> <b>

Graph/provenance navigation.

⸻

open

Syntax: open <result_id_or_object_id>

Opens item in a detailed view (like show, but friendlier).

⸻

A.11 Diagnostics & Maintenance

check

Syntax: check [--deep] [--json]

Runs local consistency checks:
	•	canonical CBOR checks (deep)
	•	missing objects referenced
	•	frontier correctness
	•	snapshot consistency

⸻

gc

Syntax: gc [--policy <name>] [--dryrun]

Garbage collection of derived artifacts and optionally old objects under policy constraints. MUST NOT delete required objects silently; highsec profile typically disables destructive GC unless retention allows.

⸻

snapshot

Syntax:
snapshot list [--lens <ver>]
snapshot make [--lens <ver>]
snapshot prune [--keep <n>]

⸻

export

Syntax:
export subject <subject_id> --out <file>
export proof <object_id> --out <file>

Exports portable bundles (useful for air-gapped transfer).

⸻

import

Syntax: import <file>

Imports object bundles.

⸻

A.12 Output formats (normative)

Every command that supports --json MUST output a single JSON object with:
	•	ok: boolean
	•	result: payload (object/array)
	•	warnings: optional array
	•	error: optional object { code, message, detail }

Example (prove):

{
  "ok": true,
  "result": {
    "object_id": "9af3...",
    "canonical": "ok",
    "signature": "ok",
    "deps_missing": [],
    "schema": { "status": "ok" },
    "contract": { "status": "accept", "reason": null },
    "final": "ACCEPTED"
  }
}

Pretty output may be richer but MUST be derivable from the JSON payload.

⸻

A.13 Standard error codes (minimum set)
	•	E_UNINITIALIZED
	•	E_LOCKED
	•	E_BAD_PASSPHRASE
	•	E_NO_KEYS
	•	E_SUBJECT_NOT_FOUND
	•	E_OBJECT_NOT_FOUND
	•	E_CANONICAL_FAIL
	•	E_SIG_INVALID
	•	E_MISSING_DEP
	•	E_SCHEMA_MISSING
	•	E_CONTRACT_MISSING
	•	E_VERSION_UNSUPPORTED
	•	E_UNAUTHORIZED
	•	E_CONTRACT_REJECT
	•	E_IO
	•	E_POLICY_DENY
	•	E_DISABLED_BY_PROFILE


# Golden Transcripts

Golden Transcript 01 — First Run + Identity Init + First Subject + First Action (Home)

$ dh

DHARMA REPL v1.0
Kernel: DHARMA Runtime v1  Suite: 1  Storage: filesystem (append-only)
Data dir: ./data
Profile: home  JSON: off  Color: on
Status: UNINITIALIZED (no identity found)
Tip: run `identity init <alias>` to create your identity.

anon@dh [no-subject] [lens:?] [peers:0] >

help
Commands (home):
  identity init|unlock|whoami|status
  subjects|use|alias
  state|tail|pending|rejected|show
  dryrun action ... | commit action ...
  why|prove|diff
  peers|sync now
  :set profile|json|color
Type: help <command> for details.

anon@dh [no-subject] [lens:?] [peers:0] >

identity init julien
Creating identity:
  alias: julien
  key type: Ed25519 (Suite 1)
Enter passphrase: ********
Confirm passphrase: ********

✅ Identity created.
  alias: julien
  pubkey: ed25519: 8b4d9c2f0f9a1d7e6a19e7b3b5f3c8d4c2a1e9f0b7d2a3c4e5f6a7b8c9d0e1f2
  identity subject: 3f6a4b1c9d0e2f3a4b5c6d7e8f90123456789abcdeffedcba9876543210a1b2c3
Stored:
  ~/.dharma/data/identity.key (encrypted)
  ~/.dharma/data/subjects/3f6a.../objects (created)

julien@dh [no-subject] [lens:1] [peers:0] >

identity whoami
Identity:
  alias: julien
  pubkey: 8b4d…e1f2
  identity subject: 3f6a…b2c3
  state: UNLOCKED
  assurance: sovereign (local key)

julien@dh [no-subject] [lens:1] [peers:0] >

subjects
No subjects found (besides identity subject).
Tip: create one with `subject create` (or install a pack).

julien@dh [no-subject] [lens:1] [peers:0] >

subject create --type std.home.ledger --title "Home Ledger"
Creating subject...
  subject_id: 9a7c1e2f3b4c5d6e7f80112233445566778899aabbccddeeff00112233445566
  genesis: core.genesis
  schema: std.home.ledger@lens=1 (installed)
  contract: std.home.ledger@lens=1 (installed)

✅ Subject created.
  subject: 9a7c…5566
  genesis object: 0c12a7…9f88

Context switched.
julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:0] >

state
Home Ledger (lens 1)
--------------------------------
currency: PHP
accounts:
  Cash:            0.00
  Bank:            0.00
  Food:            0.00
  Utilities:       0.00
  Entertainment:   0.00
recent:
  (none)

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:0] >

dryrun action Income amount=250000 account="Bank" note="Salary"
Dry-run: Income
  subject: 9a7c…5566
  lens: 1
  signer: julien (8b4d…e1f2)
  authorization: ✅ allowed (owner)

Schema check: ✅ ok
Contract validate: ✅ accept
Would write: 1 assertion
Preview state diff:
  accounts.Bank: 0.00  -> 250000.00
  recent += Income(amount=250000, account=Bank)

No changes committed.

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:0] >

commit action Income amount=250000 account="Bank" note="Salary"
Commit: Income
✅ Committed.
  object: 14be33…c901
  typ: std.home.ledger.income
  refs: []
  ts: 1736923105000000 (claim)

Snapshot policy: every 50 commits → not due (seq=1).
Broadcast: peers=0 (no sync)

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:0] >

state
Home Ledger (lens 1)
--------------------------------
currency: PHP
accounts:
  Cash:            0.00
  Bank:       250000.00
  Food:            0.00
  Utilities:       0.00
  Entertainment:   0.00
recent:
  1) Income  PHP 250000.00  -> Bank  ("Salary")

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:0] >

why accounts.Bank
WHY accounts.Bank = 250000.00
Proof chain (minimal):
  1) 14be33…c901  std.home.ledger.income
     author: julien (8b4d…e1f2)
     body: { amount: 250000, account: "Bank", note: "Salary" }
     accepted by: contract std.home.ledger@lens=1
To inspect: show 14be33…c901


⸻

Golden Transcript 02 — Two Devices Sync + Seeing Replication Happen

# Terminal A (Laptop)
$ dh
DHARMA REPL v1.0  Profile: home
Status: UNLOCKED (julien)
Discovery: ON (LAN beacon)
Listening: tcp://0.0.0.0:3000

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:0] >

peers
No peers connected.

# Terminal B (Desktop)
$ dh
DHARMA REPL v1.0  Profile: home
Status: UNLOCKED (julien)
Discovery: ON (LAN beacon)
Listening: tcp://0.0.0.0:3000

julien@dh [no-subject] [lens:1] [peers:0] >

discover status
Discovery: ON
Beacon: udp://0.0.0.0:3001 broadcast every 3s
Known LAN peers: 0

# A few seconds pass…

[DISCOVER] Found peer signal:
  addr: 192.168.1.23:3000
  peer_id: 8b4d…e1f2 (julien)
  note: same identity detected; treating as “my device”
[SYNC] Auto-connecting to 192.168.1.23:3000 …

[NET] Handshake: OK (suite 1)
[SYNC] Exchanging INV/GET/OBJ …
[SYNC] Completed:
  objects received: 148
  subjects updated: 2
  pending resolved: 0

julien@dh [no-subject] [lens:1] [peers:1] >

subjects recent
1) home.ledger        9a7c…5566   updated: 10s ago
2) identity.julien    3f6a…b2c3   updated: 1m ago

use home.ledger
Context switched: sub 9a7c…5566

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:1] >

state
Home Ledger (lens 1)
--------------------------------
currency: PHP
accounts:
  Bank: 250000.00
recent:
  1) Income  PHP 250000.00  -> Bank  ("Salary")

tail 5
ACCEPTED (last 1)
  #1  14be33…c901  std.home.ledger.income   author: julien  note:"Salary"


⸻

Golden Transcript 03 — Pending Dependencies + Auto-Resolution via Sync

$ dh
DHARMA REPL v1.0  Profile: pro
Status: UNLOCKED (julien)
Listening: tcp://0.0.0.0:3000

julien@dh [sub:cmdv.case.77=aa11…ff90] [lens:1] [peers:2] >

status --verbose
Subject: cmdv.case.77 (aa11…ff90)
Lens: 1
Accepted: 42
Pending: 3
Rejected: 1
Frontier tips: 2
Snapshots: present (lens 1 at seq=40 tip=bb02…19a0)
Missing deps:
  - 77c9…e012 (prev missing for 9f10…aa33)
  - schema artifact: 55aa…beef (needed by 1c2d…3e4f)

pending
PENDING assertions (3)
1) 9f10…aa33  typ=std.case.note.add
   reason: E_MISSING_DEP prev=77c9…e012
2) 1c2d…3e4f  typ=std.case.evidence.link
   reason: E_SCHEMA_MISSING schema=55aa…beef
3) 44dd…1122  typ=std.case.task.assign
   reason: E_MISSING_DEP ref=0ab1…cafe

prove 1c2d…3e4f
PROVE 1c2d…3e4f
  canonical: ✅ ok
  signature: ✅ ok (auth=1b22…c0de)
  deps: ✅ ok
  schema: ❌ missing artifact 55aa…beef
  contract: ⏸ pending (schema missing)
  final: PENDING
Next steps:
  - sync now
  - or install package containing schema 55aa…beef

sync now
[SYNC] Starting…
[SYNC] INV received from peer 71.56.10.9:3000
[SYNC] GET queued: 77c9…e012, 55aa…beef, 0ab1…cafe
[SYNC] OBJ received: 3
[INGEST] 77c9…e012 → committed
[INGEST] 55aa…beef (artifact) → stored
[INGEST] 0ab1…cafe → committed
[REPLAY] Re-evaluating pending set…
  1c2d…3e4f: schema now present → ACCEPTED
  9f10…aa33: prev now present → ACCEPTED
  44dd…1122: ref now present → ACCEPTED
[SYNC] Completed.

pending
No pending assertions.

status
Subject: cmdv.case.77
Accepted: 45  Pending: 0  Rejected: 1  Tips: 1


⸻

Golden Transcript 04 — Highsec Workflow: Dry-run → Transaction Card → Commit → Audit Proof

$ dh
DHARMA REPL v1.0  Profile: highsec
Status: LOCKED (identity present)
Policy: fail-closed, confirmations required, discovery OFF
Tip: run `identity unlock`

anon@dh [no-subject] [lens:1] [peers:0] >

identity unlock
Enter passphrase: ********
✅ Identity unlocked: julien (8b4d…e1f2)

julien@dh [no-subject] [lens:1] [peers:0] >

use cmdv.ap.invoice.2026.001
Context switched: sub 0f0f…d00d (alias: cmdv.ap.invoice.2026.001)

julien@dh [sub:cmdv.ap.invoice.2026.001=0f0f…d00d] [lens:1] [peers:0] >

state
Invoice (lens 1)
--------------------------------
id: INV-2026-001
vendor: SAFF
amount: PHP 889.00
status: PendingApproval
approvals:
  - requested_by: Alfred
  - required: Accountant + OpsManager
evidence:
  - pdf: present
  - delivery_receipt: present

authority Approve role="Accountant"
Authority check: Approve(role=Accountant)
  allowed: ✅ yes
  basis:
    - role.assign(Accountant) -> julien (assertion 77aa…1001)
    - subject policy: requires Accountant + OpsManager (contract rule)
  note: this action will still require OpsManager co-approval for final status change.

dryrun action Approve role="Accountant" reason="Verified vendor billing"
Dry-run: Approve
  schema: ✅ ok
  contract.validate: ✅ accept
  effect: “Accountant approval recorded”
Preview diff:
  approvals += { role: Accountant, by: julien, reason: "Verified vendor billing" }
  status remains: PendingApproval (awaiting OpsManager)

No changes committed.

commit action Approve role="Accountant" reason="Verified vendor billing"
────────────────────────────────────────────────────────
HIGHSEC TRANSACTION CARD (REVIEW REQUIRED)
Subject:    cmdv.ap.invoice.2026.001 (0f0f…d00d)
Lens:       1
Signer:     julien (8b4d…e1f2)
Action:     Approve(role=Accountant)
Args:       reason="Verified vendor billing"
Authority:  ✅ role.assign(Accountant) (77aa…1001)
Policy:     Requires co-approval OpsManager to finalize
Writes:     1 assertion
Preview:
  approvals += Accountant(julien)
  status unchanged (awaiting OpsManager)
Type `yes` to commit, anything else to abort:
────────────────────────────────────────────────────────
> yes

✅ Committed.
  object: a1b2…c3d4
  typ: cmdv.invoice.approval.add
  refs: [invoice.request…]
Snapshot: not due (seq=17)

prove a1b2…c3d4
PROVE a1b2…c3d4
  canonical: ✅ ok
  signature: ✅ ok (auth=julien)
  deps: ✅ ok
  schema: ✅ ok
  contract: ✅ accept
  final: ACCEPTED

why status
WHY status = PendingApproval
Proof chain (minimal):
  1) invoice.create … (accepted)
  2) approval.request … (accepted)
  3) approval.add(role=Accountant) a1b2…c3d4 (accepted)
Contract explanation:
  “Final status becomes Approved only after roles Accountant + OpsManager both present.”


⸻

Golden Transcript 05 — Overlays: Sending Standard Invoice to 3rd Party Without Private Fields

This shows the base+overlay concept and how the REPL makes it obvious.

$ dh
DHARMA REPL v1.0  Profile: pro
Status: UNLOCKED (julien)

julien@dh [sub:cmdv.invoice.2026.001=0f0f…d00d] [lens:1] [peers:1] >

overlay status
Overlay model: ENABLED
Base namespace: std.invoice
Overlays present (1):
  - com.ph.cmdv.invoice   (decryptable ✅)  chain: 5 assertions
Sharing policy:
  - base assertions MAY be shared to external peers if allowed
  - overlay assertions NEVER shared unless peer has overlay keys

state
Invoice (merged view)
--------------------------------
std.invoice:
  id: INV-2026-001
  vendor: SAFF
  amount: PHP 889.00
  status: PendingApproval
com.ph.cmdv.invoice (private overlay):
  internal_po_id: PO-8137-2026
  department: OPS
  margin_bucket: “thin”
  notes_internal: “match clearing fees”

# External peer (supplier) connects, they only subscribe to std.invoice
peers --verbose
Peers (1)
1) peer: saff.fr (pubkey 99aa…77cc)
   addr: 81.12.4.9:3000
   permissions: base-only (std.invoice)  overlay: denied (no keys)
   sync mode: active

sync subject
[SYNC] Subject cmdv.invoice.2026.001
[SYNC] Applying disclosure policy:
  sending base: 18 objects
  sending overlays: 0 objects (peer lacks com.ph.cmdv.invoice keys)
[SYNC] Completed.

# Now we inspect what the external peer would see (REPL simulation)
export proof subject 0f0f…d00d --as external --out /tmp/inv_export.dharmabundle
Export created:
  mode: external(base-only)
  objects: 18
  overlays: 0
  file: /tmp/inv_export.dharmabundle

# Sanity: show that PO id is not in exported view
open /tmp/inv_export.dharmabundle
Bundle view (external/base-only):
  std.invoice fields present ✅
  com.ph.cmdv.invoice fields present ❌
  integrity: ✅ signatures intact


⸻

Golden Transcript 06 — Multi-Lens Versioning: Same Subject, Two Interpretations + A/B Emissions

$ dh
DHARMA REPL v1.0  Profile: pro
Status: UNLOCKED (julien)

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:1] >

pkg list
Installed packages:
  std.home.ledger
    lenses: 1, 2
    note: lens 2 introduces “tax buckets” and auto-rounding rules

lens
Current lens: 1
Available: [1,2]

state
Home Ledger (lens 1)
accounts:
  Bank: 250000.00
  Food: 0.00
recent:
  Income Bank 250000.00

# Switch interpretation without changing history
lens set 2
Lens set: 2

state
Home Ledger (lens 2)
accounts:
  Bank: 250000.00
  Food: 0.00
tax_buckets:
  VAT_estimate: 0.00
rounding:
  policy: “cash rounding”
recent:
  Income Bank 250000.00

diff --lens 1 --lens 2
State diff (lens 1 → lens 2)
+ tax_buckets.VAT_estimate = 0.00
+ rounding.policy = "cash rounding"
(no changes to balances)

# Emit a v2 event (data carries version)
dryrun action Spend amount=999.99 category="Food" --data_ver 2
Dry-run: Spend (data_ver=2)
Contract validate: ✅ accept
Preview:
  Food: 0.00 -> 999.99
  VAT_estimate: 0.00 -> 119.99
  rounding applied: (none)
Would write: 1 assertion ver=2

commit action Spend amount=999.99 category="Food" --data_ver 2
✅ Committed object: 55ff…aa11  ver=2

# Now compare how lens 1 sees it
lens set 1
state
Home Ledger (lens 1)
accounts:
  Bank: 250000.00
  Food: 999.99
recent:
  Income …
  Spend Food 999.99

lens set 2
state
Home Ledger (lens 2)
accounts:
  Bank: 250000.00
  Food: 999.99
tax_buckets:
  VAT_estimate: 119.99
recent:
  Income …
  Spend Food 999.99 (vat tracked)


⸻

Golden Transcript 07 — Search + Vector Search + Proofable Results (Derived Indexes)

$ dh
DHARMA REPL v1.0
Status: UNLOCKED (julien)

julien@dh [no-subject] [lens:1] [peers:2] >

index status
Indexes:
  text:   ❌ not built
  vector: ❌ not built
  graph:  ✅ built (refs/deps)

index build text --scope all
Building text index…
  subjects scanned: 48
  decryptable assertions: 12,440
  artifacts included: 220
✅ text index built.
  size: 83.2MB
  build time: 12.4s

index build vector --scope all --model mini-embeddings-v1
Building vector index…
  fields embedded: note.text, invoice.memo, case.summary
  subjects scanned: 48
✅ vector index built.
  vectors: 120,110
  size: 312MB
  build time: 38.9s

find "underpayment 9 pesos"
Results (text) [3]
1) subject: cmdv.ap.invoice.2026.001  obj: a1b2…c3d4  snippet: "remaining 9 pesos..."
2) subject: home.ledger               obj: 91aa…ee02  snippet: "paid later..."
3) subject: case.payments             obj: 11bb…77aa  snippet: "underpayment…"

open 1
Object a1b2…c3d4
typ: cmdv.invoice.approval.add
body: { reason: "Verified vendor billing", note: "remaining 9 pesos ..." }

why reason
WHY reason = "Verified vendor billing"
Proof chain:
  a1b2…c3d4 (this assertion)
(Direct field from body; accepted by contract.)

vfind "payments that were short and later corrected"
Results (vector) [2]
1) subject: case.payments  obj: 11bb…77aa  score: 0.83  summary: "short payment corrected later"
2) subject: home.ledger    obj: 91aa…ee02  score: 0.78  summary: "settled difference afterward"

open 1
show 11bb…77aa
... (prints decoded assertion) ...


⸻

Golden Transcript 08 — Air-Gapped Export/Import + Verify (High Assurance)

$ dh
DHARMA REPL v1.0  Profile: highsec
Status: UNLOCKED (julien)
Discovery: OFF
Network: OFF (airgap mode)

julien@dh [sub:cmdv.case.incident.77=aa11…ff90] [lens:1] [peers:0] >

status
Subject: cmdv.case.incident.77
Accepted: 45  Pending: 0  Rejected: 1
Note: airgap mode → no sync

export subject aa11…ff90 --out /media/usb/incident77.dharmabundle
Preparing export bundle…
Policy: highsec
  include: base assertions + required artifacts + latest snapshot
  exclude: overlays not requested
  include proofs: YES
✅ Export written:
  file: /media/usb/incident77.dharmabundle
  objects: 2,140
  snapshot: lens=1 seq=40
  manifest signature: ✅ (julien)

# Move USB to another machine…

$ dh
DHARMA REPL v1.0  Profile: highsec
Status: UNINITIALIZED (read-only inspection allowed)
Network: OFF

anon@dh [no-subject] [lens:1] [peers:0] >

import /media/usb/incident77.dharmabundle
Importing…
  verifying manifest signature… ✅ ok
  checking object hashes… ✅ ok
  writing objects… ✅ ok
✅ Imported:
  objects added: 2,140
  subjects added: 1

subjects
1) cmdv.case.incident.77  aa11…ff90  updated: just now

use aa11…ff90
state
Incident Case (lens 1)
--------------------------------
severity: HIGH
status: CONTAINED
timeline:
  - initial report …
  - mitigation …
  - closure pending …

prove <some_object_id>
PROVE …
  signature: ✅ ok
  final: ACCEPTED

