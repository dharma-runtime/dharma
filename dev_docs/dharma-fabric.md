DHARMA-FABRIC v1 

Deterministic Sharding + Soft-State Discovery + Targeted Execution

Goal

Make DHARMA “just run” with:
	•	p99-first latency for OLTP queries
	•	predictable scatter/gather for OLAP
	•	safe, capability-gated access
	•	location-agnostic UX (clients never choose servers)
	•	provable correctness (watermarks + provenance + optional receipts)

This spec applies to:
	•	object fetch / subject sync
	•	DHARMA-Q queries
	•	search (text/vector/geo)
	•	compute jobs

⸻

0. Definitions
	•	ShardMap: deterministic function mapping (table,key) or (subject_id) to a shard.
	•	ReplicaSet: set of providers responsible for a shard.
	•	Soft-state Ads: TTL advertisements of endpoints, health, load, watermark.
	•	Watermark: “how fresh” a replica is for a shard + lens.
	•	Capability Token: signed authorization for scope + ops + constraints.
	•	Fast Path: single-shard (fanout=1) point query / small scan.
	•	Wide Path: scatter/gather across many shards (fanout>1), reduce monoids.

⸻

1. Core principles 

1.1 Deterministic placement, soft-state liveness
	•	Placement and replica membership are hard-state within an org (config subject or Raft).
	•	Liveness/load/freshness are soft-state via ads (TTL).

1.2 OLTP defaults to single-shard

The planner MUST attempt to answer a request with one shard:
	•	by primary key lookup
	•	by time-partition + shard range narrowing
	•	by precomputed keyed dimensions

No distributed joins by default.

1.3 Distribute aggregates, not joins

Wide path SHOULD be:
	•	partitioned scans producing mergeable monoids
	•	reduce by associative merge

Distributed joins are allowed only:
	•	when explicitly requested
	•	or when a materialized view exists

1.4 Tail latency is a first-class constraint
	•	hedged requests
	•	strict budgets/timeouts
	•	bounded work per query
	•	backpressure when wide queries are too expensive

1.5 Provenance & watermarks

Every response includes:
	•	watermark (which shard state/tip it reflects)
	•	provenance pointers (oid / oids[]) for rows (configurable)
Optional: signed receipts.

⸻

2. Sharding model (hard-state)

2.1 Shard types

DHARMA-FABRIC supports sharding for:
	•	DHARMA-Q tables (projection data)
	•	object storage (optional, if you run vault clusters)
	•	event streams (optional)

v1 focuses on DHARMA-Q tables.

2.2 Partition keys

Each table declares a partition strategy:
	1.	Key-hash sharding

	•	shard = hash(key) mod N
	•	used for keyed lookups (invoice_id, order_id, sku)

	2.	Time partitioning

	•	partition = day/month bucket on ts
	•	within partition: either key-hash shards or single shard

	3.	Geo partitioning (optional v1)

	•	partition by cell_prefix for geo tables
	•	then key-hash inside

2.3 ShardMap definition object

Hard-state config stored in an Org ShardMap Subject:
org.<org>.shardmap

ShardMap entry:
	•	table: sym
	•	strategy: enum(hash, time+hash, geo+hash)
	•	key: column name for hashing (e.g., invoice_id)
	•	time_col: optional (ts)
	•	N: number of shards
	•	replication: R replicas per shard
	•	replica_sets: mapping shard_id → list(provider_ids)
	•	lens: supported lenses/data_ver (optional)
	•	policy: constraints (max rows, max scan)

Clients MUST be able to compute shard_id locally using this map.

⸻

3. Provider ads (soft-state)

Providers publish TTL advertisements:
	•	endpoints
	•	services offered
	•	shard coverage
	•	watermarks
	•	load signals

These can be distributed via:
	•	org directory subject (preferred)
	•	LAN beacon
	•	optional DHT

3.1 Minimum ad fields (for routing)
	•	provider_id
	•	endpoints (proto + addr)
	•	services: query/search/compute/store/event
	•	shards: list of (table, shard_id, lens) served
	•	watermark: per shard/lens
	•	health: ok/degraded
	•	load: qps, queue depth (rough)
	•	ttl_s
	•	signature

⸻

4. Capability model (hard requirement)

Every request MUST include a capability token that grants:
	•	operations allowed: query.execute, search.execute, compute.execute, fetch.object
	•	scopes allowed: namespaces/tables/compartments/subjects
	•	constraints: row filters, time windows, max rows/bytes, require provenance

Providers MUST enforce capabilities before execution.

⸻

5. Execution modes

5.1 Fast path (fanout=1)

Used for:
	•	table@key point queries
	•	small filtered queries that hit one shard/partition
	•	most ERP screens

Planner rule:
	•	If query can be satisfied by one shard, MUST choose fast path.

5.2 Wide path (scatter/gather)

Used for:
	•	large scans across shards
	•	aggregates across many partitions/shards
	•	full-text / vector searches when not pre-indexed centrally

Planner rule:
	•	wide path must be bounded:
	•	max shards
	•	max partitions
	•	max time window
	•	max work budget
	•	max result size

⸻

6. Query planning (minimal but real)

6.1 Query IR

Queries compile to a small operator pipeline:
	•	scan (partitioned)
	•	filter
	•	project
	•	group/agg (monoid)
	•	sort
	•	take
	•	join (keyed dims only in v1)

6.2 Key decision: single shard test

Planner MUST attempt:
	•	can predicates constrain to a single shard?
	•	does query use keyed lookup?
	•	does it specify a key range?

If yes → fast path.

6.3 Partition pruning

If query includes time constraints and table is time partitioned:
	•	only relevant partitions are touched.

6.4 Join policy

v1 join rules:
	•	joins allowed only if:
	•	right side is keyed dim table replicated everywhere OR
	•	right side is in same shard mapping
Otherwise planner must refuse or require a materialized view.

⸻

7. Routing and replica selection

Given a target shard, the router chooses a replica.

7.1 Eligibility filters
	•	provider serves required service
	•	provider is in replica set for shard
	•	watermark meets freshness requirement
	•	provider is trusted per profile
	•	capability accepted (audience constraints)

7.2 Ranking function

Score = weighted sum:
	•	trust (allowlist/attestation)
	•	freshness distance (how behind watermark is)
	•	RTT
	•	load

7.3 Hedged requests (tail latency)

For fast path, clients SHOULD hedge:
	•	send to best replica
	•	if no response within hedge_delay_ms (e.g., 20ms), send to second-best
	•	take first successful response, cancel the other

Hedging disabled in highsec if policy forbids redundant disclosure; otherwise allowed.

⸻

8. Time budgets and backpressure (p99 discipline)

Every request has a strict budget:
	•	parse+plan: 2ms
	•	route: 2ms
	•	execute: 20–50ms (fast path)
	•	result marshal: 2–5ms

Wide path budgets are larger but capped:
	•	250ms default
	•	2s max unless explicitly allowed

Providers MUST apply backpressure:
	•	reject wide queries when overloaded (E_OVERLOADED)
	•	expose retry-after hints

⸻

9. Wide path: scatter/gather protocol

9.1 Map task

A wide query decomposes into map tasks per shard/partition:
	•	task.map includes:
	•	query fragment (scan+filter+partial agg)
	•	shard/partition identity
	•	capability
	•	deadline
	•	desired partial result format (monoid state)

9.2 Reduce topology

Reduce SHOULD be tree-based (fan-in):
	•	local reduce near data center
	•	final reduce near client

9.3 Mergeable monoids (required)

Aggregates must be representable as mergeable states:
	•	sum: (sum)
	•	count: (count)
	•	avg: (sum,count)
	•	topK: (heapK) (mergeable by heap merge)
	•	histograms: (bins[])

This guarantees associative reduce correctness.

9.4 Failure handling
	•	map tasks have deadlines
	•	stragglers can be hedged
	•	if some shards fail:
	•	either fail closed (highsec)
	•	or return partial with explicit completeness metadata (org/home profile)

Completeness metadata MUST be explicit:
	•	shards_expected
	•	shards_completed
	•	shards_failed

⸻

10. Results: watermarks, provenance, receipts

10.1 Watermark required

Every result includes:
	•	per-shard watermark used
	•	overall watermark summary (“at least up to …”)

10.2 Provenance modes

Configurable:
	•	none (fastest)
	•	oid per row
	•	oids[] for derived rows
	•	proof_pointer (compact reference to provenance table)

10.3 Receipts (optional)

Highsec profile may require signed receipt:
	•	request hash
	•	capability id
	•	provider id
	•	result hash
	•	watermark summary
	•	signature

⸻

11. Applying the same fabric to compute

Compute execution uses identical flow:
	•	determine scope + basis tip(s)
	•	shard selection (data locality)
	•	route to best executor that has data access
	•	run compute
	•	output is a proposal assertion or proposal payload + provenance
	•	endorsement required to apply

Compute can be:
	•	local
	•	single executor
	•	distributed map/reduce (training, big forecasts)

Same scheduling and budgets.

⸻

12. “Where is the data?” is always irrelevant to users

This is achieved because:
	•	placement is deterministic (ShardMap)
	•	liveness is soft-state ads
	•	authorization is capability
	•	routing is automatic
	•	results are verifiable

Users only specify:
	•	what they want
	•	what lens
	•	what freshness
	•	constraints (optional)

⸻

13. Minimal v1 deliverables (ship this)
	1.	Org ShardMap subject format + client cache
	2.	Provider ads with shard coverage + watermark + load
	3.	Capability token issuance + enforcement
	4.	Fast path:
	•	point query protocol
	•	replica selection + hedging
	5.	Wide path:
	•	map task format
	•	monoid reduce
	•	completeness metadata
	6.	Watermark inclusion + optional receipts
	7.	Backpressure + budgets

This will give you a system that feels like a “global computer” while remaining fully decentralized and safe.
