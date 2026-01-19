Below is a complete, implementable specification for a q/kdb-inspired projection database + query/search/vector engine for DHARMA.

This is DHARMA-Q v1: a projection-only system (derived, rebuildable) that consumes accepted DHARMA assertions and provides blazing fast ERP-style queries, geo, typo-resistant search, and vector search—with no SQL and a terse q-like language.

⸻

DHARMA-Q v1

A q/kdb-inspired Projection Database + Query/Search/Vector Engine for DHARMA

Status

Draft v1 — implementable, deterministic in semantics (not necessarily deterministic in ranking for fuzzy search unless configured).

⸻

0) Core principles

0.1 Truth vs projection
	•	Truth remains the DHARMA append-only object store (assertion envelopes + artifacts).
	•	DHARMA-Q stores only derived projections/indexes.
	•	DHARMA-Q MUST be disposable: delete it, rebuild it from the DHARMA event log.

0.2 Performance goals
	•	Optimize for 99.9% simple queries:
	•	filter, select, sort, limit
	•	group & aggregates
	•	small joins against keyed dimensions
	•	time bucketing
	•	geo within/near
	•	full-text search (typo tolerant)
	•	vector similarity search
	•	Predictable latency:
	•	interactive: <10–50 ms typical for common queries on warm cache
	•	dashboard: <100–300 ms typical (aggregations)
	•	heavy scans: acceptable but visible (explain must reveal cost)

0.3 Small kernel separation

DHARMA-Q MUST be behind:
	•	a feature flag, or preferably
	•	a separate binary (dharmaq / dharma-queryd) to keep DHARMA runtime certifiable and tiny.

0.4 Provenance is first-class

Every row in DHARMA-Q MUST carry:
	•	oid (source assertion object id) and/or oids (set of contributing assertion ids)
	•	sub (subject id)
This enables WHY / audit from query results back to DHARMA truth.

⸻

1) High-level architecture

DHARMA-Q is composed of:
	1.	Ingestor

	•	subscribes to accepted assertions (from local store or network)
	•	extracts facts/rows
	•	appends to hot partitions
	•	maintains indexes incrementally

	2.	Columnar Store

	•	kdb-style tables
	•	time partitioned
	•	column files + symbol dictionary
	•	hot partition mutable, cold partitions immutable/mmapped

	3.	Query Engine

	•	q-like terse language
	•	vectorized operators over columns
	•	partition pruning + predicate pushdown
	•	joins optimized for “fact ↔ dim” patterns

	4.	Search Engine

	•	tokenization + normalization
	•	inverted index
	•	typo tolerant retrieval (fast candidate generation + edit distance)
	•	scoring (BM25 or simpler TF-IDF)

	5.	Vector Engine

	•	ANN index (HNSW) per field/table/partition
	•	hybrid vector + filters + geo support
	•	optional reranking

	6.	Geo Engine

	•	fixed-point coordinates
	•	cell indexing (S2 / geohash bits)
	•	bbox + exact geometry checks
	•	near/within/intersects operators

	7.	Explain/Why

	•	query plan inspection
	•	provenance extraction for rows

⸻

2) Projection store: directory layout

All data lives under a DHARMA-Q root, e.g. data/dharmaq/.

dharmaq/
  meta/
    config.toml
    schema_catalog.cbor
    watermark.cbor
  sym/
    sym.dict        # global symbol dictionary
    sym.index       # reverse lookup (optional)
  tables/
    <table>/
      meta.cbor
      partitions/
        p=YYYY.MM.DD/
          cols/
            <col>.bin
            <col>.idx      # optional per-column index
          rowid.bin
          provenance.bin   # optional (oids lists)
          text/            # per-table text index segments (optional)
          vec/             # per-table vector index segments (optional)
          geo/             # per-table geo index segments (optional)
      hot/
        wal.bin            # crash safety for hot partition
        cols/
        indexes/
  indexes/
    text/...
    vector/...
    geo/...

Requirements
	•	Cold partitions MUST be immutable.
	•	Hot partition MAY be mutable but MUST be recoverable from WAL.
	•	A partition is “sealed” by writing a partition.seal marker and compacting/optimizing.

⸻

3) Data types (complete v1)

DHARMA-Q uses typed, columnar vectors. No floats are required for core business logic; floats MAY exist for embeddings only.

3.1 Scalar types
	•	b1  : bool (1 bit logical, stored as u8 or bitset)
	•	i32 : signed 32-bit
	•	i64 : signed 64-bit
	•	u32 : unsigned 32-bit
	•	u64 : unsigned 64-bit
	•	dec : fixed-point decimal {mantissa:i64, scale:u8}
	•	e.g., money in cents: scale=2
	•	time : i64 microseconds since epoch (UTC)
	•	dur  : i64 microseconds (duration)
	•	sym  : symbol (interned string id, u32)
	•	str  : UTF-8 string (rare; prefer sym)
	•	bytes: byte slice (artifact refs, etc.)
	•	id32 : 32-byte id (object_id, subject_id) stored as 32 bytes

3.2 Composite types
	•	list<T>: variable length list (encoded in two columns: offsets + values)
	•	dict<K,V>: map (stored as two lists; v1 discourages for hot queries)

3.3 Geo types (fixed point, deterministic)

All geo types avoid floats.
	•	geopoint
	•	lat_e7: i32 (lat * 1e7)
	•	lon_e7: i32 (lon * 1e7)
	•	optional alt_mm: i32
	•	optional acc_mm: i32
	•	geocell
	•	cell: u64 (S2 cell id or geohash bits at configured precision)
	•	geobox
	•	min/max lat/lon e7
	•	geocircle
	•	center geopoint + radius_m: u32
	•	geopoly
	•	points list geopoint + bbox
	•	holes NOT in v1 (add later)

3.4 Vector types
	•	vec_f16[n] or vec_i8[n] (preferred)
	•	vec_f32[n] allowed (but larger)
Vectors stored columnar; ANN index stores graph.

⸻

4) Tables, keys, and joins

4.1 Table kinds
	•	Fact tables: high volume, time partitioned (orders, postings, events)
	•	Dim tables: lower volume, keyed (customers, products, vendors)
	•	Index tables: inverted index postings, vector node metadata, geo cell maps

4.2 Keyed tables (kdb-inspired)

A table MAY be “keyed” by one or more columns:
	•	customer keyed by customer_id
	•	product keyed by sku or product_id

Keyed lookup must be O(1) or O(log n) depending on index type.

4.3 Join support (v1)

DHARMA-Q supports joins optimized for ERP:
	•	lj left join
	•	ij inner join
	•	aj as-of join for time-series (optional v1)
	•	join condition limited to equality on key columns (v1)

⸻

5) Column encoding and storage

5.1 Column files

Each column is stored as:
	•	header (type, count, encoding)
	•	data blocks

Encodings:
	•	plain (fixed-width)
	•	dictionary (for sym and low cardinality)
	•	RLE (run-length encoding for repeated values)
	•	delta encoding for monotonic numbers (timestamps)
	•	bitset encoding for bools and some categorical filters
	•	optional compression: LZ4/Zstd (feature flag; Zstd for cold partitions)

5.2 Nullability

Nulls are supported via:
	•	a bitmap column <col>.null OR
	•	sentinel values for some types (discouraged)

5.3 Hot partition write path

Hot partitions append to column append buffers + WAL:
	•	wal.bin records row batches (row-oriented) for recovery
	•	on flush/compaction, WAL is folded into columnar blocks

Cold partition read path is memory-mapped where possible.

⸻

6) Ingestion from DHARMA (projection pipeline)

6.1 Input stream

DHARMA-Q ingests only ACCEPTED assertions (per lens):
	•	from local DHARMA store tailing
	•	or via subscription API from a gateway/node

It must track a watermark:
	•	last processed object_id or per-subject frontier tip, per lens

6.2 Fact extraction rules

Extraction is defined by packages (stdlib or company):
	•	mapping from assertion type → rows in one or more tables
	•	must be versioned with the same data lens model

Example mapping:
	•	std.order.create → row in order + N rows in order_line
	•	std.ledger.post → N rows in ledger_posting

6.3 Idempotency

Every ingested row MUST include:
	•	oid (source assertion id)
	•	sub (subject)
Rows MUST be deduplicable by (table, oid, row_ordinal).

6.4 Provenance fields (required)

At minimum:
	•	oid: id32
	•	sub: id32
	•	ts: time (best available timestamp claim or derived ordering)

Derived rows (aggregates) SHOULD store:
	•	oids: list<id32> or a compact provenance pointer.

⸻

7) Query language (q-inspired, no SQL)

7.1 Overview

A query is an expression producing:
	•	a scalar
	•	a vector
	•	a table

The dominant form is pipeline:

<table_expr> | <op> | <op> | ...

7.2 Lexical conventions
	•	identifiers: [a-zA-Z_][a-zA-Z0-9_.]*
	•	symbols: 'foo or "foo" depending on preference; pick one
	•	time literals: 2026.01.15, now(), today()
	•	duration literals: 5s, 10m, 2h, 7d, 30d

7.3 Core operators (v1)

Source
	•	t table reference
	•	t[p=2026.01.15] partition
	•	t@key keyed lookup (if applicable)

Filter
	•	where <pred>[, <pred>...]
Predicates:
	•	= != < <= > >=
	•	in
	•	between
	•	like (prefix/suffix/contains on sym and str)
	•	isnull, notnull

Projection
	•	sel col1,col2,...
	•	sel expr as name, ...

Sort / limit
	•	sort col (asc)
	•	sort -col (desc)
	•	take n
	•	drop n

Group + aggregate
	•	by col[,col...] | agg sum(x), count(), min(x), max(x), avg(x)
	•	bucket ts 1d (adds a bucket column)
	•	by bucket ts 1d | agg ... allowed

Joins
	•	lj <table> on <a>=<b> (or on key)
	•	ij ...
	•	aj ... (optional)

Search
	•	search "query" in <table>.<field>[,<field>...] [opts...]

Vector
	•	vsearch "query" in <table>.<vecfield> k=50 [opts...]
	•	vnear <vector_literal> in ... (optional)

Geo
	•	near (lat=…,lon=…) within 5000m
	•	within zone <place_id_or_sym>
	•	within circle (...)
	•	within box (...)
	•	intersects ... (v2)

Explain/why
	•	explain <query>
	•	why row <n> or why oid <id>

⸻

8) Query engine execution model (vectorized)

8.1 Execution pipeline

Queries compile to a physical plan of operators:
	•	partition pruning
	•	column selection
	•	predicate evaluation producing a boolean mask
	•	mask application producing filtered vectors
	•	group-by via hash maps / sort-group
	•	join via keyed lookup or hash join
	•	sorting via indices
	•	take/drop by slicing

8.2 Partition pruning (mandatory)

If the query includes a time predicate on a partitioned table, engine MUST:
	•	select only relevant partitions first

8.3 Predicate pushdown (mandatory)

Filters must evaluate using only referenced columns; avoid materializing full rows.

8.4 Join strategies (v1)
	•	keyed lookup join: O(n) for left side, fast
	•	hash join: for non-keyed dims (optional)
	•	join must preserve provenance: row provenance is union of both sides

8.5 Determinism

Numeric query semantics are deterministic.
Search ranking may be deterministic if:
	•	tokenization and candidate ordering are fixed
	•	ties broken by oid ascending

Vector ANN results are not strictly deterministic (graph traversal); if determinism needed, add deterministic=true mode that forces full scan or exact kNN for small sets.

⸻

9) Full-text search (typo-resistant)

9.1 Goals
	•	fast keyword search
	•	typo tolerance (misspellings)
	•	phrase-ish behavior optional
	•	field weighting
	•	results joinable back to domain tables

9.2 Normalization pipeline

For each indexed field:
	1.	Unicode normalize (NFKC)
	2.	casefold
	3.	remove diacritics (configurable)
	4.	tokenize (unicode word boundaries)
	5.	optional stemming (off by default; ERP often wants literal)
	6.	stopwords optional (off by default for names/SKUs)

9.3 Inverted index layout

Per partition (or global for dims):
	•	term -> postings
Postings contain:
	•	doc_id (rowid) OR oid directly
	•	optional positions for phrase support
	•	per-field weights

Store postings compressed:
	•	delta-encoded docids
	•	varints
	•	optional roaring bitmap for high-frequency terms

9.4 Typo tolerance design (fast)

Do two-stage retrieval:

Stage A: Candidate generation
	•	Build an n-gram index (recommended trigram) over terms or over field text.
	•	Query trigrams of the input token to get candidate terms quickly.

Stage B: Edit-distance filtering
	•	Use a bounded Levenshtein distance (e.g., ≤1 or ≤2 depending on token length).
	•	Filter candidate terms by edit distance.
	•	Deterministically sort candidates by:
	1.	edit distance
	2.	term frequency / idf
	3.	lexicographic term

Stage C: Scoring
	•	BM25 or TF-IDF variant.
	•	Score per document = sum(term scores * field weight).
	•	tie-break by oid or rowid.

9.5 Query syntax

Examples:

search "foie gras" in product.name,product.desc
search "andouillete troyes" in product.name fuzz=2
search "saaf" in vendor.name fuzz=1
search "INV-2026" in invoice.id exact=true

Options (v1):
	•	fuzz=<0..2> default 1 for tokens length ≥5
	•	prefix=true|false
	•	fields weights: w(name)=3,w(desc)=1
	•	limit=n

9.6 Output table

Search returns a result table:
	•	oid (or doc rowid + join key)
	•	score
	•	field
	•	snippet (optional)
	•	provenance pointer to source assertion(s)

⸻

10) Vector search (ANN)

10.1 Goals
	•	semantic retrieval
	•	hybrid filtering (status, warehouse, price range)
	•	reranking optional
	•	joinable results

10.2 Vector storage

Vectors stored columnar in the table:
	•	embed: vec_i8[256] or vec_f16[384]
	•	metadata: oid, join keys

10.3 ANN index: HNSW (v1)

Per table.field per partition (or global for small dims):
	•	HNSW graph persisted in vec/
	•	node id corresponds to rowid
	•	store:
	•	level
	•	neighbor lists per level
	•	entry point
	•	vector norms if needed

10.4 Hybrid query execution

Execution order:
	1.	apply structured filters first (mask)
	2.	ANN search over candidates:
	•	either build per-partition HNSW and query those partitions
	•	or global HNSW + filter during traversal (less efficient)
	3.	optional rerank on exact similarity for top K*R (R=2..5)
	4.	return top K

10.5 Similarity metrics
	•	cosine similarity (recommended)
	•	dot product
Use fixed behavior.

10.6 Query syntax

vsearch "luxury cheese gift" in product.embed k=50
| lj product on oid=product.oid
| where price<5000
| sort -score
| take 10


⸻

11) Geo engine (fast + deterministic)

11.1 Core idea

Never scan polygons blindly. Use:
	•	cell index (geohash bits / S2 cell id)
	•	bbox reject
	•	exact check on survivors

11.2 Geo indexing

For any geo point event table:
	•	store lat_e7, lon_e7
	•	store derived cell at configured precision
Index:
	•	(cell, ts, rowid) → fast region queries

For zones/places:
	•	store polygon/circle + bbox + covering cells
	•	index zone coverage cells → candidate zones

11.3 Operators

Near

ship_evt | near (lat=14.5547, lon=121.0244) within 5000m

Execution:
	•	compute set of cells covering radius (approx)
	•	fetch candidates via cell index
	•	exact distance check using fixed-point approximation

Within

evt | within zone 'ncr.delivery

Execution:
	•	resolve zone polygon
	•	candidate points via zone cell covering
	•	exact point-in-polygon with deterministic ray casting on int coords

11.4 Deterministic geometry rules
	•	Points on boundary count as inside (recommended)
	•	Polygon rings must be canonical (fixed winding order)
	•	Max polygon vertices configured (caps worst-case)

⸻

12) Search + vector + geo combined (“universal queries”)

DHARMA-Q supports hybrid queries by pipeline:

Example: “recommended products near delivery zone, typo tolerant search”

search "andouillete" in product.name fuzz=2
| lj product on oid=product.oid
| where in_stock=true
| vsearch "classic french charcuterie" in product.embed k=200
| where price between (1500,5000)
| sort -score
| take 20

Geo + vector:

warehouse_evt | within zone 'ncr
| vsearch "cold chain risk" in incident.embed k=50


⸻

13) Explain and provenance (“WHY” for queries)

13.1 explain <query>

Must print:
	•	partitions scanned
	•	columns read
	•	indexes used (cell/text/vector)
	•	join strategy
	•	estimated cost
	•	actual runtime (if executed)

13.2 why row <n>

Returns:
	•	oid (source assertion)
	•	if derived: oids[] (or provenance pointer)
	•	optional: link to DHARMA prove output

Rule: any projection row must be traceable back to DHARMA truth.

⸻

14) API surface (for ERP apps)

Even if you’re not doing GraphQL yet, you’ll want:
	•	a query endpoint that takes a DHARMA-Q expression and returns rows
	•	a subscription endpoint for incremental updates (optional)

14.1 Query API

POST /query
	•	input: query string + parameters
	•	output: table (columnar JSON or row JSON)
	•	optional: include provenance

14.2 Prepared queries (optional v1)

Allow parameterized queries:

q("invoice | where status=$1, ts>= $2 | sort -ts | take $3", ["open", today()-30d, 50])


⸻

15) Operational behavior

15.1 Rebuild

Two modes:
	•	full rebuild: wipe DHARMA-Q store, replay all accepted assertions
	•	incremental: tail from watermark

15.2 Compaction & sealing

Hot partition:
	•	accepts writes
	•	WAL ensures crash safety
Periodic:
	•	seal partition
	•	compress columns
	•	build/optimize indexes
	•	move to cold partitions

15.3 Feature flags

Recommended:
	•	query (engine)
	•	text (inverted + fuzzy)
	•	vector (ANN)
	•	geo (cell + geometry)
	•	compression_zstd
	•	deterministic_search (tie-breaking & stable ranking)

⸻

16) Minimal v1 roadmap (what to implement first)

Phase Q1 (core ERP speed)
	•	columnar store
	•	partitions
	•	where/sel/sort/take/by/agg
	•	keyed dims + lj/ij
	•	provenance via oid

Phase Q2 (geo)
	•	geopoint + cell index
	•	near/within operators

Phase Q3 (text search)
	•	tokenization + inverted index
	•	trigram candidate generator + edit distance fuzz=1/2
	•	scoring + stable tie-break

Phase Q4 (vector)
	•	store vectors
	•	HNSW per table.field
	•	hybrid filters + rerank

Phase Q5 (why/explain complete)
	•	full explain plans
	•	provenance on derived results (oids pointers)

⸻

17) Example “99.9% ERP queries” in DHARMA-Q

Open invoices:

invoice | where status='open | sort -ts | take 50

AP aging:

invoice | where status='open
| sel id,vendor,total,due,age=(now()-due)
| by bucket age 30d
| agg sum(total),count()

Inventory by warehouse:

inventory | by wh | agg qty=sum(qty) | sort -qty

Top customers last 30 days:

order | where ts>=today()-30d
| by customer
| agg rev=sum(total)
| sort -rev
| take 20

Near deliveries:

delivery_evt | near (lat=14.55,lon=121.02) within 3000m | take 100

Typo tolerant product search:

search "comte chees" in product.name fuzz=2 | take 20

Semantic search:

vsearch "luxury french cheese board" in product.embed k=50
| lj product on oid=product.oid
| sel product.id, product.name, score
| sort -score
| take 10


⸻

Final note (important)

This design is coherent with DHARMA because:
	•	DHARMA remains the immutable truth ledger.
	•	DHARMA-Q is a fast, disposable, rebuildable projection engine.
	•	It gives you q/kdb-style speed without dragging SQL complexity in.
	•	It supports geo + typo-resistant text + vector in a unified, terse query language.
	•	It retains DHARMA’s superpower: explainability and provenance.

If you want the next step, I can produce:
	1.	a complete grammar (EBNF) for the query language, and
	2.	the exact on-disk column formats (headers, encodings, sym dictionary), and
	3.	a minimal query planner spec (partition pruning + operator fusion + join algorithms).