# Keyspace + Embedding Layer Spec (DHL + Runtime)

## 0) Purpose
Provide a universal, deterministic mechanism to:
- derive **stable, versioned, hierarchical keys** from immutable snapshots
- use those keys as **join keys** for forecasting, availability, recommendations, analytics
- support **cold start fallback** via hierarchy (coarse -> fine)

This layer must be:
- **deterministic**
- **replayable**
- **versioned**
- **domain-agnostic** (fine food, apparel, electronics, etc.)

---

## 1) Core concepts

### 1.1 Snapshot-first
Embeddings and keys are derived **only** from immutable snapshots.
- Example: `std.commerce.catalog.item_snapshot`
- If only mutable item exists, create the snapshot first.

### 1.2 EmbeddingModel
A versioned, declarative spec describing:
- which schema fields are used
- tokenization + canonicalization
- deterministic payload bytes format
- embedding engine + model version

### 1.3 Keyspace
A versioned, declarative spec describing:
- how to convert an embedding vector to a **hierarchical discrete key**
- hierarchy levels (prefix bits or PQ subspaces)
- dependent embedding model + quantizer versions

### 1.4 Key output
A stable symbolic identifier:
- includes scheme id + versions
- supports parent/child traversal
- usable as a primary key in projections

---

## 2) DSL additions
Add two new top-level blocks in DHL:

```
embedding_model "<name>" { ... }
keyspace <Name> { ... }
```

Compiled into artifacts similar to projections.

---

## 3) EmbeddingModel spec

### 3.1 DSL syntax (example)
```
embedding_model "item-embed" {
  version = 7
  subject = std.commerce.catalog.item_snapshot

  input {
    // classifications
    field state.classifications.hs_code              weight 1.0 tokenize "exact"
    field state.classifications.google_category_id   weight 0.8 tokenize "path"
    field state.classifications.internal_category_id weight 0.8 tokenize "path"

    // structured spec & constraints
    field state.spec.kind                            weight 1.0 tokenize "exact"
    field state.spec.attributes                      weight 1.0 tokenize "kv"
    field state.constraints.temp_chain               weight 1.0 tokenize "exact"
    field state.constraints.allergens                weight 0.7 tokenize "set"

    // numeric -> buckets
    number state.physical.weight_g                   weight 0.3 bucket "log_8"
    number state.physical.volume_cm3                 weight 0.2 bucket "log_8"

    // stable text
    text state.name                                  weight 0.5 normalize "basic"
    text state.short_desc                            weight 0.2 normalize "basic"
  }

  normalize {
    lowercase = true
    unicode_nfkc = true
    trim = true
    dedupe_sets = true
    sort_sets = true
    max_text_chars = 512
  }

  engine {
    provider = "openai"
    model = "text-embedding-3-large"
    dimensions = 3072
  }
}
```

### 3.2 Semantics
#### 3.2.1 Deterministic payload construction
`(subject_state, embedding_model_version) -> payload_bytes`
- canonical, stable, language-agnostic
- line-oriented tokens, UTF-8
- one token per line, stable ordering

Token format:
```
<token_key>=<token_value>\n
```

Examples:
```
HS=040690
GOOGLE_CAT=Food>Cheese>Hard
SPEC:designation=parmigiano_reggiano_dop
TEMP=chilled
ALLERGEN=milk
WEIGHT_BUCKET=log8:6
NAME=parmigiano reggiano dop
```

#### 3.2.2 Tokenization modes (required)
- **exact**: `KEY=value`
- **path**: `A>B>C` expands to `A`, `A>B`, `A>B>C`
- **kv**: map expansion: `ATTR:k=v`
- **set**: one token per element, deduped + sorted
- **text**: normalized text token
- **bucket**: numeric -> categorical token (named strategy)

#### 3.2.3 Weights (deterministic)
Two acceptable encodings (compiler chooses and records):
- token repetition proportional to weight, or
- prefix `W<weight>:` on each token

#### 3.2.4 Versioning rules (hard)
Bump version if any change to:
- fields
- tokenizers
- normalization
- buckets
- weights
- engine provider/model/dimensions

### 3.3 Compiler artifact `.embedding`
Emitted artifact includes:
- normalized field list
- tokenization plan
- canonical payload spec
- engine selection
- embedding_model_id + version
- config_hash (debug)

---

## 4) Keyspace spec

### 4.1 DSL syntax
```
keyspace DemandSpace {
  scheme = "embq"
  hierarchical = true
  levels = [4, 8, 12, 16]
  embedding_model = "item-embed"
  embedding_version = 7
  quantizer_version = 3
}
```

### 4.2 Semantics
#### 4.2.1 Scheme "embq"
Pipeline:
1. build payload bytes
2. compute embedding vector
3. quantize vector -> integer code (max bits = max(levels))
4. hierarchy = prefixes of the code

#### 4.2.2 Hierarchy
For levels `[4,8,12,16]`:
- 16-bit full code (0..65535)
- prefix levels at 4/8/12/16 bits

#### 4.2.3 Versioning
Keyspace version =
- embedding_model id + embedding_version
- quantizer_version
- scheme
Any change -> new version

### 4.3 Key type
```
struct Key {
  keyspace: Text
  scheme: Text
  embedding_version: u64
  quantizer_version: u64
  level_bits: u8
  full_bits: u8
  code: u64
}
```
Keys comparable only if versions + scheme match.

### 4.4 Runtime operations (required)
- `DemandSpace.levels() -> List<u8>`
- `DemandSpace.key(payload: Bytes, level_bits: u8) -> Key`
- `DemandSpace.key_from_subject(subject_ref: SubjectRef, level_bits: u8) -> Key`
  - reads snapshot at pinned seq
- `DemandSpace.key_full(subject_ref) -> Key`
- `DemandSpace.parent(k: Key, parent_level_bits: u8) -> Key`
- `DemandSpace.prefix(k: Key) -> u64`
- `DemandSpace.same_space(a,b) -> Bool`

Optional:
- `DemandSpace.distance(a: SubjectRef, b: SubjectRef) -> f32`
- `DemandSpace.neighbors(subject_ref, k) -> List<SubjectId>`
- `DemandSpace.explain(key, top_n) -> Explanation`

### 4.5 Quantizer requirements
- deterministic mapping per version
- stable across runtime instances
- codebooks stored + versioned

### 4.6 Compiler artifact `.keyspace`
Emits:
- keyspace config
- max_bits + allowed levels
- embedding model reference
- keyspace_id + version
- config_hash

---

## 5) Determinism + historical reads
- `read_at_seq(subject_id, seq)` is **mandatory**.
- payload depends only on snapshot + model spec
- no time, locale, or nondeterministic maps

---

## 6) Storage / projection
### 6.1 Membership projection (recommended)
`std.commerce.demand.membership`:
- item_snapshot_id
- keyspace_version
- key_full
- optionally parents (4/8/12)

Rebuild on new snapshot or version upgrades.

---

## 7) Governance + upgrades
- embedding_model version bump -> rebuild membership
- quantizer version bump -> rebuild membership
- never overwrite old keys; new version = new space

---

## 8) Query surface (internal)
- `GetItemKey(item_snapshot_id, level_bits)`
- `ListItemsInKey(key)`
- `ExplainKey(key)`
- `KeyPopulationStats(level_bits)`

---

## 9) Security / privacy
Embedding payload must exclude sensitive data:
- no customer PII
- no supplier confidential pricing
- no internal notes

---

## 10) Tests (must-have)
1. Determinism: same snapshot -> same key across runs
2. Hierarchy: parent prefixes consistent
3. Versioning: bump changes keys
4. Canonicalization: map/list reorder stable
5. Performance: membership projection N items
6. Historical read: pinned seq stable after updates
