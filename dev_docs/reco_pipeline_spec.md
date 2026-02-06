# Two-Tower Recommendation System Spec (v1)

## 0) Goals
1. Recommend **items** (variants chosen at serve time).
2. Support purchase, cross-sell, complementary use cases.
3. Cold start via DemandKey priors + content embeddings.
4. Versioned, auditable outputs.
5. Availability-aware serving.

---

## 1) Entities + keys
- Recommendation unit: **item_id**
- Every item has DemandKey full + parents
- DemandKey used for cold start + diversification

---

## 2) System architecture
Two-stage:
- **Candidate generation (two-tower retrieval)**: fast, top-K (200-2000)
- **Reranker**: applies features + business rules; outputs top-N

---

## 3) Use cases
- **Purchase**: home/category/item
- **Cross-sell**: cart/checkout
- **Complementary**: pairing (co-consumption)

---

## 4) Data inputs (contracts)

### 4.1 Interaction events
`std.reco.event`
- event_id, user_id?, session_id?, item_id, event_type, qty?, price_at_event?, location_id, channel_id, timestamp, context

### 4.2 Basket/session
`std.reco.basket`
- basket_id, user_id?, session_id?, items[], total, location_id, channel_id, created_at, converted

### 4.3 Item features snapshot
`std.reco.item_features`
- item_id, demand_key_full (+ parents), classifications, attributes, constraints, price_band, embedding_content?, updated_at

### 4.4 Availability input
`std.reco.item_servability`
- item_id + location_id + bucket_date -> purchasable bool + confidence

---

## 5) Embeddings

### 5.1 Item tower embedding
`std.reco.item_embedding`
- item_id, model_version, vector, computed_at

### 5.2 Query tower embedding
`std.reco.user_embedding`
- user_id, model_version, vector

Fallbacks:
- content embedding
- DemandKey embedding

---

## 6) Two-tower training

### Positives
- Purchase model: (user/session context -> purchased item)
- Cross-sell: (cart state -> next added/purchased item)
- Complementary: (anchor item -> co-purchase with lift)

### Negatives
- in-batch negatives
- popularity-weighted negatives
- hard negatives: same DemandKey prefix for complement model

---

## 7) Candidate generation
- ANN index per task: purchase / cross-sell / complementary
- Filter: already in cart/purchased, policy filters
- Availability-aware filter

---

## 8) Reranker
- Inputs: margin, price compatibility, temp chain, delivery feasibility, diversity penalty
- Output: ranked list + optional explanation tags

---

## 9) Business rules
- Diversity: penalize repeated narrow DemandKey prefixes
- Complement: exclude substitutes unless explicitly allowed
- Availability constraints required

---

## 10) Output contracts

### 10.1 Trending priors
`std.reco.trending_by_key`
- key: demand_key_level_bits, key_code, location_id, channel_id, time_bucket
- fields: top items, scores

### 10.2 Precomputed recommendations (optional)
`std.reco.item_to_item`
- key: (item_id, task_type, location_id?, channel_id?)
- fields: items[], scores, model_version, computed_at

---

## 11) DSL: reco_pipeline
```
reco_pipeline CrossSellV1 {
  version = 5
  task = "cross_sell"

  query_representation { type = "cart" window = "current_cart" }
  positives { from std.reco.basket rule = "item_added_after_cart_state" lookback_days = 180 }
  negatives { strategy = "in_batch + popularity" hard_negatives = "same_demand_key_prefix:12" }

  model { engine = "two_tower" embedding_dim = 128 loss = "InfoNCE" }

  candidates { ann_index = "hnsw" top_k = 500 }

  rerank { enabled = true engine = "gbm" features [margin, availability_confidence, diversity_penalty] }

  serve_filters { exclude_in_cart = true require_available = true require_temp_chain_compatible = true }
}
```

---

## 12) Evaluation & monitoring
- Offline: Recall@K, NDCG@K, coverage (cold start)
- Online: CTR, add-to-cart, conversion lift, basket value
- Segment by location/channel, DemandKey families

---

## 13) Minimal v1 build plan
1. `std.reco.event` + `std.reco.basket` projections
2. `std.reco.item_features` + DemandKey membership
3. Two-tower training (external ML)
4. ANN index and retrieval
5. Availability filter in serving
6. Reranker (GBM or simple)
7. Complementary model (co-purchase lift)
