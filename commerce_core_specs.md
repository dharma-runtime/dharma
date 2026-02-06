
COMMERCE CORE SPEC

Variant-Contract, Line-Centric Fulfillment, Invoice-from-Reality

⸻

0. Core principles (read this first)
	1.	The customer buys a Variant.
	•	The Variant defines what is promised.
	•	The PO line locks a snapshot of that Variant at order time.
	•	Catalog variants may change later; contracts do not.
	2.	Fulfillment is per PO line.
	•	Each line executes independently.
	•	Partial, split, delayed, substituted execution is normal.
	3.	Sellables are execution units, not contracts.
	•	Sellables (lots, stock units) are chosen later to satisfy the line.
	•	Which sellable fulfills a line is an operational decision.
	4.	Logistics is a grouping layer, not the source of truth.
	•	Logistics batches group line fulfillment across dates.
	•	Lines decide what executes; logistics decides how/when grouped.
	5.	Accounting truth comes from fulfillment → invoices.
	•	Orders are intent.
	•	Fulfillment is reality.
	•	Invoices are fiscal truth.
	•	Payments settle invoices, not orders.
	6.	Immutability follows execution, not documents.
	•	Each component freezes when its real-world execution starts.

⸻

1. Catalog layer

1.1 Products (merchandising)

Purpose: Presentation, storytelling, taxonomy.

Fields
	•	product_id
	•	title
	•	description_rich
	•	images[]
	•	category_id
	•	taxonomy_fields (category-defined schema)
	•	status: draft | published | archived

Products are never referenced by orders directly.

⸻

1.2 Variants (commercial offer)

A Variant is the thing the customer buys.

It defines the commercial promise and constraints, not how it is fulfilled.

Fields
	•	variant_id
	•	product_id
	•	variant_code (optional)
	•	option_values (size, cut, etc.)
	•	uom (e.g. kg, each)
	•	offer_spec (structured, critical)
	•	designation (e.g. “Parmigiano Reggiano DOP”)
	•	required attributes (origin, aging min, format, etc.)
	•	temperature chain requirements
	•	any contractual taxonomy
	•	base_price (or pricing rule pointer)
	•	status: active | archived

⚠️ Variants are mutable catalog objects.
They must never be trusted after purchase.

⸻

1.3 Variant Snapshot (contract)

A VariantSnapshot is the immutable contract captured at order time.

variant_snapshots
	•	variant_snapshot_id
	•	variant_id (traceability)
	•	snapshot_hash
	•	schema_version
	•	captured_at
	•	payload (immutable JSON)

Snapshot payload (minimum)
	•	product_id
	•	product title at purchase
	•	variant label / option values
	•	uom
	•	offer_spec
	•	display name at purchase
	•	any promised constraints

📌 This snapshot is the legal & commercial truth.

⸻

1.4 Sellables (execution units)

A Sellable is a concrete, physical unit that can be allocated and shipped.

Customers never buy sellables directly.

Fields
	•	sellable_id
	•	sc (Sellable Code, immutable)
	•	type: physical (others later)
	•	uom
	•	physical_identity
	•	lot_id
	•	expiry_date
	•	supplier_id
	•	warehouse_id
	•	received_at
	•	attributes
	•	designation
	•	aging_actual
	•	origin
	•	any fulfillment-relevant data
	•	quantity_on_hand
	•	status

Sellables are used to satisfy a variant snapshot.

⸻

2. Purchase Orders

2.1 PurchaseOrder (envelope)

A PO is a coordination envelope, not a fiscal document.

Fields
	•	po_id
	•	customer_id
	•	channel_id
	•	currency
	•	status: draft | submitted | accepted | cancelled | closed
	•	created_at

⸻

2.2 PO Line (contract line)

This is the core unit of execution.

A PO line references only a Variant Snapshot.

Fields
	•	line_id
	•	po_id
	•	variant_id
	•	variant_snapshot_id ✅
	•	ordered_qty
	•	pricing_snapshot
	•	unit_price
	•	currency
	•	tax class snapshot
	•	discount allocation snapshot
	•	requested_delivery_window (optional)
	•	line_state
	•	editable
	•	execution_locked
	•	completed
	•	cancelled
	•	remaining_qty (derived)

Invariant
	•	variant_snapshot_id is immutable from creation.
	•	The snapshot defines what must be fulfilled.

⸻

3. Allocation (planning bridge)

Allocation connects contract → execution reality.

3.1 Allocation record

allocations
	•	allocation_id
	•	line_id
	•	sellable_id
	•	qty_allocated
	•	status
	•	planned
	•	reserved
	•	picked
	•	shipped
	•	released
	•	reason
	•	created_at

Allocation rules
	•	Allocations can exist long before shipping.
	•	When an allocation becomes reserved or picked:
	•	the PO line becomes execution_locked.

⸻

4. Fulfillment (reality)

4.1 FulfillmentRecord (append-only)

A fulfillment record represents what actually happened.

Fields
	•	fulfillment_id
	•	line_id
	•	sellable_id
	•	qty_fulfilled_actual
	•	weight_actual (optional)
	•	lot_snapshot
	•	logistics_batch_id (optional)
	•	fulfilled_at

📌 Fulfillment is per line, but may reference different sellables over time.

⸻

5. Logistics (grouping layer)

5.1 LogisticsBatch

Logistics batches group fulfillment across lines and dates.

Fields
	•	batch_id
	•	type: delivery | shipment | pickup | supplier_delivery
	•	scheduled_for
	•	carrier / rider
	•	address_snapshot
	•	batch_state
	•	planning
	•	scheduled
	•	in_transit
	•	delivered
	•	cancelled

Rules
	•	Address is editable until in_transit.
	•	A batch may contain fulfillment from many lines.
	•	A line may span multiple batches.

⸻

6. Invoicing (fiscal truth)

6.1 Invoice

Invoices are generated from fulfillment, not from orders.

Fields
	•	invoice_id
	•	po_id
	•	issued_at
	•	posted_at
	•	state: draft | posted | void
	•	buyer_snapshot
	•	currency
	•	source_scope (batch or fulfillment set)

InvoiceLine
	•	invoice_line_id
	•	invoice_id
	•	fulfillment_id
	•	description (from variant snapshot)
	•	qty
	•	unit_price (pricing snapshot)
	•	tax
	•	net / gross

Once posted, invoices are immutable.

⸻

6.2 Credit Notes
	•	Always reference posted invoices.
	•	Used for returns, shortages, corrections.

⸻

7. Payments (ledger-based)

Payments are append-only, allocated to invoices.

Payment
	•	payment_id
	•	provider
	•	method
	•	authorized_amount
	•	captured_amount
	•	refunded_amount

Allocations
	•	(payment_id, invoice_id, amount)

There is no “order paid” flag.

⸻

8. State & freeze rules

8.1 Line freeze

A PO line becomes execution_locked when:
	•	an allocation is reserved
	•	a pick starts
	•	a fulfillment record is created
	•	a supplier commitment is created

After lock:
	•	variant snapshot, price, offer spec cannot change
	•	remaining qty can be split, cancelled, substituted (explicitly)

⸻

8.2 Address freeze
	•	Address freezes per logistics batch at in_transit
	•	Future batches may use different addresses

⸻

9. Commands (high level)

Ordering
	•	AddVariantToPO
	•	captures VariantSnapshot
	•	creates PO line
	•	UpdateLineQty (while editable)
	•	CancelRemainingLineQty
	•	SplitLine

Allocation / Ops
	•	PlanAllocation
	•	ReserveAllocation
	•	ReleaseAllocation

Fulfillment
	•	RecordFulfillment

Logistics
	•	CreateLogisticsBatch
	•	AttachLineToBatch
	•	DispatchBatch
	•	DeliverBatch

Invoicing
	•	IssueInvoiceFromBatch
	•	PostInvoice
	•	IssueCreditNote

Payments
	•	AuthorizePayment
	•	CapturePayment
	•	AllocatePayment

⸻

10. Matching rule (critical)

When allocating a sellable to a line:

CanSellableSatisfy(
  sellable.attributes,
  variant_snapshot.offer_spec
) == true

Matching is against the snapshot, never the live catalog variant.

⸻

11. UI model (human-friendly)

Customer service sees:
	•	PO lines as contract items (“Parmigiano Reggiano 5kg”)
	•	Clear line-level state pills
	•	Delivery dates per line
	•	No lots unless expanded

Ops sees:
	•	Allocations per line
	•	Sellables & lots
	•	Logistics batches by date

Accounting sees:
	•	Invoices + credit notes
	•	Allocations of payments
	•	No mutable totals

⸻

12. The invariant, finally stated cleanly

The PO line is the contract (variant snapshot).
Fulfillment chooses reality.
Logistics groups execution.
Invoices record truth.
Payments settle invoices.

This model is:
	•	legally correct
	•	operationally realistic
	•	customer-service friendly
	•	accounting-clean
	•	CQRS-native
