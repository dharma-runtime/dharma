# DHARMA Oracles & Side Effects: Interfacing with Reality

DHARMA is a deterministic "Truth Machine". It cannot make network calls, check the weather, or send emails directly within its kernel logic.
To interact with the outside world, DHARMA uses a standardized **Request/Response Protocol**.

---

## 1. The Core Concept: Asynchronous Bridges

Instead of "calling an API", a DHARMA contract **asserts a Request**.
An external agent (the Oracle/Bridge) sees the request, performs the work, and **asserts a Response**.

This ensures:
1.  **Determinism:** The history contains both the intent (Request) and the result (Response). Replay is safe.
2.  **Auditability:** Every external interaction is signed and time-stamped.
3.  **Resilience:** If the email server is down, the DHARMA network keeps running. The Request sits pending until the bridge recovers.

---

## 2. Oracles (Inbound Truth)

Oracles bring external facts (Price, Location, Weather) into the ledger.

### The Workflow
1.  **Contract:** "I need the distance matrix for these points."
2.  **Ledger:** Records `action.Oracle.Request(topic="maps.matrix", params=...)`.
3.  **Oracle Node:**
    -   Subscribes to `Oracle.Request`.
    -   Calls Valhalla/Google Maps API.
    -   Computes the result.
    -   **Signs** `action.Oracle.Response`.
4.  **Contract (Reactor):** Triggers on `Response`, updates state with the new data.

### Trust Model
Contracts explicitly define *who* they trust.
```dhl
validate has_role(context.signer, 'ApprovedMapProvider')
```
If the Oracle lies, their signature is proof of malfeasance.

---

## 3. Side Effects (Outbound Action)

Side Effects perform actions in the real world (Email, SMS, Payment Gateway, PDF Generation).

### The Workflow
1.  **Contract:** "Send the Invoice PDF to client@example.com."
2.  **Ledger:** Records `action.Effect.Request(type="email", params={to: "...", body: "..."})`.
3.  **Bridge Node:**
    -   Subscribes to `Effect.Request`.
    -   Generates the PDF.
    -   Connects to SMTP Server.
    -   Sends Email.
    -   **Signs** `action.Effect.Receipt(status="Sent", message_id="...")`.
4.  **Contract:** Updates state to `Sent`.

---

## 4. The `std.bridge` Specification

We define a standard schema for these interactions to ensure interoperability.

### Request
```dhl
action Request(
    topic: Text,            // e.g. "email.send", "maps.matrix"
    params: Map<Text, Any>, // The arguments
    callback: Text,         // The action to trigger with the result
    id: ObjectId            // Unique ID (usually self.id)
)
```

### Response / Receipt
```dhl
action Response(
    request_id: Ref<Request>,
    status: Enum(Success, Error, Pending),
    result: Map<Text, Any>, // The Data or Receipt Metadata
    error: Text?
)
```

---

## 5. Sync vs Async

Not all bridges can respond immediately. The protocol distinguishes **Sync** and **Async** oracles.

- **Sync:** The bridge responds during the same session/window. Callers may block until `Response` arrives.
- **Async:** The bridge **queues** the request and responds later. The request is durable and replayable.

The timing is part of the oracle advertisement and is enforced by routing and token policy.

---

## 6. The Oracle Job Queue (Async)

Async bridges must maintain a durable, deterministic queue derived from the ledger.

**Rules:**
1.  A `Request` is **pending** until a matching `Response` exists.
2.  On startup, rebuild by scanning: `pending = Requests - Responses`.
3.  Queue state is **local** (not consensus), but fully reconstructible.

**Minimal filesystem layout (no DB):**
```
data/oracle_queue/
  pending/
  inflight/
  done/
  queue.log   // append-only events
```

Workers transition jobs via atomic rename (`pending -> inflight -> done`).
If a worker crashes, jobs are re-derived from the ledger and re-queued.

---

## 7. Examples

### Example A: Logistics Optimization (Oracle)
1.  **Optimizer:** Emits `Request("maps.matrix", locations=[A,B,C])`.
2.  **Map Service:** Calculates distances. Signs `Response(result=[[0,10],[10,0]])`.
3.  **Optimizer:** Reads `Response`. Runs Solver. Emits `Route(A->B->C)`.

### Example B: Invoice Delivery (Side Effect)
1.  **Invoice:** State changes to `Approved`.
2.  **Reactor:** Emits `Request("email.send", to=client.email, subject="Invoice")`.
3.  **Mail Bridge:** Sends via SendGrid. Signs `Response(status="Success")`.
4.  **Invoice:** Reactor sees `Response`. Updates `state.delivered = true`.

---

## 8. Implementation Strategy

### The "Bridge" Daemon
A DHARMA node can run in "Bridge Mode".
-   It holds a **private key** authorized to sign Responses.
-   It runs **adapters** (Rust/Python/Node scripts) that handle specific topics.
-   It is stateless (mostly). It just transforms `Request -> API Call -> Response`.
-   For Async, it maintains the **Oracle Job Queue** described above.

This architecture keeps the DHARMA Kernel small and secure, while allowing infinite extensibility.
