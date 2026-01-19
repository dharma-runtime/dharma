# Scalable Chat Architecture (Log vs State)

In DHARMA, developers must distinguish between **Consensus State** (Shared Memory) and **Event History** (The Log).
A Chat Application is the perfect case study for this distinction.

---

## 1. The Naive Approach (Don't do this)

The instinct is to store messages in a list within the aggregate state:

```dhl
aggregate Room
    state
        messages: List<Message> // DANGER

action Post(text: Text)
    apply
        state.messages.push(Message(text, context.signer))
```

### Why it fails
1.  **Memory Explosion:** Wasm memory is finite (e.g., 1MB - 4GB). A busy chat room with 100k messages will crash the contract execution (OOM).
2.  **Cost:** Every new message requires serializing/deserializing the *entire* list of previous messages during the state transition.
3.  **Latency:** Validation becomes `O(N)` with history size.

---

## 2. The Sovereign Approach (The Log is Truth)

In DHARMA, the **Log of Assertions** is stored on disk and can be infinite.
The **State** is merely a cached reduction for *Logic Validation* (e.g., "Is user allowed to post?").

### The Contract (`std.io.chat`)

The State tracks **Membership** (ACL), not Content.

```dhl
aggregate Room
    state
        members: Map<Identity, Role>
        last_active: Timestamp

action Post(text: Text, reply_to: Bytes?)
    validate
        state.members.contains(context.signer)
    apply
        // We only update metadata!
        state.last_active = now()
```

### Why it works
1.  **Constant Size:** State size = `O(Users)`, not `O(Messages)`.
2.  **Infinite History:** You can post 1 billion messages. They simply append to `log.bin`.
3.  **Fast Writes:** Validation is `O(1)`.

---

## 3. Reconstructing the Chat (The View)

If the state doesn't have the messages, how does the UI show them?
**The UI queries the Log, not the State.**

### Using DHARMA-Q
The Query Engine indexes the *Asserted Arguments*.

```dhlq
assertions
| where subject == <RoomID>
| where type == "action.Post"
| sort ts desc
| take 50
```

### Handling Reactions
Reactions are modifications to previous messages. Since Assertions are immutable, a Reaction is a *new* assertion referencing the old one.

```dhl
action React(target_hash: Bytes, emoji: Text)
    validate
        state.members.contains(context.signer)
```

**The UI Reducer:**
The frontend (Dioxus/React) receives a stream of events:
1.  `Post(hash=A, text="Hello")` -> Render Message A.
2.  `React(target=A, emoji="👍")` -> Find Message A, append emoji.

---

## 4. Summary Pattern

For **Unbounded Streams** (Chat, Activity Logs, Sensor Data):
-   **Store in State:** Only what is needed for *Permission Checks* (Members, Latest Timestamp, Counters).
-   **Store in Log:** The actual Content.
-   **Read:** Use DHARMA-Q or simple Log Scanning.
