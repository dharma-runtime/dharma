# Why DHARMA Exists

Software is suffering from a crisis of **Truth**.

In the traditional world (CRUD), data is a "bag of current values" held hostage by a central database. In this world, we lose the context of *why* things changed, *who* authorized them, and *what* the rules were at the moment of change.

DHARMA exists to move the industry from **Mutable State** to **Immutable Intent**.

---

## 1. Why CRUD Breaks
In a CRUD system (Create, Read, Update, Delete), if a balance changes from $100 to $50, the previous state is gone. We might have "Audit Logs," but they are a side-effect, often incomplete or easily tampered with.
**In DHARMA, the log IS the state.** You cannot change a value without signing an assertion that explains the transition.

## 2. Why Workflows Rot
Most business processes are hard-coded into Application Servers. When the rules change, the old data is suddenly "illegal" under the new code, or we write complex migrations that lose information.
**In DHARMA, logic is versioned alongside data.** We use "Lenses" to view history through the rules that governed it at the time.

## 3. Why Distributed Systems Lie
Microservices often "tell" each other things happened via message queues. But without a shared kernel of truth, these systems eventually diverge. "I sent it," "I never got it."
**In DHARMA, we don't send messages; we sync history.** If we have the same log, we have the same truth.

## 4. Why Event Sourcing Almost Worked
Event Sourcing (storing every change) is the right idea, but it's usually implemented inside a single database. It lacks **Cryptographic Sovereignty**.
**In DHARMA, every event is a signed commitment.** It doesn't matter *where* the data lives (your phone, my server, an IPFS node); the signature proves it is authentic.

---

## The DHARMA Promise
We are building a system where **Business Physics** are enforced by math, not just "good intentions." 

Audit is not bolted on.
Privacy is not an afterthought.
Trust is not assumed.

**Trust the Key. Verify the Law.**
