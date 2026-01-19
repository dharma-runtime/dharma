# What DHARMA Cannot Do

Knowing what *not* to build is as important as knowing what to build. DHARMA is optimized for **Human-Speed Commitments**, not machine-speed streams.

## 1. High-Frequency Trading (HFT)
*   **Why:** DHARMA is **Async**. It relies on network propagation and signature verification.
*   **Constraint:** It cannot achieve microsecond latency. It cannot guarantee atomic arbitrage across global markets.
*   **Use:** Centralized matching engines. Use DHARMA for the *settlement*, not the *trade*.

## 2. Real-Time Multiplayer Games (FPS)
*   **Why:** DHARMA has encryption and signature overhead on every message.
*   **Constraint:** It is too heavy for 60Hz state updates (Call of Duty).
*   **Use:** UDP state compression. Use DHARMA for the *inventory* (skins, loot), not the *bullets*.

## 3. "The World's Truth" (Global Singleton)
*   **Why:** DHARMA does not enforce a single global ordering of events for 8 billion people.
*   **Constraint:** You cannot build a "Global DNS" that is instantly consistent for everyone. You will have forks and eventual consistency.
*   **Use:** Blockchains (Ethereum) if you need absolute global scarcity (e.g., a single unique NFT art piece).

## 4. Ephemeral Streaming (Netflix/Zoom)
*   **Why:** DHARMA stores history.
*   **Constraint:** You do not want to "Sign and Store" every video frame of a call. That creates petabytes of garbage.
*   **Use:** WebRTC/RTMP. Use DHARMA for the *signaling* ("Call started", "Call ended"), not the *media*.

## 5. Big Data Processing (Snowflake)
*   **Why:** DHARMA is row-oriented and cryptographically heavy.
*   **Constraint:** It is designed for "Business Data" (Millions of rows), not "Telemetry" (Trillions of rows).
*   **Use:** Parquet/Arrow on S3. Use DHARMA to track the *metadata* and *provenance* of those datasets.

## Summary
**DHARMA is for:** Contracts, Tasks, Invoices, Votes, decisions.
**DHARMA is not for:** Physics simulations, raw streams, or sub-millisecond race conditions.
