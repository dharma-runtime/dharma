# DHARMA for National Security

**High Assurance. Compartmentalization. Resilience.**
DHARMA is designed to operate in "Denied Environments" where the internet is untrusted or unavailable.

## 1. The "Disconnected Field" Problem
*   **Scenario:** A submarine or a forward operating base. Satellite link is down.
*   **DHARMA Solution:** Local-First.
    *   The commander issues orders (`action.Order`).
    *   The team executes (`action.Report`).
    *   The local mesh syncs via radio/LAN.
    *   When the satellite connects, the history bursts to HQ. Nothing is lost.

## 2. Compartmentalization (Need-to-Know)
*   **Problem:** Preventing leaks (Snowden/Manning).
*   **DHARMA Solution:** Encryption Layers.
    *   Every "Mission" is a separate Subject with unique keys.
    *   Access is granted via **Capability Tokens** (Task 22).
    *   Revocation is cryptographic (`Key Rotation`). Once a key is rotated, a compromised actor cannot decrypt future messages.

## 3. Provenance & Chain of Custody
*   **Problem:** Intelligence requires verifying the source. "Who took this photo? When?"
*   **DHARMA Solution:** Immutable Lineage.
    *   The camera signs the photo (Hardware Key).
    *   The analyst signs the report.
    *   The commander signs the decision.
    *   **Result:** A perfect, unbroken chain of evidence from sensor to shooter.

## 4. "Darknet" Operations
*   **Scenario:** Covert communication.
*   **DHARMA Solution:** Relays.
    *   DHARMA traffic looks like random encrypted noise (`Noise_XX`).
    *   It runs over any transport (TCP, UDP, USB stick).
    *   It leaves no metadata on central servers (because there are no central servers).

**DHARMA is the "Digital Backbone" for sovereign operations.**
It provides the durability of paper with the speed of a network.
