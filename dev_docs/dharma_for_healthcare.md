# DHARMA for Healthcare

**Patient Sovereignty. Interoperability. Privacy.**
DHARMA solves the "Siloed Patient Data" crisis by giving the patient the keys.

## 1. The "Portable Record"
*   **Problem:** Your X-Ray is at Hospital A. Your Blood Test is at Lab B. Your Prescription is at Clinic C.
*   **DHARMA Solution:** The Patient Identity is the "Root".
    *   Subject: `Patient.HealthRecord`.
    *   Lab B asserts `BloodTestResult` to the Patient's subject.
    *   Hospital A asserts `Diagnosis`.
    *   **Result:** The patient has the *only* complete copy of their history.

## 2. Consent & Sharing
*   **Scenario:** Visiting a Specialist.
*   **DHARMA Solution:** Granting Access.
    *   Patient generates a **Read Token** (or wraps the Subject Key) for the Specialist.
    *   Specialist syncs the record instantly.
    *   Patient revokes the key after the visit.

## 3. Clinical Trials & Research
*   **Scenario:** Proving a drug works without leaking patient names.
*   **DHARMA Solution:** Zero-Knowledge Proofs (Future) or Pseudonymous Data.
    *   Patients submit data to a `Research` subject using a random one-time key.
    *   The data is signed and valid, but the identity is protected.

## 4. Supply Chain Safety (Pharma)
*   **Problem:** Counterfeit drugs.
*   **DHARMA Solution:** Track and Trace.
    *   Manufacturer signs `BatchCreated`.
    *   Distributor signs `Received`.
    *   Pharmacist signs `Dispensed`.
    *   Patient scans the box: "This path is unbroken."

**DHARMA restores the Hippocratic Oath to data:**
"First, do no harm." (By not leaking it).
