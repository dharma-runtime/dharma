# DHARMA for Commerce

**Frictionless B2B. Self-Driving Contracts.**
DHARMA automates the "Boring Backend" of the global economy.

## 1. The "Live Invoice"
*   **Problem:** Invoices are dead PDFs. You email them. You wait. You chase.
*   **DHARMA Solution:** The Invoice is a Shared Subject.
    *   Vendor asserts: `Invoice.Issue(items=[...])`.
    *   Client asserts: `Invoice.Approve`.
    *   Bank (Reactor) sees Approval -> Releases Payment -> Asserts `Invoice.Paid`.
    *   **Result:** Days Sales Outstanding (DSO) drops from 45 days to minutes.

## 2. Supply Chain Visibility
*   **Scenario:** Just-in-Time Manufacturing.
*   **DHARMA Solution:** Shared State.
    *   Supplier updates `Inventory` subject.
    *   Manufacturer's system subscribes to it.
    *   When `Inventory < Threshold`, Manufacturer automatically issues a `PurchaseOrder`.
    *   **Result:** No emails. No phone calls. The factories talk to each other.

## 3. Reputation & Credit
*   **Problem:** Getting a loan requires faxing 3 years of statements.
*   **DHARMA Solution:** Cryptographic Credit History.
    *   A business can prove: "I have paid 500 invoices on time."
    *   They share the **Read Key** to their `Invoices` subject with the Bank.
    *   The Bank verifies the signatures of the suppliers.
    *   **Result:** Instant, algorithmic credit scoring based on real trade data.

## 4. Gig Economy / Freelancers
*   **Scenario:** Getting paid for work.
*   **DHARMA Solution:** Escrow Contracts.
    *   Client deposits funds into a DHARMA Escrow subject.
    *   Freelancer delivers work.
    *   Client signs `Accept`.
    *   Escrow releases funds.
    *   **Safety:** If Client disappears, an Arbiter (defined in DHL) can resolve the dispute.

**DHARMA removes the "Trust Tax" from business.**
You don't need to trust your partner to pay. You trust the contract.
