# Domain Ownership Journey

## Goal
Create a real-world domain (corp.ph.cmdv), obtain parent authorization, and onboard
employees, suppliers, and clients with scoped permissions.

## Prerequisites
- You have a keypair for the parent domain owner (corp.ph).
- You have a keypair for the child domain owner (corp.ph.cmdv).
- Your node can sync `sys.directory` from a seed relay.

## The End-to-End Journey

### 1) Parent domain exists (corp.ph)
If you already own `corp.ph`, the directory should include:
- `fabric.domain.register` for `corp.ph` with the parent owner key.
- `fabric.domain.policy` describing levels, features, and delegates.

If `corp.ph` does not exist, its owner must first register it in `sys.directory`.

### 2) Request the child domain (corp.ph.cmdv)
The founder of cmdv submits a request to the directory:
- `fabric.domain.request`
  - `domain = "corp.ph.cmdv"`
  - `parent = "corp.ph"`
  - `requester_key = <cmdv_owner_key>`
  - signed by the cmdv owner

### 3) Parent authorizes the request
The `corp.ph` owner approves the child:
- `fabric.domain.authorize`
  - `domain = "corp.ph.cmdv"`
  - `parent = "corp.ph"`
  - `request_id = <hash_of_request>`
  - `authorized_owner = <cmdv_owner_key>`
  - signed by the corp.ph owner

### 4) Register the child domain
The cmdv owner registers the domain:
- `fabric.domain.register`
  - `domain = "corp.ph.cmdv"`
  - `owner_key = <cmdv_owner_key>`
  - `parent_auth = <hash_of_authorize>`

Validation rule: this register is valid only if the parent authorization exists
and is signed by the current `corp.ph` owner key.

### 5) Publish the domain policy
The cmdv owner publishes a policy for `corp.ph.cmdv`:
- Levels: `admin`, `employee`, `supplier`, `client`, `public`
- Feature flags:
  - `allow_sync`: on/off
  - `allow_custom_query`: on/off
  - `allow_planq_replication`: on/off
- Allowed actions and predefined queries per level

This policy is the canonical gatekeeper for what the domain permits.

### 6) Add employees (trusted operators)
Issue CapTokens with `level=employee` or `level=admin`:
- Employees get full sync and local replication.
- Admins can issue other tokens if delegated.

Optionally: delegate token issuance by adding keys to the policy delegates list.

### 7) Add suppliers (restricted partners)
Issue CapTokens with `level=supplier`:
- Typically no custom queries.
- Limited predefined queries only.
- Optional sync restrictions (no full replication).

### 8) Add clients (public or semi-public)
Issue CapTokens with `level=client` or `public`:
- Only predefined queries
- No PlanQ replication
- Minimal action surface

### 9) Discovery and joining
- Nodes sync `sys.directory` from seeds to discover relays and policies.
- Clients present their CapToken to relays/providers.
- For “join requests,” the V1 workflow is: request out-of-band, then receive a
  scoped CapToken (future work can add on-chain join-request assertions).

### 10) Oracles and bridges
If a node advertises an oracle (e.g., email bridge for `corp.ph.cmdv`):
- It publishes an Ad with `oracles` and `policy_hash`.
- Access is enforced by CapTokens scoped to that domain and level.
- Oracle mode is one of: input-only, request/response, or output-only.

## Result
- The public chain remains consistent.
- The private domain is sovereign.
- Every participant receives only the authority they need.
