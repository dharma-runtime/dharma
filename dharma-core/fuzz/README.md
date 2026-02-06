# dharma-core fuzzing

This directory contains `cargo-fuzz` targets for parser entry points that consume untrusted input.

## Prerequisites

- Rust nightly toolchain
- `cargo-fuzz` (`cargo install cargo-fuzz --locked`)

## Run locally

From this directory:

```bash
cargo +nightly fuzz run cbor_ensure_canonical
```

Or run a short bounded session:

```bash
cargo +nightly fuzz run cbor_ensure_canonical -- -max_total_time=30
```

## Targets

- `cbor_ensure_canonical`
- `cbor_decode_value`
- `assertion_plaintext_from_cbor`
- `assertion_envelope_from_cbor`
- `handshake_decode_plain_frame`
- `handshake_session_decrypt`
- `contract_parse_contract_result`
- `sync_message_from_cbor`

## Seed corpus

Seed inputs are committed under `corpus/<target>/seed-*.cbor`.

- Existing vectors are copied from `tests/vectors`.
- Additional seeds cover handshake frame parsing, sync message parsing, and contract result parsing.
