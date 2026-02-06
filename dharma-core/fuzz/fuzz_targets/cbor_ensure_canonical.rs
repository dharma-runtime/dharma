#![no_main]

use dharma_core::cbor;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = cbor::ensure_canonical(data);
});
