#![no_main]

use dharma_core::envelope::AssertionEnvelope;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = AssertionEnvelope::from_cbor(data);
});
