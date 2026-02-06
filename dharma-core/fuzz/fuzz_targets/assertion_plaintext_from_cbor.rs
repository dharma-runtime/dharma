#![no_main]

use dharma_core::assertion::AssertionPlaintext;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = AssertionPlaintext::from_cbor(data);
});
