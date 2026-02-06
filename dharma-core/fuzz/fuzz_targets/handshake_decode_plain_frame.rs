#![no_main]

use dharma_core::net::handshake;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = handshake::fuzz_decode_plain_frame(data);
});
