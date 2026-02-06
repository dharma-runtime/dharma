#![no_main]

use dharma_core::sync::SyncMessage;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = SyncMessage::from_cbor(data);
});
