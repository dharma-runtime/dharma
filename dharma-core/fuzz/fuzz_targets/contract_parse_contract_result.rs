#![no_main]

use dharma_core::contract;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = contract::fuzz_parse_contract_result(data);
});
