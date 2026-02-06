#![no_main]

use dharma_core::net::handshake::Session;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut recv_key = [0u8; 32];
    let key_len = data.len().min(32);
    recv_key[..key_len].copy_from_slice(&data[..key_len]);

    let mut session = Session::new([0u8; 32], recv_key);
    let _ = session.decrypt(data);

    let msg_type = data.get(32).copied().unwrap_or(0);
    let payload_start = data.len().min(33);
    let payload_end = data.len().min(payload_start + 4096);
    let payload = &data[payload_start..payload_end];

    let mut sender = Session::new(recv_key, [0u8; 32]);
    if let Ok(mut frame) = sender.encrypt(msg_type, payload) {
        if !frame.is_empty() {
            let flip_index = data.get(payload_end).copied().unwrap_or(0) as usize % frame.len();
            let flip_mask = data.get(payload_end + 1).copied().unwrap_or(0xff);
            frame[flip_index] ^= flip_mask;
        }
        let mut receiver = Session::new([0u8; 32], recv_key);
        let _ = receiver.decrypt(&frame);
    }
});
