use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let wat = r#"
    (module
      (memory (export "memory") 1)
      (global $heap (mut i32) (i32.const 64))
      (global $len (mut i32) (i32.const 0))
      (func $alloc (export "alloc") (param $size i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $heap))
        (global.set $heap (i32.add (global.get $heap) (local.get $size)))
        (local.get $ptr)
      )
      (func (export "result_len") (result i32)
        (global.get $len)
      )
      (func (export "validate") (param i32 i32 i32 i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (call $write_result))
        (local.get $ptr)
      )
      (func (export "reduce") (param i32 i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (call $write_result))
        (local.get $ptr)
      )
      (func $write_result (result i32)
        (local $ptr i32)
        (local.set $ptr (call $alloc (i32.const 27)))
        ;; CBOR map: {"ok": true, "reason": null, "status": "accept"}
        (i32.store8 (local.get $ptr) (i32.const 0xa3))
        ;; "ok"
        (i32.store8 (i32.add (local.get $ptr) (i32.const 1)) (i32.const 0x62))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 2)) (i32.const 0x6f))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 3)) (i32.const 0x6b))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 4)) (i32.const 0xf5))
        ;; "reason"
        (i32.store8 (i32.add (local.get $ptr) (i32.const 5)) (i32.const 0x66))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 6)) (i32.const 0x72))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 7)) (i32.const 0x65))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 8)) (i32.const 0x61))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 9)) (i32.const 0x73))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 10)) (i32.const 0x6f))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 11)) (i32.const 0x6e))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 12)) (i32.const 0xf6))
        ;; "status"
        (i32.store8 (i32.add (local.get $ptr) (i32.const 13)) (i32.const 0x66))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 14)) (i32.const 0x73))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 15)) (i32.const 0x74))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 16)) (i32.const 0x61))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 17)) (i32.const 0x74))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 18)) (i32.const 0x75))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 19)) (i32.const 0x73))
        ;; "accept"
        (i32.store8 (i32.add (local.get $ptr) (i32.const 20)) (i32.const 0x66))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 21)) (i32.const 0x61))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 22)) (i32.const 0x63))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 23)) (i32.const 0x63))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 24)) (i32.const 0x65))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 25)) (i32.const 0x70))
        (i32.store8 (i32.add (local.get $ptr) (i32.const 26)) (i32.const 0x74))
        (global.set $len (i32.const 27))
        (local.get $ptr)
      )
    )
    "#;

    let bytes = wat::parse_str(wat).expect("valid wat");
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    fs::write(out_dir.join("accept.wasm"), bytes).expect("write wasm");
    println!("cargo:rerun-if-changed=build.rs");
}
