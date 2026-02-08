use crate::error::DharmaError;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const DEFAULT_MAX_FRAME_SIZE: usize = 1_048_576;
static MAX_FRAME_SIZE: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_FRAME_SIZE);

pub fn set_max_frame_size(size: usize) {
    if size > 0 {
        MAX_FRAME_SIZE.store(size, Ordering::Relaxed);
    }
}

pub fn max_frame_size() -> usize {
    MAX_FRAME_SIZE.load(Ordering::Relaxed)
}

pub fn read_frame(reader: &mut dyn Read) -> Result<Vec<u8>, DharmaError> {
    read_frame_optional(reader)?.ok_or_else(|| DharmaError::Network("unexpected eof".to_string()))
}

pub fn read_frame_optional(reader: &mut dyn Read) -> Result<Option<Vec<u8>>, DharmaError> {
    let mut len_buf = [0u8; 4];
    if !read_exact_or_eof(reader, &mut len_buf)? {
        return Ok(None);
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    let limit = max_frame_size();
    if len > limit {
        return Err(DharmaError::Network("frame too large".to_string()));
    }
    let mut buf = vec![0u8; len];
    if !read_exact_or_eof(reader, &mut buf)? {
        return Err(DharmaError::Network("unexpected eof".to_string()));
    }
    Ok(Some(buf))
}

pub fn write_frame(writer: &mut dyn Write, bytes: &[u8]) -> Result<(), DharmaError> {
    let limit = max_frame_size();
    if bytes.len() > limit {
        return Err(DharmaError::Network("frame too large".to_string()));
    }
    let len = bytes.len() as u32;
    let mut buf = Vec::with_capacity(4 + bytes.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(bytes);
    writer.write_all(&buf)?;
    writer.flush()?;
    Ok(())
}

fn read_exact_or_eof(reader: &mut dyn Read, buf: &mut [u8]) -> Result<bool, DharmaError> {
    let mut offset = 0;
    while offset < buf.len() {
        match reader.read(&mut buf[offset..]) {
            Ok(0) => {
                if offset == 0 {
                    return Ok(false);
                }
                return Err(DharmaError::Network("unexpected eof".to_string()));
            }
            Ok(n) => offset += n,
            Err(err) => {
                use std::io::ErrorKind;
                match err.kind() {
                    ErrorKind::WouldBlock | ErrorKind::TimedOut => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    _ => return Err(err.into()),
                }
            }
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_write_roundtrip() {
        let mut buffer = Vec::new();
        write_frame(&mut buffer, b"hello").unwrap();
        let mut cursor = std::io::Cursor::new(buffer);
        let out = read_frame(&mut cursor).unwrap();
        assert_eq!(out, b"hello");
    }

    #[test]
    fn read_frame_optional_eof() {
        let mut cursor = std::io::Cursor::new(Vec::new());
        let out = read_frame_optional(&mut cursor).unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn frame_size_limit_rejects_large_payloads() {
        set_max_frame_size(4);
        let mut buffer = Vec::new();
        assert!(write_frame(&mut buffer, b"hello").is_err());
        set_max_frame_size(DEFAULT_MAX_FRAME_SIZE);
    }

    #[test]
    fn read_frame_rejects_oversized_advertised_length() {
        let limit = max_frame_size().min(u32::MAX as usize - 1);
        let oversized = (limit + 1) as u32;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&oversized.to_be_bytes());
        let mut cursor = std::io::Cursor::new(bytes);
        let err = read_frame_optional(&mut cursor).unwrap_err();
        match err {
            DharmaError::Network(msg) => assert!(msg.contains("frame too large")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
