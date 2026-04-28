use chrono::{DateTime, Utc};

const TID_ALPHABET: &[u8; 32] = b"234567abcdefghijklmnopqrstuvwxyz";

const TID_INVALID: u8 = 0xFF;

const TID_DECODE: [u8; 256] = {
    let mut table = [TID_INVALID; 256];
    let alphabet: [u8; 32] = *TID_ALPHABET;
    let mut i = 0;
    while i < 32 {
        table[alphabet[i] as usize] = i as u8;
        i += 1;
    }
    table
};

pub fn decode_tid_micros(tid: &str) -> Option<i64> {
    if tid.len() != 13 {
        return None;
    }
    let mut value: u64 = 0;
    for &b in tid.as_bytes() {
        let idx = TID_DECODE[b as usize];
        if idx == TID_INVALID {
            return None;
        }
        value = (value << 5) | idx as u64;
    }
    let timestamp_micros = (value >> 10) as i64;
    Some(timestamp_micros)
}

pub fn tid_to_datetime(tid: &str) -> Option<DateTime<Utc>> {
    let micros = decode_tid_micros(tid)?;
    DateTime::<Utc>::from_timestamp_micros(micros)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_known_tid() {
        let micros = decode_tid_micros("3jzfcijpj2z2a").unwrap();
        assert!(micros > 1_600_000_000_000_000);
        assert!(micros < 2_500_000_000_000_000);
    }

    #[test]
    fn rejects_wrong_length() {
        assert!(decode_tid_micros("self").is_none());
        assert!(decode_tid_micros("").is_none());
    }

    #[test]
    fn rejects_invalid_char() {
        // '!' is not in the TID alphabet
        assert!(decode_tid_micros("3jzfcijpj2z2!").is_none());
        // uppercase is not in the alphabet either
        assert!(decode_tid_micros("3JZFCIJPJ2Z2A").is_none());
    }
}
