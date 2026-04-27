use anyhow::{anyhow, Result};
use bincode::Options;
use serde::{Deserialize, Serialize};

pub const CF_DID_IDS: &str = "did_ids";
pub const CF_TARGET_IDS: &str = "target_ids";
pub const CF_TARGET_LINKS: &str = "target_links";
pub const CF_LINK_TARGETS: &str = "link_targets";

pub fn bincode_opts() -> impl bincode::Options + Copy {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_varint_encoding()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Did(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Collection(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RPath(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RKey(pub String);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DidId(pub u64);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TargetId(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DidIdValue(pub DidId, pub bool);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Target(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TargetKey(pub Target, pub Collection, pub RPath);

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TargetLinkers(pub Vec<(DidId, RKey)>);

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordLinkKey(pub DidId, pub Collection, pub RKey);

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RecordLinkTargets(pub Vec<RecordLinkTarget>);

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordLinkTarget(pub RPath, pub TargetId);

pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    bincode_opts()
        .deserialize::<T>(bytes)
        .map_err(|e| anyhow!("bincode decode failed: {e}"))
}

pub fn decode_did(key: &[u8]) -> Result<Did> {
    decode(key)
}

pub fn decode_did_id_value(value: &[u8]) -> Result<DidIdValue> {
    decode(value)
}

pub fn decode_target_key(key: &[u8]) -> Result<TargetKey> {
    decode(key)
}

pub fn decode_record_link_key(key: &[u8]) -> Result<RecordLinkKey> {
    decode(key)
}

pub fn decode_record_link_targets(value: &[u8]) -> Result<RecordLinkTargets> {
    decode(value)
}

pub fn decode_did_id_be(key: &[u8]) -> Option<DidId> {
    if key.len() != 8 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(key);
    Some(DidId(u64::from_be_bytes(buf)))
}

pub fn target_id_be_key(id: TargetId) -> [u8; 8] {
    id.0.to_be_bytes()
}

pub fn parse_at_uri(uri: &str) -> Option<AtUri> {
    let rest = uri.strip_prefix("at://")?;
    let mut parts = rest.splitn(3, '/');
    let did = parts.next()?.to_string();
    let collection = parts.next()?.to_string();
    let rkey = parts.next()?.to_string();
    Some(AtUri {
        did,
        collection,
        rkey,
    })
}

#[derive(Debug, Clone)]
pub struct AtUri {
    pub did: String,
    pub collection: String,
    pub rkey: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_uri_works() {
        let u = parse_at_uri("at://did:plc:abc/app.bsky.feed.post/3jzabcdefghij").unwrap();
        assert_eq!(u.did, "did:plc:abc");
        assert_eq!(u.collection, "app.bsky.feed.post");
        assert_eq!(u.rkey, "3jzabcdefghij");
    }

    #[test]
    fn rejects_non_at_uri() {
        assert!(parse_at_uri("https://example.com").is_none());
    }

    #[test]
    fn round_trip_did() {
        let d = Did("did:plc:foo".to_string());
        let bytes = bincode_opts().serialize(&d).unwrap();
        let back: Did = decode(&bytes).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn round_trip_target_key() {
        let tk = TargetKey(
            Target("at://did:plc:foo/app.bsky.feed.post/3j".to_string()),
            Collection("app.bsky.feed.like".to_string()),
            RPath(".subject.uri".to_string()),
        );
        let bytes = bincode_opts().serialize(&tk).unwrap();
        let back: TargetKey = decode(&bytes).unwrap();
        assert_eq!(tk, back);
    }

    #[test]
    fn round_trip_did_id_value() {
        let v = DidIdValue(DidId(42), false);
        let bytes = bincode_opts().serialize(&v).unwrap();
        let back: DidIdValue = decode(&bytes).unwrap();
        assert_eq!(back.0 .0, 42);
        assert!(!back.1);
    }
}
