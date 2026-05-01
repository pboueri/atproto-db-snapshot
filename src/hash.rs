//! Synthetic 64-bit IDs for entity-level parquet output.
//!
//! Constellation already assigns a stable `did_id: u64` for each DID;
//! we preserve those everywhere we reference an actor. URIs (post AT
//! URIs, target strings) have no pre-existing dictionary, so we hash
//! them with xxhash3-64 to get a `u64` that's stable across runs and
//! identical between any two callers that hash the same string.
//!
//! Collisions are statistically rare (~ N²/2⁶⁴ — for N=10⁹ unique
//! URIs that's ~3 expected collisions) and detectable in DuckDB via
//! `GROUP BY uri_id HAVING COUNT(DISTINCT uri) > 1`. A fixup pass at
//! the start of hydrate remaps the loser of any collision.
//!
//! `id_for_record(did_id, rkey)` is a per-row primary key for
//! follow/block/like/repost rows, where the natural key (record's
//! author DID + rkey) is already unique.
use twox_hash::XxHash64;
use std::hash::Hasher;

/// `id_for_uri` is the canonical mapping URI string → uri_id used by
/// every URI column (`posts.uri_id`, `posts.reply_root_uri_id`,
/// `likes.subject_uri_id`, etc.). Two callers that pass the same
/// string get the same `u64` by construction; that's the only
/// invariant downstream joins depend on.
#[inline]
pub fn id_for_uri(uri: &str) -> u64 {
    let mut h = XxHash64::with_seed(0);
    h.write(uri.as_bytes());
    h.finish()
}

/// `id_for_record` is a per-row id for follow/block/like/repost
/// records, derived from the record's author `did_id` and `rkey`. It
/// is not used as a join key — it just exists so each row has a
/// deterministic primary key. The DOM separation tag prevents
/// collisions across entity types in case some downstream consumer
/// pools ids from multiple tables.
#[inline]
pub fn id_for_record(tag: &str, did_id: u64, rkey: &str) -> u64 {
    let mut h = XxHash64::with_seed(0);
    h.write(tag.as_bytes());
    h.write(&[0u8]);
    h.write(&did_id.to_le_bytes());
    h.write(rkey.as_bytes());
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_hash_is_stable() {
        let a = id_for_uri("at://did:plc:foo/app.bsky.feed.post/3xyz");
        let b = id_for_uri("at://did:plc:foo/app.bsky.feed.post/3xyz");
        assert_eq!(a, b);
        let c = id_for_uri("at://did:plc:foo/app.bsky.feed.post/3xyz0");
        assert_ne!(a, c);
    }

    #[test]
    fn record_id_distinguishes_tag() {
        let l = id_for_record("like", 42, "3xyz");
        let r = id_for_record("repost", 42, "3xyz");
        assert_ne!(l, r);
    }
}
