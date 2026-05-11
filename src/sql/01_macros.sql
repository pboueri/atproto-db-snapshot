-- TID-decode a 13-char rkey into the timestamp it encodes. Mirrors
-- src/tid.rs::decode_tid_micros: each base32 char contributes 5 bits to
-- a 65-bit value; the bottom 10 bits are a clock id (dropped). We skip
-- straight to the >>10 result so all arithmetic fits in BIGINT (i64).
--
-- Position i (0-indexed) holds bits 5*(12-i)..5*(12-i)+5 of the value.
-- After >>10, position i contributes alphabet_idx << (5*(12-i)-10) =
-- 5*(10-i) for i in 0..10. Positions 11 and 12 are the clock id bits and
-- contribute zero.
CREATE OR REPLACE MACRO tid_to_ts(rkey) AS (
  CASE WHEN length(rkey) = 13 THEN make_timestamp(
    ((instr('234567abcdefghijklmnopqrstuvwxyz', substring(rkey,  1, 1)) - 1)::BIGINT << 50) |
    ((instr('234567abcdefghijklmnopqrstuvwxyz', substring(rkey,  2, 1)) - 1)::BIGINT << 45) |
    ((instr('234567abcdefghijklmnopqrstuvwxyz', substring(rkey,  3, 1)) - 1)::BIGINT << 40) |
    ((instr('234567abcdefghijklmnopqrstuvwxyz', substring(rkey,  4, 1)) - 1)::BIGINT << 35) |
    ((instr('234567abcdefghijklmnopqrstuvwxyz', substring(rkey,  5, 1)) - 1)::BIGINT << 30) |
    ((instr('234567abcdefghijklmnopqrstuvwxyz', substring(rkey,  6, 1)) - 1)::BIGINT << 25) |
    ((instr('234567abcdefghijklmnopqrstuvwxyz', substring(rkey,  7, 1)) - 1)::BIGINT << 20) |
    ((instr('234567abcdefghijklmnopqrstuvwxyz', substring(rkey,  8, 1)) - 1)::BIGINT << 15) |
    ((instr('234567abcdefghijklmnopqrstuvwxyz', substring(rkey,  9, 1)) - 1)::BIGINT << 10) |
    ((instr('234567abcdefghijklmnopqrstuvwxyz', substring(rkey, 10, 1)) - 1)::BIGINT <<  5) |
    ((instr('234567abcdefghijklmnopqrstuvwxyz', substring(rkey, 11, 1)) - 1)::BIGINT)
  ) ELSE NULL END
);

-- URI / URL reconstruction (pure string ops). The snapshot stores
-- actors by numeric did_id and posts by xxhash3-64 uri_id; the
-- human-readable URI is reconstructable from (actors.did, posts.rkey).
-- These macros centralize the format so queries don't repeat the
-- concatenation (and don't drift if bsky.app ever changes its URL
-- shape). Use these in any query that already joins posts to actors.
-- For one-off uri_id → URL lookups, see 06_url_macros.sql which adds
-- correlated-subquery variants that do the join inline.

CREATE OR REPLACE MACRO post_at_uri(did, rkey) AS
  'at://' || did || '/app.bsky.feed.post/' || rkey;

CREATE OR REPLACE MACRO post_url(did, rkey) AS
  'https://bsky.app/profile/' || did || '/post/' || rkey;

CREATE OR REPLACE MACRO actor_url(did) AS
  'https://bsky.app/profile/' || did;
