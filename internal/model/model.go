// Package model defines the persistent record types that flow between the
// jetstream/bootstrap producers and the parquet/duckdb consumers.
//
// Each record type maps 1:1 to a parquet file in raw/YYYY-MM-DD/ and feeds the
// downstream snapshot DuckDB tables. Field naming favors clarity over brevity
// and matches the column names used in the final DuckDB schema so the
// `read_parquet → CREATE TABLE` step in snapshot is essentially a passthrough.
//
// Conventions:
//   - All timestamps are UTC.
//   - IDs are deterministic int64 derived via internal/intern (xxhash64); the
//     original strings travel alongside in actors/posts so collisions can be
//     detected at snapshot time.
//   - Op is "create" or "delete"; deletes carry minimal payload — usually just
//     the URI / DID and timestamps — but use the same struct so the writer per
//     collection stays simple.
//   - Source is "bootstrap" or "firehose"; useful for diagnostics and lets the
//     snapshot job tell baseline records from incremental ones.
package model

import "time"

// Op classifies a record as a creation or a tombstone.
type Op string

const (
	OpCreate Op = "create"
	OpDelete Op = "delete"
)

// Source records which pipeline produced a record.
type Source string

const (
	SourceBootstrap Source = "bootstrap"
	SourceFirehose  Source = "firehose"
)

// Profile captures app.bsky.actor.profile records.
type Profile struct {
	DID         string    `json:"did"`
	DIDID       int64     `json:"did_id"`
	Handle      string    `json:"handle,omitempty"`
	DisplayName string    `json:"display_name,omitempty"`
	Description string    `json:"description,omitempty"`
	AvatarCID   string    `json:"avatar_cid,omitempty"`
	BannerCID   string    `json:"banner_cid,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	IndexedAt   time.Time `json:"indexed_at"`
	Op          Op        `json:"op"`
	Source      Source    `json:"source"`
}

// Follow captures app.bsky.graph.follow records.
type Follow struct {
	SrcDID    string    `json:"src_did"`
	SrcDIDID  int64     `json:"src_did_id"`
	DstDID    string    `json:"dst_did"`
	DstDIDID  int64     `json:"dst_did_id"`
	RKey      string    `json:"rkey"`
	CreatedAt time.Time `json:"created_at"`
	IndexedAt time.Time `json:"indexed_at"`
	Op        Op        `json:"op"`
	Source    Source    `json:"source"`
}

// Block captures app.bsky.graph.block records (same shape as Follow).
type Block struct {
	SrcDID    string    `json:"src_did"`
	SrcDIDID  int64     `json:"src_did_id"`
	DstDID    string    `json:"dst_did"`
	DstDIDID  int64     `json:"dst_did_id"`
	RKey      string    `json:"rkey"`
	CreatedAt time.Time `json:"created_at"`
	IndexedAt time.Time `json:"indexed_at"`
	Op        Op        `json:"op"`
	Source    Source    `json:"source"`
}

// Post captures app.bsky.feed.post records.
//
// Langs and Labels are stored as comma-joined strings rather than list columns
// for portability across parquet readers. Snapshot splits them back into
// arrays when building the final DuckDB tables.
type Post struct {
	URI             string    `json:"uri"`
	URIID           int64     `json:"uri_id"`
	DID             string    `json:"did"`
	DIDID           int64     `json:"did_id"`
	RKey            string    `json:"rkey"`
	CID             string    `json:"cid,omitempty"`
	Text            string    `json:"text"`
	Langs           string    `json:"langs"`  // comma-separated
	Labels          string    `json:"labels"` // comma-separated
	ReplyParentURI  string    `json:"reply_parent_uri,omitempty"`
	ReplyParentID   int64     `json:"reply_parent_uri_id,omitempty"`
	ReplyRootURI    string    `json:"reply_root_uri,omitempty"`
	ReplyRootURIID  int64     `json:"reply_root_uri_id,omitempty"`
	QuoteParentURI  string    `json:"quote_parent_uri,omitempty"`
	QuoteParentID   int64     `json:"quote_parent_uri_id,omitempty"`
	HasMedia        bool      `json:"has_media"`
	CreatedAt       time.Time `json:"created_at"`
	IndexedAt       time.Time `json:"indexed_at"`
	Op              Op        `json:"op"`
	Source          Source    `json:"source"`
}

// PostMedia captures embed media references derived from post records.
//
// We deliberately drop alt text and pixel content; only the link, kind, and
// position survive — enough to count "posts with media" and group by media
// type without retaining user content beyond the post text itself.
type PostMedia struct {
	PostURI    string    `json:"post_uri"`
	PostURIID  int64     `json:"post_uri_id"`
	DID        string    `json:"did"`
	DIDID      int64     `json:"did_id"`
	Index      int32     `json:"idx"` // position within the embed list
	MediaType  string    `json:"media_type"` // "image" | "video" | "external" | "record"
	URL        string    `json:"url,omitempty"`
	BlobCID    string    `json:"blob_cid,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	IndexedAt  time.Time `json:"indexed_at"`
}

// Like captures app.bsky.feed.like records.
type Like struct {
	ActorDID    string    `json:"actor_did"`
	ActorDIDID  int64     `json:"actor_did_id"`
	SubjectURI  string    `json:"subject_uri"`
	SubjectID   int64     `json:"subject_uri_id"`
	RKey        string    `json:"rkey"`
	CreatedAt   time.Time `json:"created_at"`
	IndexedAt   time.Time `json:"indexed_at"`
	Op          Op        `json:"op"`
	Source      Source    `json:"source"`
}

// Repost captures app.bsky.feed.repost records (same shape as Like).
type Repost struct {
	ActorDID    string    `json:"actor_did"`
	ActorDIDID  int64     `json:"actor_did_id"`
	SubjectURI  string    `json:"subject_uri"`
	SubjectID   int64     `json:"subject_uri_id"`
	RKey        string    `json:"rkey"`
	CreatedAt   time.Time `json:"created_at"`
	IndexedAt   time.Time `json:"indexed_at"`
	Op          Op        `json:"op"`
	Source      Source    `json:"source"`
}

// Collection enumerates the ATProto record collections this snapshotter cares about.
type Collection string

const (
	CollectionProfile Collection = "app.bsky.actor.profile"
	CollectionFollow  Collection = "app.bsky.graph.follow"
	CollectionBlock   Collection = "app.bsky.graph.block"
	CollectionPost    Collection = "app.bsky.feed.post"
	CollectionLike    Collection = "app.bsky.feed.like"
	CollectionRepost  Collection = "app.bsky.feed.repost"
)

// AllCollections is the firehose subscription set.
var AllCollections = []Collection{
	CollectionProfile,
	CollectionFollow,
	CollectionBlock,
	CollectionPost,
	CollectionLike,
	CollectionRepost,
}
