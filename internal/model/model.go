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
//
// Parquet tags (`parquet:"name,..."`) define the on-disk column name and the
// codec. We use zstd everywhere — it pays for itself many times over on text
// columns and DuckDB reads it natively.
package model

import "time"

// Op values classify a record as a creation or a tombstone. Stored as a
// plain string so parquet-go writes a vanilla BYTE_ARRAY column.
const (
	OpCreate = "create"
	OpDelete = "delete"
)

// Source values record which pipeline produced a record.
const (
	SourceBootstrap = "bootstrap"
	SourceFirehose  = "firehose"
)

// Profile captures app.bsky.actor.profile records.
type Profile struct {
	DID         string    `json:"did" parquet:"did,zstd"`
	DIDID       int64     `json:"did_id" parquet:"did_id,zstd"`
	Handle      string    `json:"handle,omitempty" parquet:"handle,zstd"`
	DisplayName string    `json:"display_name,omitempty" parquet:"display_name,zstd"`
	Description string    `json:"description,omitempty" parquet:"description,zstd"`
	AvatarCID   string    `json:"avatar_cid,omitempty" parquet:"avatar_cid,zstd"`
	BannerCID   string    `json:"banner_cid,omitempty" parquet:"banner_cid,zstd"`
	CreatedAt   time.Time `json:"created_at" parquet:"created_at,timestamp(microsecond),zstd"`
	IndexedAt   time.Time `json:"indexed_at" parquet:"indexed_at,timestamp(microsecond),zstd"`
	Op          string    `json:"op" parquet:"op,zstd"`
	Source      string    `json:"source" parquet:"source,zstd"`
}

// Follow captures app.bsky.graph.follow records.
type Follow struct {
	SrcDID    string    `json:"src_did" parquet:"src_did,zstd"`
	SrcDIDID  int64     `json:"src_did_id" parquet:"src_did_id,zstd"`
	DstDID    string    `json:"dst_did" parquet:"dst_did,zstd"`
	DstDIDID  int64     `json:"dst_did_id" parquet:"dst_did_id,zstd"`
	RKey      string    `json:"rkey" parquet:"rkey,zstd"`
	CreatedAt time.Time `json:"created_at" parquet:"created_at,timestamp(microsecond),zstd"`
	IndexedAt time.Time `json:"indexed_at" parquet:"indexed_at,timestamp(microsecond),zstd"`
	Op        string    `json:"op" parquet:"op,zstd"`
	Source    string    `json:"source" parquet:"source,zstd"`
}

// Block captures app.bsky.graph.block records (same shape as Follow).
type Block struct {
	SrcDID    string    `json:"src_did" parquet:"src_did,zstd"`
	SrcDIDID  int64     `json:"src_did_id" parquet:"src_did_id,zstd"`
	DstDID    string    `json:"dst_did" parquet:"dst_did,zstd"`
	DstDIDID  int64     `json:"dst_did_id" parquet:"dst_did_id,zstd"`
	RKey      string    `json:"rkey" parquet:"rkey,zstd"`
	CreatedAt time.Time `json:"created_at" parquet:"created_at,timestamp(microsecond),zstd"`
	IndexedAt time.Time `json:"indexed_at" parquet:"indexed_at,timestamp(microsecond),zstd"`
	Op        string    `json:"op" parquet:"op,zstd"`
	Source    string    `json:"source" parquet:"source,zstd"`
}

// Post captures app.bsky.feed.post records.
//
// Langs and Labels are stored as comma-joined strings rather than list columns
// for portability across parquet readers. Snapshot splits them back into
// arrays via DuckDB's string_split when building the final tables.
type Post struct {
	URI            string    `json:"uri" parquet:"uri,zstd"`
	URIID          int64     `json:"uri_id" parquet:"uri_id,zstd"`
	DID            string    `json:"did" parquet:"did,zstd"`
	DIDID          int64     `json:"did_id" parquet:"did_id,zstd"`
	RKey           string    `json:"rkey" parquet:"rkey,zstd"`
	CID            string    `json:"cid,omitempty" parquet:"cid,zstd"`
	Text           string    `json:"text" parquet:"text,zstd"`
	Langs          string    `json:"langs" parquet:"langs,zstd"`
	Labels         string    `json:"labels" parquet:"labels,zstd"`
	ReplyParentURI string    `json:"reply_parent_uri,omitempty" parquet:"reply_parent_uri,zstd"`
	ReplyParentID  int64     `json:"reply_parent_uri_id,omitempty" parquet:"reply_parent_uri_id,zstd"`
	ReplyRootURI   string    `json:"reply_root_uri,omitempty" parquet:"reply_root_uri,zstd"`
	ReplyRootID    int64     `json:"reply_root_uri_id,omitempty" parquet:"reply_root_uri_id,zstd"`
	QuoteParentURI string    `json:"quote_parent_uri,omitempty" parquet:"quote_parent_uri,zstd"`
	QuoteParentID  int64     `json:"quote_parent_uri_id,omitempty" parquet:"quote_parent_uri_id,zstd"`
	HasMedia       bool      `json:"has_media" parquet:"has_media,zstd"`
	CreatedAt      time.Time `json:"created_at" parquet:"created_at,timestamp(microsecond),zstd"`
	IndexedAt      time.Time `json:"indexed_at" parquet:"indexed_at,timestamp(microsecond),zstd"`
	Op             string    `json:"op" parquet:"op,zstd"`
	Source         string    `json:"source" parquet:"source,zstd"`
}

// PostMedia captures embed media references derived from post records.
//
// We deliberately drop alt text and pixel content; only the link, kind, and
// position survive — enough to count "posts with media" and group by media
// type without retaining user content beyond the post text itself.
type PostMedia struct {
	PostURI   string    `json:"post_uri" parquet:"post_uri,zstd"`
	PostURIID int64     `json:"post_uri_id" parquet:"post_uri_id,zstd"`
	DID       string    `json:"did" parquet:"did,zstd"`
	DIDID     int64     `json:"did_id" parquet:"did_id,zstd"`
	Index     int32     `json:"idx" parquet:"idx,zstd"`
	MediaType string    `json:"media_type" parquet:"media_type,zstd"`
	URL       string    `json:"url,omitempty" parquet:"url,zstd"`
	BlobCID   string    `json:"blob_cid,omitempty" parquet:"blob_cid,zstd"`
	CreatedAt time.Time `json:"created_at" parquet:"created_at,timestamp(microsecond),zstd"`
	IndexedAt time.Time `json:"indexed_at" parquet:"indexed_at,timestamp(microsecond),zstd"`
}

// Like captures app.bsky.feed.like records.
type Like struct {
	ActorDID   string    `json:"actor_did" parquet:"actor_did,zstd"`
	ActorDIDID int64     `json:"actor_did_id" parquet:"actor_did_id,zstd"`
	SubjectURI string    `json:"subject_uri" parquet:"subject_uri,zstd"`
	SubjectID  int64     `json:"subject_uri_id" parquet:"subject_uri_id,zstd"`
	RKey       string    `json:"rkey" parquet:"rkey,zstd"`
	CreatedAt  time.Time `json:"created_at" parquet:"created_at,timestamp(microsecond),zstd"`
	IndexedAt  time.Time `json:"indexed_at" parquet:"indexed_at,timestamp(microsecond),zstd"`
	Op         string    `json:"op" parquet:"op,zstd"`
	Source     string    `json:"source" parquet:"source,zstd"`
}

// Repost captures app.bsky.feed.repost records (same shape as Like).
type Repost struct {
	ActorDID   string    `json:"actor_did" parquet:"actor_did,zstd"`
	ActorDIDID int64     `json:"actor_did_id" parquet:"actor_did_id,zstd"`
	SubjectURI string    `json:"subject_uri" parquet:"subject_uri,zstd"`
	SubjectID  int64     `json:"subject_uri_id" parquet:"subject_uri_id,zstd"`
	RKey       string    `json:"rkey" parquet:"rkey,zstd"`
	CreatedAt  time.Time `json:"created_at" parquet:"created_at,timestamp(microsecond),zstd"`
	IndexedAt  time.Time `json:"indexed_at" parquet:"indexed_at,timestamp(microsecond),zstd"`
	Op         string    `json:"op" parquet:"op,zstd"`
	Source     string    `json:"source" parquet:"source,zstd"`
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
