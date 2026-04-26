// Package archive emits the per-day parquet event log files (and the
// per-day _manifest.json) defined in spec §6.
//
// Inputs are typed Go structs — one collection's events per WriteX call.
// Output is a flat directory of zstd-compressed parquet files, one per
// non-empty collection, plus _manifest.json. Path layout matches §6:
//
//	<dayDir>/posts.parquet
//	<dayDir>/likes.parquet
//	...
//	<dayDir>/_manifest.json
//
// Internally we stage rows into an in-memory DuckDB via the Appender API
// (fast bulk load) and then COPY ... TO ... (FORMAT PARQUET, COMPRESSION
// zstd, COMPRESSION_LEVEL 6, ROW_GROUP_SIZE_BYTES 134217728). Sort key
// per §6: did (posts/profile_updates/deletions) or
// liker_did/reposter_did/src_did (edges).
package archive

import "time"

// Day is a UTC calendar day in "YYYY-MM-DD" form. The day's date is
// written into the `date DATE` column on every row of every parquet file.
type Day string

// PostEvent is one row of posts.parquet (§6).
type PostEvent struct {
	EventType      string // "create" | "update"
	URI            string
	DID            string
	Rkey           string
	CID            string
	EventTS        time.Time
	RecordTS       time.Time
	Text           string
	Lang           string
	ReplyRootURI   string
	ReplyParentURI string
	QuoteURI       string
	EmbedType      string
}

// PostEmbedEvent is one row of post_embeds.parquet (002 §1.2). Emitted
// only for posts that have an embed; posts without an embed produce no
// row in this file.
type PostEmbedEvent struct {
	URI               string
	DID               string
	Rkey              string
	EventTS           time.Time
	Kind              string // see 002 §1.1 (e.g. "recordWithMedia:images")
	ExternalURI       string
	ExternalDomain    string
	ExternalTitle     string
	ImageCount        int
	ImageWithAltCount int
	HasVideo          bool // distinguishes "no video" from "video, no alt"
	VideoHasAlt       bool
}

// LikeEvent is one row of likes.parquet.
type LikeEvent struct {
	EventType  string
	URI        string
	LikerDID   string
	SubjectURI string
	SubjectCID string
	EventTS    time.Time
	RecordTS   time.Time
}

// RepostEvent is one row of reposts.parquet.
type RepostEvent struct {
	EventType   string
	URI         string
	ReposterDID string
	SubjectURI  string
	SubjectCID  string
	EventTS     time.Time
	RecordTS    time.Time
}

// FollowEvent is one row of follows.parquet.
type FollowEvent struct {
	EventType string
	URI       string
	SrcDID    string
	DstDID    string
	EventTS   time.Time
	RecordTS  time.Time
}

// BlockEvent is one row of blocks.parquet.
type BlockEvent struct {
	EventType string
	URI       string
	SrcDID    string
	DstDID    string
	EventTS   time.Time
	RecordTS  time.Time
}

// ProfileUpdateEvent is one row of profile_updates.parquet.
type ProfileUpdateEvent struct {
	DID         string
	Handle      string
	DisplayName string
	Description string
	AvatarCID   string
	EventTS     time.Time
}

// DeleteEvent is one row of deletions.parquet.
type DeleteEvent struct {
	Collection string
	URI        string
	DID        string
	EventTS    time.Time
}

// Manifest is the on-disk shape of `_manifest.json` per §6.
type Manifest struct {
	Date                 Day              `json:"date"`
	SchemaVersion        string           `json:"schema_version"`
	JetstreamCursorStart int64            `json:"jetstream_cursor_start"`
	JetstreamCursorEnd   int64            `json:"jetstream_cursor_end"`
	BuiltAt              time.Time        `json:"built_at"`
	RowCounts            map[string]int64 `json:"row_counts"`
	Bytes                map[string]int64 `json:"bytes"`
}
