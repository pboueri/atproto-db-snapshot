// Package parse holds the typed records emitted by repo crawlers and
// consumed by the graph writer. The types are intentionally minimal —
// all decoding happens in the producer (currently internal/pds), and
// consumers (currently internal/build) only read these structs.
package parse

import "time"

// Records holds everything extracted for a single DID. DIDs in
// *_did / subject fields are raw — interning happens downstream.
type Records struct {
	AuthorDID string

	Profile *ProfileRec

	Follows []FollowRec
	Blocks  []BlockRec
}

// ProfileRec is the subset of app.bsky.actor.profile we persist.
type ProfileRec struct {
	DisplayName *string
	Description *string
	AvatarCID   *string
	CreatedAt   *time.Time
}

// FollowRec mirrors a single app.bsky.graph.follow record.
type FollowRec struct {
	Rkey      string
	TargetDID string
	CreatedAt time.Time
}

// BlockRec mirrors a single app.bsky.graph.block record.
type BlockRec struct {
	Rkey      string
	TargetDID string
	CreatedAt time.Time
}

// ParseAtTime tolerates the assorted ISO-8601 shapes ATProto clients emit.
// Returns zero time if parsing fails — the record is kept but timestamped
// null-ish.
func ParseAtTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
