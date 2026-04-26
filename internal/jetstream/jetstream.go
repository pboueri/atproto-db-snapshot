// Package jetstream subscribes to Bluesky's jetstream firehose.
//
// We define an internal Event type rather than reusing a third-party SDK
// because the schema we care about is a tiny subset of what jetstream emits
// and changing it shouldn't ripple into the rest of the codebase.
package jetstream

import (
	"context"
	"encoding/json"
	"time"
)

// Kind names the top-level event kind.
type Kind string

const (
	KindCommit   Kind = "commit"
	KindIdentity Kind = "identity"
	KindAccount  Kind = "account"
)

// Operation is the commit operation type.
type Operation string

const (
	OpCreate Operation = "create"
	OpUpdate Operation = "update"
	OpDelete Operation = "delete"
)

// Event is one decoded jetstream message — only the fields the snapshotter uses.
type Event struct {
	DID    string `json:"did"`
	TimeUS int64  `json:"time_us"`
	Kind   Kind   `json:"kind"`
	Commit *Commit `json:"commit,omitempty"`
}

// Commit captures the per-record commit payload.
type Commit struct {
	Rev        string          `json:"rev"`
	Operation  Operation       `json:"operation"`
	Collection string          `json:"collection"`
	RKey       string          `json:"rkey"`
	CID        string          `json:"cid,omitempty"`
	Record     json.RawMessage `json:"record,omitempty"`
}

// SubscribeOptions controls the subscription.
type SubscribeOptions struct {
	// Cursor is microseconds since epoch; zero means "now".
	Cursor int64
	// Collections subsets by NSID. Empty = all.
	Collections []string
	// DIDs subsets by author. Empty = all.
	DIDs []string
}

// Subscriber abstracts the jetstream connection.
//
// Subscribe streams events on the returned channel until ctx is cancelled or
// the stream errors. The channel closes when the stream ends; any error is
// reported through the returned error chan, which sends at most one value.
type Subscriber interface {
	Subscribe(ctx context.Context, opts SubscribeOptions) (<-chan Event, <-chan error)
}

// EventTime converts an Event's microsecond timestamp to a UTC time.Time.
func EventTime(e Event) time.Time {
	return time.UnixMicro(e.TimeUS).UTC()
}
