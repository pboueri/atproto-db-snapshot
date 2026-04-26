package registry

import (
	"sort"
	"sync"
	"time"
)

// Registry is an in-memory DID → actor_id map. For the 10k-DID bootstrap
// this is fine (max ~10M DIDs including like/follow targets). A persistent
// SQLite-backed implementation is deferred to the long-running path.
type Registry struct {
	mu    sync.Mutex
	byDID map[string]int64
	next  int64
	// FirstSeen tracks when we first minted an id for this DID.
	FirstSeen map[int64]time.Time
}

func New() *Registry {
	return &Registry{
		byDID:     make(map[string]int64, 16384),
		next:      1,
		FirstSeen: make(map[int64]time.Time, 16384),
	}
}

// GetOrAssign returns the actor_id for did, minting one if absent.
func (r *Registry) GetOrAssign(did string) int64 {
	id, _ := r.GetOrAssignFresh(did)
	return id
}

// GetOrAssignFresh returns the actor_id for did, minting one if absent.
// fresh is true if this call minted a new id.
func (r *Registry) GetOrAssignFresh(did string) (id int64, fresh bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.byDID[did]; ok {
		return existing, false
	}
	id = r.next
	r.next++
	r.byDID[did] = id
	r.FirstSeen[id] = time.Now().UTC()
	return id, true
}

// Reload seeds the registry from a previously-persisted set of entries.
// Called on graph-backfill resume so freshly-minted ids continue past the
// last persisted actor_id. Safe to call only on an empty Registry.
func (r *Registry) Reload(entries []Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range entries {
		r.byDID[e.DID] = e.ActorID
		r.FirstSeen[e.ActorID] = e.FirstSeen
		if e.ActorID >= r.next {
			r.next = e.ActorID + 1
		}
	}
}

// Len reports the number of DIDs in the registry.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byDID)
}

// Snapshot returns a stable slice of (actor_id, did, first_seen) tuples.
// Caller should not mutate. Ordered by actor_id ascending.
type Entry struct {
	ActorID   int64
	DID       string
	FirstSeen time.Time
}

func (r *Registry) Snapshot() []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Entry, 0, len(r.byDID))
	for did, id := range r.byDID {
		out = append(out, Entry{ActorID: id, DID: did, FirstSeen: r.FirstSeen[id]})
	}
	// Sort by actor_id ascending — sorted integer PKs compress much better
	// under DuckDB's RLE + bit-packing than unsorted.
	sort.Slice(out, func(i, j int) bool { return out[i].ActorID < out[j].ActorID })
	return out
}
