package repo

import (
	"context"
	"sync"

	"github.com/pboueri/atproto-db-snapshot/internal/model"
)

// Fake is an in-memory Client used by tests. Programs populate it via Set
// and the bootstrap pipeline reads from it through the Client interface.
//
// The fake ignores the pds argument passed to ListRecords — production
// resolves it from PLC, but tests don't have a PLC roundtrip to plumb
// through and the fake's single-tenant identity is the (did, collection)
// key.
type Fake struct {
	mu      sync.Mutex
	records map[fakeKey][]Record
	// FailOnce, if non-empty, makes the next ListRecords call for any of the
	// listed DIDs return an error once before succeeding. Tests use this to
	// exercise the bootstrap retry / resume path.
	FailOnce map[string]bool
}

type fakeKey struct {
	did        string
	collection model.Collection
}

func NewFake() *Fake {
	return &Fake{records: map[fakeKey][]Record{}, FailOnce: map[string]bool{}}
}

// Set installs the records the next ListRecords(_, did, collection) call returns.
func (f *Fake) Set(did string, collection model.Collection, records []Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records[fakeKey{did, collection}] = records
}

func (f *Fake) ListRecords(ctx context.Context, _ string, did string, collection model.Collection) ([]Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailOnce[did] {
		delete(f.FailOnce, did)
		return nil, errFakeOnce
	}
	out := make([]Record, len(f.records[fakeKey{did, collection}]))
	copy(out, f.records[fakeKey{did, collection}])
	for i := range out {
		out[i].DID = did
		out[i].Collection = collection
		out[i].RKey = rkeyFromURI(out[i].URI)
	}
	return out, nil
}

type fakeOnceErr struct{}

func (fakeOnceErr) Error() string { return "repo fake: induced one-time failure" }

var errFakeOnce = fakeOnceErr{}
