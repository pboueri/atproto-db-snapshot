package slingshot

import (
	"context"
	"sync"
)

// Fake is an in-memory Client used by tests. Programs populate it via
// Set; missing keys return ErrNotFound (matching production behavior so
// the bootstrap caller's "no profile → bare actor row" path is exercised).
type Fake struct {
	mu      sync.Mutex
	records map[fakeKey]Record
	// FailOnce, if a DID is present, makes the next GetRecord for that DID
	// return an error before clearing the entry.
	FailOnce map[string]bool
}

type fakeKey struct {
	did, collection, rkey string
}

func NewFake() *Fake {
	return &Fake{records: map[fakeKey]Record{}, FailOnce: map[string]bool{}}
}

// Set installs the record returned for (did, collection, rkey).
func (f *Fake) Set(did, collection, rkey string, rec Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records[fakeKey{did, collection, rkey}] = rec
}

func (f *Fake) GetRecord(ctx context.Context, did, collection, rkey string) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailOnce[did] {
		delete(f.FailOnce, did)
		return Record{}, errFakeOnce{}
	}
	rec, ok := f.records[fakeKey{did, collection, rkey}]
	if !ok {
		return Record{}, ErrNotFound
	}
	return rec, nil
}

type errFakeOnce struct{}

func (errFakeOnce) Error() string { return "slingshot fake: induced one-time failure" }
