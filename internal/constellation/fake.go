package constellation

import (
	"context"
	"sync"
)

// Fake is an in-memory Client used by tests. Programs populate it via Set
// keyed on (target, collection, path); GetBacklinks replays the configured
// links one by one and honors yield's stop signal.
type Fake struct {
	mu    sync.Mutex
	links map[fakeKey][]Link
	// FailOnce, if a target is present, makes the next GetBacklinks call
	// for that target return an error before clearing the entry. Tests use
	// this to exercise retry / resume.
	FailOnce map[string]bool
}

type fakeKey struct {
	target, collection, path string
}

func NewFake() *Fake {
	return &Fake{links: map[fakeKey][]Link{}, FailOnce: map[string]bool{}}
}

// Set installs the links the next GetBacklinks(target, collection, path) will
// stream. Subsequent Set calls overwrite.
func (f *Fake) Set(target, collection, path string, links []Link) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]Link, len(links))
	copy(cp, links)
	f.links[fakeKey{target, collection, path}] = cp
}

func (f *Fake) GetBacklinks(ctx context.Context, target, collection, path string, yield func(Link) bool) error {
	f.mu.Lock()
	if f.FailOnce[target] {
		delete(f.FailOnce, target)
		f.mu.Unlock()
		return errFakeOnce{}
	}
	links := f.links[fakeKey{target, collection, path}]
	cp := make([]Link, len(links))
	copy(cp, links)
	f.mu.Unlock()

	for _, l := range cp {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !yield(l) {
			return nil
		}
	}
	return nil
}

type errFakeOnce struct{}

func (errFakeOnce) Error() string { return "constellation fake: induced one-time failure" }
