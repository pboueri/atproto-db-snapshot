package plc

import (
	"context"
	"time"
)

// Fake is an in-memory Directory used by tests.
type Fake struct {
	Entries []Entry
}

// NewFake builds a Fake from a slice of DIDs, assigning monotonically
// increasing createdAt timestamps starting at base.
func NewFake(base time.Time, dids []string) *Fake {
	f := &Fake{}
	for i, d := range dids {
		f.Entries = append(f.Entries, Entry{DID: d, CreatedAt: base.Add(time.Duration(i) * time.Second)})
	}
	return f
}

func (f *Fake) Stream(ctx context.Context, since time.Time, yield func(Entry) bool) error {
	for _, e := range f.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !e.CreatedAt.After(since) {
			continue
		}
		if !yield(e) {
			return nil
		}
	}
	return nil
}
