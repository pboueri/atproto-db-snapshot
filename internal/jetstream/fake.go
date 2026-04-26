package jetstream

import (
	"context"
	"sync"
	"time"
)

// Fake is a programmable Subscriber for tests. It plays back a fixed slice of
// events and then optionally blocks (Block=true) so the run command stays
// busy until the test cancels its context.
type Fake struct {
	mu     sync.Mutex
	events []Event
	// Block, if true, keeps the channel open after all events have been
	// emitted instead of closing it. Tests that want to mimic a long-running
	// stream and exercise context-cancel paths use this.
	Block bool
	// Delay between events. Zero is fine for most tests.
	Delay time.Duration
	// EmitAt counts how many events the most recent Subscribe has emitted;
	// useful for tests that interrupt mid-stream.
	emitted int
}

// NewFake returns a Fake preloaded with events.
func NewFake(events []Event) *Fake { return &Fake{events: events} }

// Append schedules an event for emission on a still-open subscription.
func (f *Fake) Append(e Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
}

// Emitted reports the number of events delivered by the latest Subscribe call.
func (f *Fake) Emitted() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.emitted
}

func (f *Fake) Subscribe(ctx context.Context, opts SubscribeOptions) (<-chan Event, <-chan error) {
	out := make(chan Event)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)
		f.mu.Lock()
		f.emitted = 0
		evs := append([]Event(nil), f.events...)
		f.mu.Unlock()

		for _, e := range evs {
			if opts.Cursor > 0 && e.TimeUS <= opts.Cursor {
				continue
			}
			if !matchesCollections(e, opts.Collections) || !matchesDIDs(e, opts.DIDs) {
				continue
			}
			select {
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			case out <- e:
				f.mu.Lock()
				f.emitted++
				f.mu.Unlock()
			}
			if f.Delay > 0 {
				select {
				case <-ctx.Done():
					errc <- ctx.Err()
					return
				case <-time.After(f.Delay):
				}
			}
		}
		if f.Block {
			<-ctx.Done()
			errc <- ctx.Err()
		}
	}()

	return out, errc
}

func matchesCollections(e Event, collections []string) bool {
	if len(collections) == 0 || e.Commit == nil {
		return true
	}
	for _, c := range collections {
		if c == e.Commit.Collection {
			return true
		}
	}
	return false
}

func matchesDIDs(e Event, dids []string) bool {
	if len(dids) == 0 {
		return true
	}
	for _, d := range dids {
		if d == e.DID {
			return true
		}
	}
	return false
}
