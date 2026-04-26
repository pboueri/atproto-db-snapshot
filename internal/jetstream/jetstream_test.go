package jetstream

import (
	"context"
	"testing"
	"time"
)

func TestFakeEmitsAndCloses(t *testing.T) {
	events := []Event{
		{DID: "did:plc:a", TimeUS: 1, Kind: KindCommit, Commit: &Commit{Collection: "app.bsky.feed.post", Operation: OpCreate, RKey: "1"}},
		{DID: "did:plc:b", TimeUS: 2, Kind: KindCommit, Commit: &Commit{Collection: "app.bsky.feed.like", Operation: OpCreate, RKey: "2"}},
	}
	f := NewFake(events)
	out, errc := f.Subscribe(context.Background(), SubscribeOptions{})

	var got []Event
	for e := range out {
		got = append(got, e)
	}
	if len(got) != 2 {
		t.Errorf("got %d events, want 2", len(got))
	}
	if err := <-errc; err != nil {
		t.Errorf("unexpected err: %v", err)
	}
}

func TestFakeFiltersByCollection(t *testing.T) {
	f := NewFake([]Event{
		{Kind: KindCommit, Commit: &Commit{Collection: "app.bsky.feed.post"}},
		{Kind: KindCommit, Commit: &Commit{Collection: "app.bsky.feed.like"}},
	})
	out, _ := f.Subscribe(context.Background(), SubscribeOptions{Collections: []string{"app.bsky.feed.post"}})
	var got []Event
	for e := range out {
		got = append(got, e)
	}
	if len(got) != 1 {
		t.Errorf("got %d, want 1 (filtered)", len(got))
	}
}

func TestFakeBlockUntilCancel(t *testing.T) {
	f := NewFake(nil)
	f.Block = true
	ctx, cancel := context.WithCancel(context.Background())
	out, errc := f.Subscribe(ctx, SubscribeOptions{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	for range out {
	}
	err := <-errc
	if err == nil {
		t.Errorf("expected cancellation error")
	}
}
