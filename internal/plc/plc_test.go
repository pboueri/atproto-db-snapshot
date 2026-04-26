package plc

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFakeStream(t *testing.T) {
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	f := NewFake(base, []string{"did:plc:a", "did:plc:b", "did:plc:c"})

	var got []string
	err := f.Stream(context.Background(), time.Time{}, func(e Entry) bool {
		got = append(got, e.DID)
		return true
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("got %d entries, want 3", len(got))
	}
}

func TestFakeStreamSince(t *testing.T) {
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	f := NewFake(base, []string{"did:plc:a", "did:plc:b", "did:plc:c"})

	var got []string
	if err := f.Stream(context.Background(), base.Add(time.Second), func(e Entry) bool {
		got = append(got, e.DID)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	// since exclusive: skip a (t+0), b (t+1) -> only c
	if len(got) != 1 || got[0] != "did:plc:c" {
		t.Errorf("got %v, want [did:plc:c]", got)
	}
}

func TestHTTPStreamPaginates(t *testing.T) {
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		after := r.URL.Query().Get("after")
		w.Header().Set("Content-Type", "application/jsonlines")
		switch after {
		case "":
			fmt.Fprintln(w, jsonLine("did:plc:a", base))
			fmt.Fprintln(w, jsonLine("did:plc:b", base.Add(time.Second)))
		case base.Add(time.Second).Format(time.RFC3339Nano):
			fmt.Fprintln(w, jsonLine("did:plc:c", base.Add(2*time.Second)))
		default:
			// Empty page -> done.
		}
	}))
	defer srv.Close()

	d := &HTTPDirectory{BaseURL: srv.URL, Client: srv.Client(), PageSize: 2}
	var got []string
	err := d.Stream(context.Background(), time.Time{}, func(e Entry) bool {
		got = append(got, e.DID)
		return true
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	want := []string{"did:plc:a", "did:plc:b", "did:plc:c"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("got %v, want %v", got, want)
	}
	if calls < 2 {
		t.Errorf("expected at least 2 paginated calls, got %d", calls)
	}
}

func jsonLine(did string, t time.Time) string {
	return fmt.Sprintf(`{"did":%q,"createdAt":%q}`, did, t.UTC().Format(time.RFC3339Nano))
}
