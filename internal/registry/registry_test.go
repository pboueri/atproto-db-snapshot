package registry

import (
	"sync"
	"testing"
)

func TestGetOrAssignIdempotent(t *testing.T) {
	r := New()
	id1 := r.GetOrAssign("did:plc:a")
	id2 := r.GetOrAssign("did:plc:a")
	if id1 != id2 {
		t.Fatalf("GetOrAssign not idempotent: %d != %d", id1, id2)
	}
	if id1 != 1 {
		t.Fatalf("first id should be 1; got %d", id1)
	}
}

func TestGetOrAssignMonotonic(t *testing.T) {
	r := New()
	dids := []string{"did:plc:a", "did:plc:b", "did:plc:c", "did:plc:d"}
	var last int64
	for _, d := range dids {
		id := r.GetOrAssign(d)
		if id <= last {
			t.Fatalf("ids not monotonic: %d after %d for %q", id, last, d)
		}
		last = id
	}
	// Re-assign existing DID shouldn't bump.
	again := r.GetOrAssign("did:plc:b")
	if again != 2 {
		t.Fatalf("re-assign changed id: got %d want 2", again)
	}
	// Len reflects distinct DIDs.
	if got := r.Len(); got != 4 {
		t.Fatalf("Len = %d; want 4", got)
	}
}

func TestSnapshotOrdering(t *testing.T) {
	r := New()
	// Insert in reverse alphabetical order so IDs and DID order differ.
	for _, d := range []string{"did:plc:z", "did:plc:m", "did:plc:a"} {
		r.GetOrAssign(d)
	}
	snap := r.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("Snapshot len = %d; want 3", len(snap))
	}
	// Snapshot must be sorted ascending by ActorID.
	for i := 1; i < len(snap); i++ {
		if snap[i-1].ActorID >= snap[i].ActorID {
			t.Fatalf("snapshot ids not ascending at index %d: %d then %d",
				i, snap[i-1].ActorID, snap[i].ActorID)
		}
	}
	// DID order should match insertion order (not alphabetical).
	wantDIDs := []string{"did:plc:z", "did:plc:m", "did:plc:a"}
	for i, e := range snap {
		if e.DID != wantDIDs[i] {
			t.Fatalf("snap[%d].DID = %q; want %q", i, e.DID, wantDIDs[i])
		}
		if e.FirstSeen.IsZero() {
			t.Fatalf("snap[%d].FirstSeen is zero", i)
		}
	}
}

func TestConcurrentGetOrAssign(t *testing.T) {
	t.Parallel()
	r := New()

	const goroutines = 32
	const perGoroutine = 100
	var wg sync.WaitGroup
	results := make([][]int64, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ids := make([]int64, perGoroutine)
			for i := 0; i < perGoroutine; i++ {
				// half unique per goroutine, half shared across goroutines
				var did string
				if i%2 == 0 {
					did = "did:plc:shared" + itoaLocal(i)
				} else {
					did = "did:plc:g" + itoaLocal(g) + "-" + itoaLocal(i)
				}
				ids[i] = r.GetOrAssign(did)
			}
			results[g] = ids
		}(g)
	}
	wg.Wait()

	// Every assigned id must be unique per distinct DID. Verify via snapshot.
	snap := r.Snapshot()
	seenIDs := make(map[int64]string, len(snap))
	seenDIDs := make(map[string]int64, len(snap))
	for _, e := range snap {
		if other, ok := seenIDs[e.ActorID]; ok {
			t.Fatalf("duplicate actor_id %d for %q and %q", e.ActorID, other, e.DID)
		}
		seenIDs[e.ActorID] = e.DID
		if other, ok := seenDIDs[e.DID]; ok {
			t.Fatalf("duplicate DID %q with ids %d and %d", e.DID, other, e.ActorID)
		}
		seenDIDs[e.DID] = e.ActorID
	}
	// Expected distinct DIDs: shared (perGoroutine/2) + unique (goroutines * perGoroutine/2).
	wantDistinct := perGoroutine/2 + goroutines*(perGoroutine/2)
	if len(snap) != wantDistinct {
		t.Fatalf("distinct DIDs = %d; want %d", len(snap), wantDistinct)
	}
}

// itoaLocal avoids strconv import so the test stays tight.
func itoaLocal(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
