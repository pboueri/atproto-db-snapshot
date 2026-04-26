package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/run"
)

// TestPipelineRestartResume proves run.Run is SIGTERM-safe: kill the
// consumer mid-stream, restart it, and end up with no events lost and
// no duplicates. The dedup property is interesting because the spec
// (§8 step 6) rewinds the cursor on reconnect by `rewind_seconds`,
// which deliberately RE-EMITS already-seen events — the staging table
// PK + INSERT OR IGNORE is what collapses the overlap.
func TestPipelineRestartResume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping restart-resume e2e pipeline test in -short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 30 events, monotonically increasing time_us. Use post creates so
	// they're all routed to a single staging table (simplifies counting).
	const N = 30
	const pauseAfter = 12

	events := make([]jetstreamEventLine, 0, N)
	_, baseUS := yesterdayUTCMicros()
	for i := 0; i < N; i++ {
		rec, _ := json.Marshal(map[string]any{
			"text":      fmt.Sprintf("ev #%d", i),
			"createdAt": time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339),
			"langs":     []string{"en"},
		})
		events = append(events, jetstreamEventLine{
			DID:    "did:plc:resume",
			TimeUS: baseUS + int64(i+1),
			Kind:   "commit",
			Commit: jetstreamCommit{
				Rev:        "rev1",
				Operation:  "create",
				Collection: "app.bsky.feed.post",
				Rkey:       fmt.Sprintf("rs-%03d", i),
				CID:        fmt.Sprintf("bafyrs%d", i),
				Record:     rec,
			},
		})
	}

	// Paging fake: pauses after 12 events on the first connection,
	// holds the connection idle until the consumer disconnects, then
	// emits the rest from the persisted cursor on reconnect.
	fake := newFakeJetstreamPaging(events, pauseAfter)
	t.Cleanup(fake.Close)

	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}

	cfg := config.Default()
	cfg.DataDir = dataDir
	cfg.Jetstream.Endpoints = []string{fake.URL()}
	cfg.Jetstream.Compress = false
	// Tight checkpoint cadence so the cursor is flushed to disk fast
	// enough that the test's SIGTERM doesn't beat it.
	cfg.Jetstream.CheckpointEveryRows = 2
	cfg.Jetstream.CheckpointInterval = 100 * time.Millisecond
	cfg.Jetstream.RolloverGrace = 1 * time.Second
	cfg.Jetstream.MinFreeBytes = 0
	cfg.Jetstream.NoUpload = true
	cfg.Jetstream.LagAlarm = 0
	// Rewind > 0 to exercise the dedup overlap. With 1s rewind and our
	// fixture spanning ~30µs of time_us, the second connection will
	// rewind well past the start, so the fake server resends EVERY
	// event, and PK + INSERT OR IGNORE is what keeps the count at 30.
	cfg.Jetstream.RewindSeconds = 1

	logger := newDiscardLogger()
	stagingPath := filepath.Join(dataDir, "staging.db")
	cursorPath := filepath.Join(dataDir, "cursor.json")

	// ---- First run: consume up to ~pauseAfter events, then SIGTERM ----
	ctx1, cancel1 := context.WithCancel(ctx)
	run1Done := make(chan error, 1)
	go func() {
		run1Done <- run.Run(ctx1, cfg, nil, logger)
	}()

	// Wait until at least 10 events have landed in staging.
	if err := wait(ctx, 20*time.Second, func() bool {
		n, err := countStagingRows(stagingPath)
		if err != nil {
			return false
		}
		return n >= 10
	}); err != nil {
		cancel1()
		<-run1Done
		t.Fatalf("first run never reached 10 events: %v", err)
	}

	cancel1()
	if err := <-run1Done; err != nil && err != context.Canceled {
		t.Fatalf("first run.Run returned: %v", err)
	}

	// Cursor.json must have advanced past 0.
	cb, err := os.ReadFile(cursorPath)
	if err != nil {
		t.Fatalf("read cursor.json: %v", err)
	}
	var c1 struct {
		Cursor   int64  `json:"cursor"`
		Endpoint string `json:"endpoint"`
	}
	if err := json.Unmarshal(cb, &c1); err != nil {
		t.Fatalf("parse cursor.json: %v", err)
	}
	if c1.Cursor <= 0 {
		t.Fatalf("cursor.json did not advance: cursor=%d", c1.Cursor)
	}

	// Sanity: row count after SIGTERM is between 10 and pauseAfter.
	n1, err := countStagingRows(stagingPath)
	if err != nil {
		t.Fatalf("count after first run: %v", err)
	}
	if n1 < 10 || n1 > int64(pauseAfter) {
		t.Fatalf("post-SIGTERM staging rows = %d; want between 10 and %d", n1, pauseAfter)
	}

	// ---- Second run: fresh ctx, same data dir → fresh staging open ----
	ctx2, cancel2 := context.WithCancel(ctx)
	run2Done := make(chan error, 1)
	go func() {
		run2Done <- run.Run(ctx2, cfg, nil, logger)
	}()

	// Wait until total staging row count reaches 30.
	if err := wait(ctx, 25*time.Second, func() bool {
		n, err := countStagingRows(stagingPath)
		if err != nil {
			return false
		}
		return n >= int64(N)
	}); err != nil {
		cancel2()
		<-run2Done
		n, _ := countStagingRows(stagingPath)
		t.Fatalf("second run never reached %d events: have %d, err=%v", N, n, err)
	}

	cancel2()
	if err := <-run2Done; err != nil && err != context.Canceled {
		t.Fatalf("second run.Run returned: %v", err)
	}

	// Final assertion: exactly N rows, no loss and no duplicates.
	n2, err := countStagingRows(stagingPath)
	if err != nil {
		t.Fatalf("count after second run: %v", err)
	}
	if n2 != int64(N) {
		t.Fatalf("final staging rows = %d; want %d (loss or duplicate-rewind)", n2, N)
	}

	// And we should have observed >= 2 connections to the fake — first
	// (paused) plus the resume.
	if got := fake.Connections(); got < 2 {
		t.Errorf("fake server saw %d connections; want >= 2", got)
	}
}
