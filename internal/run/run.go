// Package run implements `at-snapshotter run` — the long-running Jetstream
// consumer documented in specs/001_bootstrap.md §8.
//
// The package is structured as four cooperating pieces:
//
//   - jetstream.go : the WebSocket client (connect, decompress, decode,
//     reconnect with endpoint failover).
//   - staging.go   : the SQLite staging database, checkpoint loop, and
//     atomic cursor.json persistence.
//   - rollover.go  : the UTC-midnight rollover scheduler that seals
//     yesterday's events into per-collection parquet shards and
//     uploads them via the ObjectStore.
//   - parquet.go   : DuckDB-backed parquet emit + per-day _manifest.json.
//
// run.Run wires those four pieces together and owns shutdown.
package run

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// Run starts the Jetstream consumer and blocks until ctx is cancelled.
//
// `store` may be nil; in that case (or when cfg.Jetstream.NoUpload is true),
// the consumer still rolls over to local parquet but skips object-store
// uploads. This is intentional — the spec leaves the object store as a
// caller-supplied dependency, and we prefer to keep working in single-host
// mode rather than fail on a missing implementation.
func Run(ctx context.Context, cfg config.Config, store objstore.ObjectStore, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if len(cfg.Jetstream.Endpoints) == 0 {
		return errors.New("run: no Jetstream endpoints configured")
	}
	if len(cfg.Jetstream.WantedCollections) == 0 {
		return errors.New("run: no wantedCollections configured")
	}

	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "./data"
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("run: mkdir %s: %w", dataDir, err)
	}
	dailyDir := filepath.Join(dataDir, "daily")
	if err := os.MkdirAll(dailyDir, 0o755); err != nil {
		return fmt.Errorf("run: mkdir %s: %w", dailyDir, err)
	}

	stagingPath := filepath.Join(dataDir, "staging.db")
	cursorPath := filepath.Join(dataDir, "cursor.json")

	// ---- staging db + cursor ----
	st, err := openStaging(stagingPath, cfg.Jetstream, logger)
	if err != nil {
		return fmt.Errorf("run: open staging: %w", err)
	}
	defer st.Close()

	cur, err := loadCursor(cursorPath)
	if err != nil {
		return fmt.Errorf("run: load cursor: %w", err)
	}
	logger.Info("loaded cursor",
		"cursor", cur.Cursor,
		"endpoint", cur.Endpoint,
		"updated_at", cur.UpdatedAt,
	)

	// ---- rollover scheduler ----
	roll := &rollover{
		dataDir:     dataDir,
		dailyDir:    dailyDir,
		stagingPath: stagingPath,
		store:       store,
		cfg:         cfg.Jetstream,
		staging:     st,
		logger:      logger,
	}

	// Run rollover scheduler, jetstream consumer, and checkpoint loop
	// concurrently. Cancel the shared context on any fatal exit.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	// Checkpoint + cursor persist loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		st.checkpointLoop(ctx, cursorPath)
	}()

	// Rollover scheduler.
	wg.Add(1)
	go func() {
		defer wg.Done()
		roll.runScheduler(ctx)
	}()

	// Jetstream consumer (blocks until ctx done or fatal error).
	js := &jetstreamConsumer{
		cfg:     cfg.Jetstream,
		dataDir: dataDir,
		staging: st,
		cursor:  cur,
		logger:  logger,
	}
	consumeErr := js.run(ctx)

	// Initiate shutdown.
	cancel()

	// Wait for sidecar goroutines.
	wg.Wait()

	// Final flush + cursor write — use a fresh, short-lived context so a
	// SIGTERM-cancelled parent ctx doesn't abort the shutdown work.
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer flushCancel()
	if err := st.checkpoint(flushCtx); err != nil {
		logger.Warn("final checkpoint failed", "err", err)
	}
	if c := st.lastCursor(); c.Cursor > 0 {
		if err := saveCursorAtomic(cursorPath, c); err != nil {
			logger.Warn("final cursor save failed", "err", err)
		} else {
			logger.Info("final cursor saved", "cursor", c.Cursor)
		}
	}

	if consumeErr != nil && !errors.Is(consumeErr, context.Canceled) {
		return consumeErr
	}
	return nil
}

// secondsUntilUTCMidnightPlusGrace returns the duration from `now` to the
// next UTC-midnight + grace boundary.
func secondsUntilUTCMidnightPlusGrace(now time.Time, grace time.Duration) time.Duration {
	tomorrow := time.Date(now.UTC().Year(), now.UTC().Month(), now.UTC().Day(), 0, 0, 0, 0, time.UTC).Add(24 * time.Hour)
	return tomorrow.Add(grace).Sub(now)
}
