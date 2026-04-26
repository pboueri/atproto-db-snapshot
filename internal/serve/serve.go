// Package serve implements the `at-snapshotter serve` HTTP dashboard.
//
// See specs/001_bootstrap.md §10 for the canonical card layout, route
// list, and Prometheus metric names.
//
// The dashboard is read-only and stateless. Every HTTP request re-reads
// from `DataDir` (cursor.json, latest.json, logs/, daily/, etc.) and
// from `current_graph.duckdb` via `ATTACH ... (READ_ONLY)`, so it never
// blocks `run` or `build`. Sources that are missing or locked render as
// `—` in the dashboard and as nulls in the JSON API.
package serve

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// Options configures the serve subcommand.
type Options struct {
	// Listen is the bind address, e.g. "127.0.0.1:8080".
	Listen string

	// DataDir is the local working directory that holds cursor.json,
	// latest.json, logs/, daily/, staging.db, current_graph.duckdb.
	DataDir string

	// RefreshSeconds is the dashboard auto-refresh interval. Default 5.
	RefreshSeconds int

	// LogTailLines is the number of trailing log lines to scan when
	// computing events/sec and surfacing recent errors. Default 20000.
	LogTailLines int

	// Store is optional. When non-nil the Object-store card lists daily/
	// and bootstrap/ counts; otherwise the card renders as `—`.
	Store objstore.ObjectStore
}

// withDefaults fills in spec-defined defaults for fields the caller left
// at their zero value.
func (o Options) withDefaults() Options {
	if o.RefreshSeconds <= 0 {
		o.RefreshSeconds = 5
	}
	if o.LogTailLines <= 0 {
		o.LogTailLines = 20000
	}
	if o.Listen == "" {
		o.Listen = "127.0.0.1:8080"
	}
	return o
}

// Run starts the HTTP server. It blocks until ctx is canceled or the
// listener fails. On ctx cancellation the server is gracefully shut down
// with a 5s grace period.
func Run(ctx context.Context, opts Options, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	opts = opts.withDefaults()

	srv := newServer(opts, logger)

	httpSrv := &http.Server{
		Addr:              opts.Listen,
		Handler:           srv.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ln, err := net.Listen("tcp", opts.Listen)
	if err != nil {
		return fmt.Errorf("serve: listen %s: %w", opts.Listen, err)
	}

	logger.Info("serve listening", "addr", ln.Addr().String(), "data_dir", opts.DataDir)

	errCh := make(chan error, 1)
	go func() {
		if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			logger.Warn("serve shutdown", "err", err)
		}
		// Drain Serve goroutine.
		<-errCh
		return nil
	case err, ok := <-errCh:
		if !ok {
			return nil
		}
		return err
	}
}
