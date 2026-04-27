// Package bootstrap implements the `at-snapshot bootstrap` subcommand.
//
// The command builds the canonical baseline social graph by combining:
//
//   - the canonical DID list from PLC (one streaming GET to /export, paginated
//     by createdAt cursor);
//   - per-DID profile / follow / block records from Constellation (one
//     listRecords per (did, collection)).
//
// The output lands at bootstrap/YYYY-MM-DD/social_graph.duckdb in object
// storage exactly once; the spec calls out that this file must never be
// overwritten so subsequent snapshot runs always read a stable baseline.
//
// Resumability:
//
//   - Local staging: while the run is in progress, all writes go to a local
//     file at {data_dir}/bootstrap-staging/social_graph.duckdb. We only copy
//     it up to object storage at the end of a successful run.
//   - Per-DID idempotency: every DID's profile + follows + blocks land in a
//     single transaction along with a row in bootstrap_progress. On restart
//     we re-read the progress table and skip already-completed DIDs.
//   - Crash safety: a partial DID's records are never visible because we
//     commit them in the same transaction as the progress row.
//
// Concurrency: cfg.Concurrency goroutines pull DIDs off a channel, fetch the
// three collections, and hand a bundle to a single writer goroutine that
// owns the duckdb connection. Single-writer keeps the SQL simple and avoids
// fighting DuckDB's writer lock.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/atrecord"
	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/model"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
	"github.com/pboueri/atproto-db-snapshot/internal/plc"
	"github.com/pboueri/atproto-db-snapshot/internal/repo"
)

// Deps lets tests inject fake source clients and object stores. Run wires
// production defaults via FromConfig.
type Deps struct {
	PLC      plc.Directory
	Repo     repo.Client
	ObjStore objstore.Store
	// Now lets tests pin the partition date the bootstrap publishes under.
	Now func() time.Time
}

// Run executes the bootstrap pipeline using the production source clients.
func Run(ctx context.Context, cfg config.Config) error {
	obj, err := objstore.FromConfig(cfg)
	if err != nil {
		return err
	}
	deps := Deps{
		PLC:      plc.NewHTTP(cfg.PLCEndpoint),
		Repo:     repo.NewHTTP(),
		ObjStore: obj,
		Now:      func() time.Time { return time.Now().UTC() },
	}
	return RunWith(ctx, cfg, deps)
}

// RunWith is the testable entrypoint: every IO dependency is on Deps so tests
// can stub them out.
func RunWith(ctx context.Context, cfg config.Config, deps Deps) error {
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Now().UTC() }
	}

	stagingPath := filepath.Join(cfg.DataDir, "bootstrap-staging", "social_graph.duckdb")
	st, err := openStore(stagingPath)
	if err != nil {
		return err
	}
	// We deliberately don't `defer st.Close()` here. DuckDB writes a WAL
	// alongside the .duckdb file and only checkpoints into the main file on
	// a clean Close — so we must close before the upload below or the
	// uploaded file is missing every row. The Close is invoked explicitly
	// just before uploadFile, with deferred safety nets along error paths.
	closed := false
	closeOnce := func() error {
		if closed {
			return nil
		}
		closed = true
		return st.Close()
	}
	defer closeOnce()

	if err := st.MarkStarted(ctx, cfg.PLCEndpoint, "pds:listRecords"); err != nil {
		return err
	}
	completed, err := st.CompletedDIDs(ctx)
	if err != nil {
		return err
	}
	slog.Info("bootstrap resume state", "already_complete", len(completed))

	// Channels: PLC stream -> jobs; workers -> bundles -> writer.
	// Each job carries the DID and the resolved PDS endpoint from PLC; a
	// DID without a PDS (tombstoned) is dropped at producer time.
	jobs := make(chan plc.Entry, cfg.Concurrency*4)
	bundles := make(chan didBundle, cfg.Concurrency*4)

	var (
		fetched   atomic.Int64
		written   atomic.Int64
		fetchErrs atomic.Int64
	)

	// Producer: PLC stream pushes DIDs not in `completed`. If cfg.MaxDIDs is
	// set, stop yielding once we've enqueued that many *new* DIDs (already-
	// completed DIDs from prior runs don't count against the cap, since the
	// cap exists to bound new fetch work).
	prodErr := make(chan error, 1)
	go func() {
		defer close(jobs)
		yielded := 0
		err := deps.PLC.Stream(ctx, time.Time{}, func(e plc.Entry) bool {
			if _, ok := completed[e.DID]; ok {
				return true
			}
			if e.PDS == "" {
				// Tombstoned or pre-v2 DIDs without a current PDS — skip
				// rather than queue a doomed listRecords job.
				return true
			}
			if cfg.MaxDIDs > 0 && yielded >= cfg.MaxDIDs {
				return false
			}
			select {
			case <-ctx.Done():
				return false
			case jobs <- e:
				yielded++
				return true
			}
		})
		prodErr <- err
	}()

	// Worker pool: fetch per-DID records.
	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		startWorkers(ctx, cfg.Concurrency, jobs, bundles, deps, &fetched, &fetchErrs)
	}()

	// Writer: drain bundles until workers close the channel.
	writerErr := make(chan error, 1)
	go func() {
		defer close(writerErr)
		for b := range bundles {
			if err := st.commitBundle(ctx, b); err != nil {
				writerErr <- err
				return
			}
			n := written.Add(1)
			if n%1000 == 0 {
				slog.Info("bootstrap progress", "completed_dids", n)
			}
		}
	}()

	// Stats ticker.
	statsCtx, stopStats := context.WithCancel(ctx)
	defer stopStats()
	go statsLoop(statsCtx, cfg.StatsInterval, &fetched, &written, &fetchErrs)

	// Wait for workers to finish (which closes bundles, which lets the
	// writer exit), then check for errors from each stage.
	<-workersDone
	close(bundles)
	if err := <-writerErr; err != nil {
		return fmt.Errorf("bootstrap: writer: %w", err)
	}
	if err := <-prodErr; err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("bootstrap: plc producer: %w", err)
	}

	if err := st.MarkFinished(ctx); err != nil {
		return err
	}
	slog.Info("bootstrap complete",
		"fetched", fetched.Load(),
		"written", written.Load(),
		"fetch_errors", fetchErrs.Load(),
	)

	// Close the duckdb before uploading so DuckDB checkpoints the WAL into
	// the main file. Otherwise the uploaded file is missing every row.
	if err := closeOnce(); err != nil {
		return fmt.Errorf("bootstrap: close staging: %w", err)
	}

	// Upload the final duckdb to objstore. The remote path is fixed for
	// today's date; the spec says it's never overwritten — if a prior
	// successful bootstrap exists at that path, refuse rather than
	// silently clobbering.
	day := deps.Now().Format("2006-01-02")
	remote := fmt.Sprintf("bootstrap/%s/social_graph.duckdb", day)
	if _, err := deps.ObjStore.Stat(ctx, remote); err == nil {
		return fmt.Errorf("bootstrap: %s already exists in object store; refusing to overwrite", remote)
	} else if !errors.Is(err, objstore.ErrNotExist) {
		return fmt.Errorf("bootstrap: stat %s: %w", remote, err)
	}

	return uploadFile(ctx, deps.ObjStore, remote, stagingPath)
}

// startWorkers fans out fetch goroutines; closes bundles once all workers exit.
func startWorkers(ctx context.Context, n int, jobs <-chan plc.Entry, bundles chan<- didBundle, deps Deps, fetched, fetchErrs *atomic.Int64) {
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					b, err := fetchBundle(ctx, deps, job)
					if err != nil {
						fetchErrs.Add(1)
						slog.Warn("bootstrap fetch error", "did", job.DID, "pds", job.PDS, "err", err)
						continue
					}
					fetched.Add(1)
					select {
					case <-ctx.Done():
						return
					case bundles <- b:
					}
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		<-done
	}
}

// fetchBundle pulls profile + follows + blocks for a single DID off its
// home PDS. Failures on individual collections are logged but don't fail the
// bundle — a deactivated repo, for example, may have one collection
// returning 4xx but others succeeding. The progress row still gets written
// because we'd rather make forward progress than block on flaky individuals.
func fetchBundle(ctx context.Context, deps Deps, job plc.Entry) (didBundle, error) {
	now := deps.Now()
	b := didBundle{did: job.DID}

	if profileRecs, err := deps.Repo.ListRecords(ctx, job.PDS, job.DID, model.CollectionProfile); err == nil {
		if len(profileRecs) > 0 {
			// app.bsky.actor.profile is a singleton-ish: rkey "self".
			rec := profileRecs[0]
			p, derr := atrecord.DecodeProfile(rec.Value, job.DID, now, model.SourceBootstrap)
			if derr != nil {
				return didBundle{}, derr
			}
			b.profile = &p
		}
	} else {
		return didBundle{}, fmt.Errorf("profile fetch: %w", err)
	}

	if followRecs, err := deps.Repo.ListRecords(ctx, job.PDS, job.DID, model.CollectionFollow); err == nil {
		for _, r := range followRecs {
			f, derr := atrecord.DecodeFollow(r.Value, job.DID, r.RKey, now, model.SourceBootstrap)
			if derr != nil {
				slog.Warn("decode follow", "did", job.DID, "rkey", r.RKey, "err", derr)
				continue
			}
			b.follows = append(b.follows, f)
		}
	} else {
		return didBundle{}, fmt.Errorf("follow fetch: %w", err)
	}

	if blockRecs, err := deps.Repo.ListRecords(ctx, job.PDS, job.DID, model.CollectionBlock); err == nil {
		for _, r := range blockRecs {
			bl, derr := atrecord.DecodeBlock(r.Value, job.DID, r.RKey, now, model.SourceBootstrap)
			if derr != nil {
				slog.Warn("decode block", "did", job.DID, "rkey", r.RKey, "err", derr)
				continue
			}
			b.blocks = append(b.blocks, bl)
		}
	} else {
		return didBundle{}, fmt.Errorf("block fetch: %w", err)
	}

	return b, nil
}

func uploadFile(ctx context.Context, obj objstore.Store, dst, src string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	return obj.Put(ctx, dst, f, "application/x-duckdb")
}

func statsLoop(ctx context.Context, every time.Duration, fetched, written, fetchErrs *atomic.Int64) {
	if every <= 0 {
		every = 30 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			slog.Info("bootstrap stats",
				"fetched", fetched.Load(),
				"written", written.Load(),
				"fetch_errors", fetchErrs.Load(),
			)
		}
	}
}
