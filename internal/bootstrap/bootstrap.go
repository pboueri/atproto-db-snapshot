// Package bootstrap implements the `at-snapshot bootstrap` subcommand.
//
// The command builds the canonical baseline social graph by combining:
//
//   - the canonical DID list from PLC (one streaming GET to /export, paginated
//     by createdAt cursor);
//   - per-DID profile records from Slingshot
//     (https://slingshot.microcosm.blue), an edge cache that fronts every
//     PDS via com.atproto.repo.getRecord;
//   - per-DID *incoming* follow / block backlinks from Constellation
//     (https://constellation.microcosm.blue), the network-wide backlinks
//     index. Querying Constellation with target=<did> returns every record
//     in the network whose .subject points at <did> — i.e. the DIDs that
//     follow / block <did>. Iterating every DID in PLC as a target therefore
//     enumerates the same edge set as the original "ask each DID's PDS for
//     its outgoing follows" approach, but routed through one centralized
//     index instead of fan-out into thousands of rate-limited PDSes.
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
//   - Per-DID idempotency: every chunk is committed in its own transaction
//     with INSERT OR REPLACE on the natural keys. bootstrap_progress is
//     only updated by the FINAL chunk for a DID, so a crash mid-DID leaves
//     orphan rows that the resume re-fetch will overwrite cleanly.
//   - Crash safety: chunks for one DID may straddle a crash; on restart
//     the DID is re-fetched and the orphan rows are upserted.
//
// Concurrency: cfg.Concurrency goroutines pull DIDs off a channel, fetch
// profile + paginated backlinks, and stream chunks to a single writer
// goroutine that owns the duckdb connection. Single-writer keeps the SQL
// simple and avoids fighting DuckDB's writer lock.
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
	"github.com/pboueri/atproto-db-snapshot/internal/constellation"
	"github.com/pboueri/atproto-db-snapshot/internal/model"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
	"github.com/pboueri/atproto-db-snapshot/internal/plc"
	"github.com/pboueri/atproto-db-snapshot/internal/slingshot"
)

// chunkBatchSize is the number of follow/block rows accumulated before a
// chunk is flushed to the writer. Tuned so a chunk's transaction is small
// enough to not stall the writer (~hundred-row INSERT batches commit in
// under 100ms on local DuckDB) and large enough to amortize transaction
// overhead. Constellation paginates at 100 rows; one chunk = one page.
const chunkBatchSize = 500

// Deps lets tests inject fake source clients and object stores. Run wires
// production defaults via FromConfig.
type Deps struct {
	PLC           plc.Directory
	Slingshot     slingshot.Client
	Constellation constellation.Client
	ObjStore      objstore.Store
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
		PLC:           plc.NewHTTP(cfg.PLCEndpoint),
		Slingshot:     slingshot.NewHTTP(cfg.SlingshotEndpoint, cfg.Contact, cfg.MicrocosmRateLimit, cfg.MicrocosmBurst),
		Constellation: constellation.NewHTTP(cfg.ConstellationEndpoint, cfg.Contact, cfg.MicrocosmRateLimit, cfg.MicrocosmBurst),
		ObjStore:      obj,
		Now:           func() time.Time { return time.Now().UTC() },
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

	if err := st.MarkStarted(ctx, cfg.PLCEndpoint, cfg.ConstellationEndpoint); err != nil {
		return err
	}
	completed, err := st.CompletedDIDs(ctx)
	if err != nil {
		return err
	}
	slog.Info("bootstrap resume state", "already_complete", len(completed))

	// Channels: PLC stream -> jobs; workers -> chunks -> writer.
	jobs := make(chan plc.Entry, cfg.Concurrency*4)
	chunks := make(chan didChunk, cfg.Concurrency*4)

	var (
		fetched   atomic.Int64
		written   atomic.Int64
		fetchErrs atomic.Int64
	)

	// Producer: PLC stream pushes DIDs not in `completed`. The PDS field on
	// plc.Entry is no longer used — Slingshot resolves identities itself
	// and Constellation is keyed only by DID. We still read PLC to enumerate
	// every DID in the network (Constellation can't tell us about DIDs that
	// have zero incoming links, so PLC remains the source of truth for
	// "which DIDs exist").
	prodErr := make(chan error, 1)
	go func() {
		defer close(jobs)
		yielded := 0
		seen := make(map[string]struct{}, len(completed))
		err := deps.PLC.Stream(ctx, time.Time{}, func(e plc.Entry) bool {
			if _, ok := completed[e.DID]; ok {
				return true
			}
			if _, ok := seen[e.DID]; ok {
				// Same DID can appear in PLC's chronological export
				// across multiple ops (key rotation, PDS migration, etc.).
				// Dedupe in-memory so we don't queue duplicate jobs.
				return true
			}
			seen[e.DID] = struct{}{}
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
		startWorkers(ctx, cfg.Concurrency, jobs, chunks, deps, &fetched, &fetchErrs)
	}()

	// Writer: drain chunks until workers close the channel.
	writerErr := make(chan error, 1)
	go func() {
		defer close(writerErr)
		for c := range chunks {
			if err := st.commitChunk(ctx, c); err != nil {
				writerErr <- err
				return
			}
			if c.final {
				n := written.Add(1)
				if n%1000 == 0 {
					slog.Info("bootstrap progress", "completed_dids", n)
				}
			}
		}
	}()

	// Sidecar progress writer. The monitor reads this file instead of
	// opening the duckdb directly, since DuckDB's writer lock prevents any
	// other process from opening the .duckdb file (even read-only) while
	// bootstrap is running.
	progressPath := filepath.Join(cfg.DataDir, "bootstrap-staging", "progress.json")
	pw, err := newProgressWriter(progressPath, st, &fetched, &written, &fetchErrs, cfg.PLCEndpoint)
	if err != nil {
		return err
	}
	if err := pw.Tick(ctx); err != nil {
		slog.Warn("bootstrap progress sidecar", "err", err)
	}

	// Stats ticker. Writes the sidecar at the same cadence so monitor reads
	// stay current.
	statsCtx, stopStats := context.WithCancel(ctx)
	defer stopStats()
	go statsLoop(statsCtx, cfg.StatsInterval, &fetched, &written, &fetchErrs)
	go progressLoop(statsCtx, cfg.StatsInterval, pw)

	// Wait for workers to finish (which closes chunks, which lets the
	// writer exit), then check for errors from each stage.
	<-workersDone
	close(chunks)
	if err := <-writerErr; err != nil {
		return fmt.Errorf("bootstrap: writer: %w", err)
	}
	if err := <-prodErr; err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("bootstrap: plc producer: %w", err)
	}

	if err := st.MarkFinished(ctx); err != nil {
		return err
	}
	if err := pw.Close(ctx); err != nil {
		slog.Warn("bootstrap progress final write", "err", err)
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

// startWorkers fans out fetch goroutines; closes when all workers exit.
func startWorkers(ctx context.Context, n int, jobs <-chan plc.Entry, chunks chan<- didChunk, deps Deps, fetched, fetchErrs *atomic.Int64) {
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
					if err := fetchDID(ctx, deps, job, chunks); err != nil {
						fetchErrs.Add(1)
						slog.Warn("bootstrap fetch error", "did", job.DID, "err", err)
						// Even on error, send a final chunk so the DID is
						// marked complete with whatever we got. Otherwise
						// resume would re-fetch indefinitely on a DID whose
						// upstream is permanently broken.
						select {
						case <-ctx.Done():
						case chunks <- didChunk{did: job.DID, final: true}:
						}
						continue
					}
					fetched.Add(1)
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		<-done
	}
}

// fetchDID streams chunks for one DID's profile + incoming follows + incoming
// blocks. Every successful return is followed by a `final: true` chunk so
// the writer marks bootstrap_progress.
//
// On Slingshot ErrNotFound for the profile, the actor row is still written
// (as a minimal "DID exists, no public profile" placeholder) by the final
// chunk's INSERT OR IGNORE.
func fetchDID(ctx context.Context, deps Deps, job plc.Entry, chunks chan<- didChunk) error {
	now := deps.Now()
	did := job.DID

	// Profile: getRecord at rkey=self.
	var profile *model.Profile
	rec, err := deps.Slingshot.GetRecord(ctx, did, string(model.CollectionProfile), "self")
	switch {
	case err == nil:
		p, derr := atrecord.DecodeProfile(rec.Value, did, now, model.SourceBootstrap)
		if derr != nil {
			return fmt.Errorf("decode profile: %w", derr)
		}
		profile = &p
	case errors.Is(err, slingshot.ErrNotFound):
		// Tombstoned / no public profile — fall through; the final chunk
		// still writes a bare actor row.
	default:
		return fmt.Errorf("profile fetch: %w", err)
	}

	if err := sendChunk(ctx, chunks, didChunk{did: did, profile: profile}); err != nil {
		return err
	}

	// Follows: incoming backlinks where this DID is the target's .subject.
	if err := streamBacklinks(ctx, deps.Constellation, did, model.CollectionFollow, now, chunks); err != nil {
		return fmt.Errorf("follow backlinks: %w", err)
	}
	if err := streamBacklinks(ctx, deps.Constellation, did, model.CollectionBlock, now, chunks); err != nil {
		return fmt.Errorf("block backlinks: %w", err)
	}

	// Final marker — even when there were no follows / blocks, this fires
	// the bare-actor INSERT OR IGNORE and writes bootstrap_progress.
	return sendChunk(ctx, chunks, didChunk{did: did, final: true})
}

// streamBacklinks paginates Constellation for (target=did, collection,
// path=.subject) and flushes a chunk every chunkBatchSize rows. It does
// NOT send the final chunk — that's the caller's responsibility so it can
// be coordinated with the other collections.
func streamBacklinks(ctx context.Context, c constellation.Client, did string, collection model.Collection, now time.Time, chunks chan<- didChunk) error {
	var follows []model.Follow
	var blocks []model.Block

	flush := func() error {
		if len(follows) == 0 && len(blocks) == 0 {
			return nil
		}
		err := sendChunk(ctx, chunks, didChunk{did: did, follows: follows, blocks: blocks})
		follows = nil
		blocks = nil
		return err
	}

	err := c.GetBacklinks(ctx, did, string(collection), ".subject", func(l constellation.Link) bool {
		switch collection {
		case model.CollectionFollow:
			follows = append(follows, atrecord.BuildFollowFromBacklink(l.DID, did, l.RKey, now, model.SourceBootstrap))
		case model.CollectionBlock:
			blocks = append(blocks, atrecord.BuildBlockFromBacklink(l.DID, did, l.RKey, now, model.SourceBootstrap))
		}
		if len(follows)+len(blocks) >= chunkBatchSize {
			if err := flush(); err != nil {
				return false
			}
		}
		return ctx.Err() == nil
	})
	if err != nil {
		return err
	}
	return flush()
}

// sendChunk forwards c to chunks while honoring context cancellation.
func sendChunk(ctx context.Context, chunks chan<- didChunk, c didChunk) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case chunks <- c:
		return nil
	}
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

// progressLoop writes the sidecar JSON on the same cadence as stats logging.
func progressLoop(ctx context.Context, every time.Duration, pw *progressWriter) {
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
			if err := pw.Tick(ctx); err != nil {
				slog.Warn("bootstrap progress tick", "err", err)
			}
		}
	}
}
