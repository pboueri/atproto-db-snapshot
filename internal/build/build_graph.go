package build

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/constellation"
	"github.com/pboueri/atproto-db-snapshot/internal/duckdbstore"
	"github.com/pboueri/atproto-db-snapshot/internal/parse"
	"github.com/pboueri/atproto-db-snapshot/internal/pds"
	"github.com/pboueri/atproto-db-snapshot/internal/plc"
	"github.com/pboueri/atproto-db-snapshot/internal/registry"
)

// runGraphBackfill executes the listRecords-based graph backfill per
// specs/003_listrecords_backfill.md.
//
// Streaming architecture:
//
//   - Pre-seed: enumerates the existing pds_endpoints table (resume
//     state from prior runs) and submits unprocessed DIDs to the
//     dispatcher immediately, so listRecords work starts at t=0.
//   - Phase 1: PLC enumeration runs concurrently — fetcher and flusher
//     are pipelined through an internal channel, and each flushed batch
//     is also fed to the dispatcher so newly-learned DIDs queue up
//     without waiting for PLC to finish.
//   - Dispatcher: lazily spawns one per-host worker pool the first time
//     a DID for that host arrives. Pools live until the dispatcher is
//     closed, then drain.
//   - Writer: a single goroutine drains record batches into DuckDB via
//     the StreamingWriter (separate connection from PLC upserts, so the
//     two write paths don't contend on a single sql.DB conn).
//   - Phase 5 (optional Constellation enrichment) still runs after
//     phase 1+3 fully drain, since it iterates over the final actor set.
//
// Returns the on-disk path of the produced current_graph.duckdb.
func runGraphBackfill(ctx context.Context, cfg config.Config, logger *slog.Logger) (string, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return "", err
	}
	outPath := filepath.Join(cfg.DataDir, "current_graph.duckdb")

	store, err := duckdbstore.Open(outPath, "8GB", 2)
	if err != nil {
		return "", fmt.Errorf("open duckdb: %w", err)
	}
	defer store.Close()

	// Resume registry / processed set.
	reg := registry.New()
	prior, err := store.LoadRegistry()
	if err != nil {
		return "", fmt.Errorf("load registry: %w", err)
	}
	if len(prior) > 0 {
		reg.Reload(prior)
	}
	processed, err := store.LoadProcessedDIDs()
	if err != nil {
		return "", fmt.Errorf("load processed dids: %w", err)
	}
	if len(prior) > 0 || len(processed) > 0 {
		logger.Info("resume detected",
			"registry_actors", len(prior),
			"processed_dids", len(processed),
		)
	}

	// ---- writer setup ----
	writer, err := duckdbstore.NewStreamingWriter(outPath)
	if err != nil {
		return "", fmt.Errorf("open writer: %w", err)
	}
	seenRegistry := make(map[int64]struct{})
	for _, e := range reg.Snapshot() {
		seenRegistry[e.ActorID] = struct{}{}
	}
	processedActorIDs, err := loadProcessedActorIDs(store)
	if err != nil {
		writer.Close()
		return "", fmt.Errorf("load processed actor_ids: %w", err)
	}
	seenProcessed := make(map[int64]struct{}, len(processedActorIDs))
	for _, id := range processedActorIDs {
		seenProcessed[id] = struct{}{}
	}

	stats := &backfillStats{}
	// Big buffer so the per-host worker pools don't synchronously block
	// on the writer when CHECKPOINT is in progress. Writer is the
	// single-goroutine bottleneck; this is its absorber.
	records := make(chan *parse.Records, 1024)
	writeDone := make(chan error, 1)
	go writerLoop(ctx, writer, store, reg, records, seenRegistry, seenProcessed, stats, logger, writeDone)

	// ---- progress logger ----
	progDone := make(chan struct{})
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-progDone:
				return
			case <-t.C:
				logger.Info("progress",
					"dids_submitted", atomic.LoadInt64(&stats.didsSubmitted),
					"dids_done", atomic.LoadInt64(&stats.didsDone),
					"dids_in_flight", atomic.LoadInt64(&stats.didsInFlight),
					"hosts_active", atomic.LoadInt64(&stats.hostsActive),
					"hosts_circuit_open", atomic.LoadInt64(&stats.hostsCircuit),
					"plc_ops", atomic.LoadInt64(&stats.plcOps),
					"plc_endpoints_flushed", atomic.LoadInt64(&stats.plcEndpointsFlushed),
					"errors_429", atomic.LoadInt64(&stats.errors429),
					"errors_5xx", atomic.LoadInt64(&stats.errors5xx),
					"errors_timeout", atomic.LoadInt64(&stats.errorsTimeout),
					"errors_other", atomic.LoadInt64(&stats.errorsOther),
					"follows", atomic.LoadInt64(&stats.follows),
					"blocks", atomic.LoadInt64(&stats.blocks),
					"actors_registered", atomic.LoadInt64(&stats.registered),
				)
			}
		}
	}()

	// ---- streaming dispatcher + producers ----
	eg, gctx := errgroup.WithContext(ctx)
	disp := newDispatcher(gctx, eg, cfg, records, stats, logger)
	disp.limit = cfg.DIDLimit

	var preDone, phase1Done sync.WaitGroup
	preDone.Add(1)
	phase1Done.Add(1)

	// Producer A: pre-seed from existing pds_endpoints.
	eg.Go(func() error {
		defer preDone.Done()
		return preSeedFromStore(gctx, store, disp, processed, logger)
	})

	// Producer B: phase 1 streaming PLC enumeration.
	eg.Go(func() error {
		defer phase1Done.Done()
		return phase1PLCStreaming(gctx, cfg, store, logger, stats, func(batch map[string]string) {
			for did, ep := range batch {
				if _, done := processed[did]; done {
					continue
				}
				disp.submit(did, ep)
			}
		})
	})

	// Closer: when both producers are done, close all per-host channels.
	eg.Go(func() error {
		preDone.Wait()
		phase1Done.Wait()
		disp.close()
		return nil
	})

	werr := eg.Wait()

	// All producers + per-host pools are done. Tear down writer.
	close(records)
	close(progDone)
	if rerr := <-writeDone; rerr != nil && werr == nil {
		werr = rerr
	}
	if cerr := writer.FlushAll(); cerr != nil && werr == nil {
		werr = cerr
	}
	if cerr := writer.Close(); cerr != nil && werr == nil {
		werr = cerr
	}
	if werr != nil {
		return "", werr
	}

	// -------- Materialize + counts --------
	logger.Info("materializing target-only actors")
	if err := store.MaterializeTargetOnlyActors(time.Now().UTC()); err != nil {
		return "", fmt.Errorf("materialize target-only: %w", err)
	}
	logger.Info("recomputing counts")
	if err := store.RecomputeCounts(); err != nil {
		return "", fmt.Errorf("recompute counts: %w", err)
	}

	// -------- Phase 5: optional Constellation enrichment --------
	if cfg.Constellation.Enabled {
		if err := phase5Constellation(ctx, cfg, store, logger); err != nil {
			logger.Warn("constellation enrichment failed (non-fatal)", "err", err)
		}
	}

	if err := store.WriteMeta("backfill-graph", int64(cfg.DIDLimit), int64(reg.Len())); err != nil {
		return "", fmt.Errorf("write meta: %w", err)
	}
	logger.Info("checkpointing")
	if err := store.Checkpoint(); err != nil {
		return "", fmt.Errorf("checkpoint: %w", err)
	}

	logger.Info("graph backfill complete",
		"path", outPath,
		"follows", atomic.LoadInt64(&stats.follows),
		"blocks", atomic.LoadInt64(&stats.blocks),
		"actors_registered", atomic.LoadInt64(&stats.registered),
	)
	return outPath, nil
}

// ---------- backfill stats ----------

type backfillStats struct {
	follows             int64
	blocks              int64
	registered          int64
	didsSubmitted       int64
	didsDone            int64
	didsInFlight        int64
	hostsActive         int64
	hostsCircuit        int64
	plcOps              int64
	plcEndpointsFlushed int64
	errors429           int64
	errors5xx           int64
	errorsTimeout       int64
	errorsOther         int64
}

// ---------- Phase 1: streaming PLC enumeration ----------

// phase1PLCStreaming enumerates the PLC export with the fetcher and
// flusher decoupled by an internal channel — the fetcher is HTTP-bound,
// the flusher is DuckDB-bound, and they overlap. After every successful
// flush, onBatch(batch) is invoked with the just-persisted (did →
// endpoint) map so the streaming dispatcher can begin listRecords work
// without waiting for phase 1 to finish.
//
// Tombstoned DIDs are deleted from pds_endpoints in the same flush;
// they are NOT passed to onBatch.
//
// If the most recent pds_endpoints.resolved_at is within
// cfg.PLC.RefreshDays, this is a no-op (returns nil).
func phase1PLCStreaming(
	ctx context.Context,
	cfg config.Config,
	store *duckdbstore.Store,
	logger *slog.Logger,
	stats *backfillStats,
	onBatch func(map[string]string),
) error {
	cursorPath := filepath.Join(cfg.DataDir, "plc_cursor.json")

	mostRecent, err := store.MostRecentPDSResolveAt()
	if err != nil {
		return fmt.Errorf("most-recent resolve_at: %w", err)
	}
	if !mostRecent.IsZero() && cfg.PLC.RefreshDays > 0 {
		age := time.Since(mostRecent)
		if age < time.Duration(cfg.PLC.RefreshDays)*24*time.Hour {
			logger.Info("skipping PLC enumeration",
				"most_recent_resolved_at", mostRecent,
				"age", age.Round(time.Hour),
				"refresh_days", cfg.PLC.RefreshDays,
			)
			return nil
		}
	}

	cursor, err := plc.LoadCursor(cursorPath)
	if err != nil {
		return fmt.Errorf("load cursor: %w", err)
	}
	logger.Info("phase 1: PLC enumeration", "endpoint", cfg.PLC.Endpoint, "cursor", cursor.Cursor)

	cli := plc.New(cfg.PLC.Endpoint, cfg.PLC.RPS, cfg.HTTPTimeout)
	cli.PageSize = cfg.PLC.PageSize

	// Buffered op channel between fetcher and flusher.
	opCh := make(chan plc.Op, 8192)

	eg, gctx := errgroup.WithContext(ctx)
	var finalCursor string

	// Fetcher.
	eg.Go(func() error {
		defer close(opCh)
		c, err := cli.Stream(gctx, cursor.Cursor, func(op plc.Op) error {
			select {
			case opCh <- op:
				return nil
			case <-gctx.Done():
				return gctx.Err()
			}
		})
		finalCursor = c
		return err
	})

	// Flusher.
	eg.Go(func() error {
		const flushEvery = 50_000
		endpoints := map[string]string{}
		tombstones := map[string]struct{}{}
		var lastRawCursor string

		flush := func() error {
			if len(endpoints) > 0 {
				batch := endpoints
				endpoints = map[string]string{}
				if err := store.BulkUpsertPDSEndpoints(batch, time.Now().UTC()); err != nil {
					return fmt.Errorf("bulk upsert: %w", err)
				}
				atomic.AddInt64(&stats.plcEndpointsFlushed, int64(len(batch)))
				if onBatch != nil {
					onBatch(batch)
				}
			}
			if len(tombstones) > 0 {
				ts := make([]string, 0, len(tombstones))
				for d := range tombstones {
					ts = append(ts, d)
				}
				tombstones = map[string]struct{}{}
				if err := store.DeletePDSEndpoints(ts); err != nil {
					return fmt.Errorf("delete tombstones: %w", err)
				}
			}
			if lastRawCursor != "" {
				if err := plc.SaveCursorAtomic(cursorPath, plc.CursorState{Cursor: lastRawCursor}); err != nil {
					logger.Warn("plc cursor save failed", "err", err)
				}
			}
			return nil
		}

		var batched int
		for op := range opCh {
			atomic.AddInt64(&stats.plcOps, 1)
			if op.Tombstone {
				tombstones[op.DID] = struct{}{}
				delete(endpoints, op.DID)
			} else if op.Endpoint != "" {
				endpoints[op.DID] = op.Endpoint
				delete(tombstones, op.DID)
			}
			lastRawCursor = op.RawCursor
			batched++
			if batched >= flushEvery {
				if err := flush(); err != nil {
					return err
				}
				batched = 0
			}
		}
		// Final partial flush.
		if err := flush(); err != nil {
			return err
		}
		return nil
	})

	werr := eg.Wait()
	// Save the fetcher's reported final cursor (may be ahead of the last
	// op the flusher saw if the flusher errored mid-batch).
	if finalCursor != "" {
		if err := plc.SaveCursorAtomic(cursorPath, plc.CursorState{Cursor: finalCursor}); err != nil {
			logger.Warn("plc final cursor save failed", "err", err)
		}
	}
	if werr != nil {
		return werr
	}
	logger.Info("phase 1: complete",
		"plc_ops", atomic.LoadInt64(&stats.plcOps),
		"endpoints_flushed", atomic.LoadInt64(&stats.plcEndpointsFlushed),
		"final_cursor", finalCursor,
	)
	return nil
}

// ---------- Pre-seed from existing pds_endpoints ----------

// preSeedFromStore enumerates the persisted pds_endpoints table and
// submits every unprocessed DID to the dispatcher. This is what makes
// resume + streaming dispatch start producing work at t=0 instead of
// waiting for phase 1 to complete.
//
// The naive single-goroutine submit serializes pre-seed at the slowest
// host's drain rate (e.g. bsky.social handles ~50% of DIDs but its
// per-host worker pool drains at ~16 DIDs/sec, so a serial pre-seed
// blocks the whole feeder behind that one host). We bucket by host
// upfront and feed each bucket from its own goroutine so hosts ramp
// up in parallel.
func preSeedFromStore(
	ctx context.Context,
	store *duckdbstore.Store,
	disp *dispatcher,
	processed map[string]struct{},
	logger *slog.Logger,
) error {
	endpoints, err := store.LoadPDSEndpoints()
	if err != nil {
		return fmt.Errorf("preseed load: %w", err)
	}
	logger.Info("pre-seed: loaded existing pds_endpoints", "total", len(endpoints))

	// Bucket by normalized host so we know how to fan out.
	type didEP struct{ did, ep string }
	byHost := map[string][]didEP{}
	for did, ep := range endpoints {
		if _, done := processed[did]; done {
			continue
		}
		host := normalizeHost(ep)
		if host == "" {
			continue
		}
		byHost[host] = append(byHost[host], didEP{did, ep})
	}

	var totalQueued int
	for _, b := range byHost {
		totalQueued += len(b)
	}
	logger.Info("pre-seed: bucketed", "hosts", len(byHost), "dids", totalQueued)

	// One goroutine per host bucket. submit() only blocks on its own
	// host's channel, so different hosts proceed independently.
	var wg sync.WaitGroup
	var submitted int64
	for _, bucket := range byHost {
		bucket := bucket
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, item := range bucket {
				if ctx.Err() != nil {
					return
				}
				disp.submit(item.did, item.ep)
				atomic.AddInt64(&submitted, 1)
			}
		}()
	}
	wg.Wait()

	logger.Info("pre-seed: dispatched", "submitted", atomic.LoadInt64(&submitted))
	return ctx.Err()
}

// ---------- Streaming dispatcher ----------

// dispatcher routes (did, host) submissions to per-host worker pools.
// Pools are created lazily the first time a host appears, which means
// hosts learned mid-PLC start producing work immediately.
//
// Submissions are deduplicated by DID across the lifetime of the
// dispatcher, so pre-seed and phase 1 can both submit without causing
// the same DID to be fetched twice.
//
// Closing the dispatcher closes every per-host input channel. Pools
// drain remaining DIDs and exit, which lets the parent errgroup's
// Wait() return.
type dispatcher struct {
	cfg     config.Config
	out     chan<- *parse.Records
	stats   *backfillStats
	logger  *slog.Logger
	g       *errgroup.Group
	gctx    context.Context
	workers int
	limit   int

	mu     sync.Mutex
	pools  map[string]*hostPool
	seen   map[string]struct{}
	closed bool
}

type hostPool struct {
	in     chan string
	client *pds.Client
}

func newDispatcher(
	ctx context.Context,
	g *errgroup.Group,
	cfg config.Config,
	out chan<- *parse.Records,
	stats *backfillStats,
	logger *slog.Logger,
) *dispatcher {
	return &dispatcher{
		cfg:     cfg,
		out:     out,
		stats:   stats,
		logger:  logger,
		g:       g,
		gctx:    ctx,
		workers: cfg.PDS.PerHostWorkers,
		pools:   map[string]*hostPool{},
		seen:    map[string]struct{}{},
	}
}

// submit routes one (did, endpoint) pair to its host pool. Spawns the
// pool on first sight. Dedupes by DID. Respects DIDLimit.
func (d *dispatcher) submit(did, endpoint string) {
	host := normalizeHost(endpoint)
	if host == "" {
		return
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	if _, dup := d.seen[did]; dup {
		d.mu.Unlock()
		return
	}
	if d.limit > 0 && len(d.seen) >= d.limit {
		d.mu.Unlock()
		return
	}
	d.seen[did] = struct{}{}
	atomic.AddInt64(&d.stats.didsSubmitted, 1)

	p, ok := d.pools[host]
	if !ok {
		client := pds.New(host, pds.Options{
			HTTPTimeout:      d.cfg.PDS.HTTPTimeout,
			RPS:              d.cfg.PDS.PerHostRPS,
			MaxRetries:       d.cfg.PDS.MaxRetries,
			BreakerThreshold: d.cfg.PDS.BreakerThreshold,
			BreakerCooldown:  d.cfg.PDS.BreakerCooldown,
		})
		p = &hostPool{
			// Generous buffer so phase-1 onBatch (which submits up to
			// ~50K DIDs at once, ~50% of which can land on bsky.social)
			// doesn't block the PLC flusher.
			in:     make(chan string, 4096),
			client: client,
		}
		d.pools[host] = p
		atomic.AddInt64(&d.stats.hostsActive, 1)
		d.g.Go(func() error {
			defer atomic.AddInt64(&d.stats.hostsActive, -1)
			return d.runPool(p)
		})
	}
	d.mu.Unlock()

	// Send outside the lock so other hosts' submissions aren't blocked.
	select {
	case p.in <- did:
	case <-d.gctx.Done():
	}
}

// close marks the dispatcher closed and shuts down every per-host
// input channel. Pools drain in-flight DIDs then exit.
func (d *dispatcher) close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	pools := make([]*hostPool, 0, len(d.pools))
	for _, p := range d.pools {
		pools = append(pools, p)
	}
	d.mu.Unlock()
	for _, p := range pools {
		close(p.in)
	}
}

// runPool fans out cfg.PDS.PerHostWorkers workers for one host.
func (d *dispatcher) runPool(p *hostPool) error {
	var wg sync.WaitGroup
	for i := 0; i < d.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for did := range p.in {
				atomic.AddInt64(&d.stats.didsInFlight, 1)
				rec, err := fetchOne(d.gctx, p.client, did, d.stats, d.logger)
				atomic.AddInt64(&d.stats.didsInFlight, -1)
				if err != nil && errors.Is(err, context.Canceled) {
					return
				}
				if rec != nil {
					select {
					case d.out <- rec:
					case <-d.gctx.Done():
						return
					}
				}
				atomic.AddInt64(&d.stats.didsDone, 1)
			}
		}()
	}
	wg.Wait()
	if p.client.CircuitOpen() {
		atomic.AddInt64(&d.stats.hostsCircuit, 1)
	}
	return nil
}

// normalizeHost extracts the scheme://host (no path) from an endpoint URL.
// Falls back to the raw string if parsing fails.
func normalizeHost(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return endpoint
	}
	scheme := u.Scheme
	if scheme == "" {
		scheme = "https"
	}
	return scheme + "://" + u.Host
}

// ---------- per-DID listRecords fetch ----------

// fetchOne calls listRecords for the three target collections of a DID
// and assembles a parse.Records. Returns nil + nil if the DID was
// skippable (RepoNotFound, 404, breaker open).
func fetchOne(ctx context.Context, client *pds.Client, did string, stats *backfillStats, logger *slog.Logger) (*parse.Records, error) {
	rec := &parse.Records{AuthorDID: did}

	// profile
	if err := client.ListRecords(ctx, did, "app.bsky.actor.profile", func(value json.RawMessage, _ string) error {
		dec, err := pds.Decode(value, "app.bsky.actor.profile")
		if err != nil {
			return err
		}
		if p, ok := dec.(*parse.ProfileRec); ok {
			rec.Profile = p
		}
		return nil
	}); err != nil {
		if classifyAndAccount(err, stats) {
			return rec, nil
		}
		logger.Debug("listRecords profile failed", "did", did, "err", err)
		return nil, err
	}

	// follows
	if err := client.ListRecords(ctx, did, "app.bsky.graph.follow", func(value json.RawMessage, rkey string) error {
		dec, err := pds.Decode(value, "app.bsky.graph.follow")
		if err != nil {
			return err
		}
		if f, ok := dec.(parse.FollowRec); ok {
			f.Rkey = rkey
			if f.TargetDID != "" {
				rec.Follows = append(rec.Follows, f)
			}
		}
		return nil
	}); err != nil {
		if classifyAndAccount(err, stats) {
			return rec, nil
		}
		logger.Debug("listRecords follow failed", "did", did, "err", err)
		return nil, err
	}

	// blocks
	if err := client.ListRecords(ctx, did, "app.bsky.graph.block", func(value json.RawMessage, rkey string) error {
		dec, err := pds.Decode(value, "app.bsky.graph.block")
		if err != nil {
			return err
		}
		if b, ok := dec.(parse.BlockRec); ok {
			b.Rkey = rkey
			if b.TargetDID != "" {
				rec.Blocks = append(rec.Blocks, b)
			}
		}
		return nil
	}); err != nil {
		if classifyAndAccount(err, stats) {
			return rec, nil
		}
		logger.Debug("listRecords block failed", "did", did, "err", err)
		return nil, err
	}

	return rec, nil
}

// classifyAndAccount returns true when the error is a "skip this DID"
// signal (and bumps the appropriate counter). Returns false for fatal
// errors that should bubble up.
func classifyAndAccount(err error, stats *backfillStats) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, pds.ErrSkip):
		return true
	case errors.Is(err, pds.ErrCircuitOpen):
		return true
	case errors.Is(err, context.DeadlineExceeded):
		atomic.AddInt64(&stats.errorsTimeout, 1)
		return true
	}
	msg := err.Error()
	switch {
	case contains(msg, "429"):
		atomic.AddInt64(&stats.errors429, 1)
	case contains(msg, " 5"):
		atomic.AddInt64(&stats.errors5xx, 1)
	case contains(msg, "timeout") || contains(msg, "deadline"):
		atomic.AddInt64(&stats.errorsTimeout, 1)
	default:
		atomic.AddInt64(&stats.errorsOther, 1)
	}
	return true
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ---------- Phase 5: Constellation enrichment ----------

func phase5Constellation(ctx context.Context, cfg config.Config, store *duckdbstore.Store, logger *slog.Logger) error {
	cli := constellation.New(cfg.Constellation.Endpoint, cfg.Constellation.RPS, cfg.HTTPTimeout)
	logger.Info("phase 5: Constellation enrichment", "endpoint", cfg.Constellation.Endpoint)

	rows, err := store.DB.Query(`SELECT did FROM actors WHERE repo_processed = TRUE`)
	if err != nil {
		return fmt.Errorf("scan actors: %w", err)
	}
	defer rows.Close()

	var (
		ok     int64
		failed int64
	)
	for rows.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var did string
		if err := rows.Scan(&did); err != nil {
			return err
		}
		c, err := cli.Counts(ctx, did)
		if err != nil {
			failed++
			logger.Debug("constellation lookup failed", "did", did, "err", err)
			continue
		}
		if err := store.UpdateActorReceivedCounts(did, c.LikesReceived, c.RepostsReceived); err != nil {
			failed++
			logger.Debug("update received counts failed", "did", did, "err", err)
			continue
		}
		if c.FollowerCount > 0 {
			if _, err := store.DB.Exec(`UPDATE actors SET follower_count = ? WHERE did = ?`, c.FollowerCount, did); err != nil {
				failed++
				continue
			}
		}
		ok++
	}
	logger.Info("phase 5: complete", "updated", ok, "failed", failed)
	return rows.Err()
}

// ---------- writer ----------

type writerStats struct {
	follows    int64
	blocks     int64
	registered int64
}

func writerLoop(
	ctx context.Context,
	w *duckdbstore.StreamingWriter,
	s *duckdbstore.Store,
	reg *registry.Registry,
	in <-chan *parse.Records,
	seenRegistry map[int64]struct{},
	seenProcessed map[int64]struct{},
	stats *backfillStats,
	logger *slog.Logger,
	done chan<- error,
) {
	defer close(done)
	// FlushAll is cheap (Appender.Flush() pushes the in-memory column
	// buffer to the WAL — necessary so seenProcessed survives a crash).
	flushTick := time.NewTicker(60 * time.Second)
	defer flushTick.Stop()
	// CHECKPOINT is expensive (rewrites the on-disk database to merge
	// the WAL); on a 4 GB DB it can stall the writer for tens of seconds
	// while upstream worker pools fill. Run it far less often. We still
	// CHECKPOINT explicitly at end-of-run.
	ckptTick := time.NewTicker(15 * time.Minute)
	defer ckptTick.Stop()

	for {
		select {
		case <-flushTick.C:
			if err := w.FlushAll(); err != nil {
				logger.Warn("periodic flush failed", "err", err)
			}
		case <-ckptTick.C:
			t0 := time.Now()
			if err := s.Checkpoint(); err != nil {
				logger.Warn("periodic checkpoint failed", "err", err)
			} else {
				logger.Info("periodic checkpoint", "took", time.Since(t0).Round(time.Millisecond))
			}
		case rec, ok := <-in:
			if !ok {
				done <- nil
				return
			}
			ws := &writerStats{}
			if err := writeOne(w, reg, rec, seenRegistry, seenProcessed, ws); err != nil {
				done <- err
				for range in {
				}
				return
			}
			atomic.AddInt64(&stats.follows, ws.follows)
			atomic.AddInt64(&stats.blocks, ws.blocks)
			atomic.AddInt64(&stats.registered, ws.registered)
		case <-ctx.Done():
			done <- ctx.Err()
			return
		}
	}
}

// writeOne resolves DIDs and streams graph rows from one repo to DuckDB.
func writeOne(
	w *duckdbstore.StreamingWriter,
	reg *registry.Registry,
	r *parse.Records,
	seenRegistry map[int64]struct{},
	seenProcessed map[int64]struct{},
	stats *writerStats,
) error {
	now := time.Now().UTC()

	authorID, fresh := reg.GetOrAssignFresh(r.AuthorDID)
	if fresh {
		if err := w.AppendRegistryEntry(authorID, r.AuthorDID, now); err != nil {
			return fmt.Errorf("registry %d: %w", authorID, err)
		}
		seenRegistry[authorID] = struct{}{}
		stats.registered++
	} else if _, ok := seenRegistry[authorID]; !ok {
		seenRegistry[authorID] = struct{}{}
	}

	if _, alreadyWritten := seenProcessed[authorID]; !alreadyWritten {
		row := duckdbstore.ActorRow{
			ActorID:       authorID,
			DID:           r.AuthorDID,
			IndexedAt:     now,
			RepoProcessed: true,
		}
		if r.Profile != nil {
			if r.Profile.DisplayName != nil {
				row.DisplayName = *r.Profile.DisplayName
			}
			if r.Profile.Description != nil {
				row.Description = *r.Profile.Description
			}
			if r.Profile.AvatarCID != nil {
				row.AvatarCID = *r.Profile.AvatarCID
			}
			if r.Profile.CreatedAt != nil && !r.Profile.CreatedAt.IsZero() {
				row.CreatedAt = *r.Profile.CreatedAt
			}
		}
		if err := w.AppendActor(row); err != nil {
			return fmt.Errorf("actor %d: %w", authorID, err)
		}
		seenProcessed[authorID] = struct{}{}
	}

	followSeen := make(map[string]struct{}, len(r.Follows))
	for _, f := range r.Follows {
		if f.TargetDID == "" {
			continue
		}
		if _, dup := followSeen[f.Rkey]; dup {
			continue
		}
		followSeen[f.Rkey] = struct{}{}
		dstID, dstFresh := reg.GetOrAssignFresh(f.TargetDID)
		if dstFresh {
			if err := w.AppendRegistryEntry(dstID, f.TargetDID, now); err != nil {
				return fmt.Errorf("registry dst %d: %w", dstID, err)
			}
			seenRegistry[dstID] = struct{}{}
			stats.registered++
		}
		if err := w.AppendFollow(duckdbstore.FollowRow{
			SrcID:     authorID,
			DstID:     dstID,
			Rkey:      f.Rkey,
			CreatedAt: f.CreatedAt,
		}); err != nil {
			return fmt.Errorf("follow %d/%s: %w", authorID, f.Rkey, err)
		}
		stats.follows++
	}

	blockSeen := make(map[string]struct{}, len(r.Blocks))
	for _, b := range r.Blocks {
		if b.TargetDID == "" {
			continue
		}
		if _, dup := blockSeen[b.Rkey]; dup {
			continue
		}
		blockSeen[b.Rkey] = struct{}{}
		dstID, dstFresh := reg.GetOrAssignFresh(b.TargetDID)
		if dstFresh {
			if err := w.AppendRegistryEntry(dstID, b.TargetDID, now); err != nil {
				return fmt.Errorf("registry dst %d: %w", dstID, err)
			}
			seenRegistry[dstID] = struct{}{}
			stats.registered++
		}
		if err := w.AppendBlock(duckdbstore.BlockRow{
			SrcID:     authorID,
			DstID:     dstID,
			Rkey:      b.Rkey,
			CreatedAt: b.CreatedAt,
		}); err != nil {
			return fmt.Errorf("block %d/%s: %w", authorID, b.Rkey, err)
		}
		stats.blocks++
	}
	return nil
}

// loadProcessedActorIDs reads actor_ids of every actor whose repo has
// already been processed in a prior run.
func loadProcessedActorIDs(s *duckdbstore.Store) ([]int64, error) {
	rows, err := s.DB.Query(`SELECT actor_id FROM actors WHERE repo_processed = TRUE`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// removeIfExists deletes path if it's there.
func removeIfExists(path string) error {
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
