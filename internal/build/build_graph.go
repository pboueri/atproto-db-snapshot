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
	"sort"
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
// specs/003_listrecords_backfill.md. Five phases:
//
//  1. PLC enumeration — paginate plc.directory/export, replay last-write-
//     wins per DID, persist (did, endpoint) into pds_endpoints. Skipped
//     entirely when MostRecentPDSResolveAt is within cfg.PLC.RefreshDays.
//  2. Bucket DIDs by host — load pds_endpoints, subtract DIDs whose
//     repos are already processed, partition by endpoint host.
//  3. Per-host listRecords pools — one errgroup per host (size
//     cfg.PDS.PerHostWorkers); each worker fetches profile/follow/block
//     for one DID and emits a *parse.Records.
//  4. Writer goroutine — drains records into DuckDB (unchanged from the
//     legacy CAR path).
//  5. Optional Constellation enrichment — per-DID /links/all, updates
//     actors.likes_received_count / reposts_received_count.
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

	// -------- Phase 1: PLC enumeration --------
	if err := phase1PLC(ctx, cfg, store, logger); err != nil {
		return "", fmt.Errorf("plc: %w", err)
	}

	// -------- Phase 2: load + bucket DIDs by host --------
	allEndpoints, err := store.LoadPDSEndpoints()
	if err != nil {
		return "", fmt.Errorf("load pds_endpoints: %w", err)
	}
	logger.Info("pds_endpoints loaded", "total", len(allEndpoints))
	hosts, totalDIDs := bucketByHost(allEndpoints, processed, cfg.DIDLimit)
	logger.Info("bucket-by-host complete", "hosts", len(hosts), "dids_total", totalDIDs)

	if totalDIDs == 0 {
		logger.Info("no DIDs to process; skipping listRecords")
	}

	// -------- Phase 3+4: per-host pools + writer --------
	stats, err := phase3Dispatch(ctx, cfg, store, reg, hosts, totalDIDs, logger)
	if err != nil {
		return "", err
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
		"follows", stats.follows,
		"blocks", stats.blocks,
		"actors_registered", stats.registered,
	)
	return outPath, nil
}

// ---------- Phase 1: PLC enumeration ----------

func phase1PLC(ctx context.Context, cfg config.Config, store *duckdbstore.Store, logger *slog.Logger) error {
	cursorPath := filepath.Join(cfg.DataDir, "plc_cursor.json")

	// Skip a fresh dump if the most recent resolved_at is within RefreshDays.
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

	endpoints := map[string]string{}
	tombstones := map[string]struct{}{}
	var (
		opCount     int64
		flushedRows int64
	)
	const flushEvery = 50_000

	flush := func(force bool) error {
		if !force && len(endpoints)+len(tombstones) < flushEvery {
			return nil
		}
		if len(endpoints) > 0 {
			if err := store.BulkUpsertPDSEndpoints(endpoints, time.Now().UTC()); err != nil {
				return fmt.Errorf("bulk upsert: %w", err)
			}
			flushedRows += int64(len(endpoints))
			endpoints = map[string]string{}
		}
		if len(tombstones) > 0 {
			ts := make([]string, 0, len(tombstones))
			for d := range tombstones {
				ts = append(ts, d)
			}
			if err := store.DeletePDSEndpoints(ts); err != nil {
				return fmt.Errorf("delete tombstones: %w", err)
			}
			tombstones = map[string]struct{}{}
		}
		return nil
	}

	final, err := cli.Stream(ctx, cursor.Cursor, func(op plc.Op) error {
		opCount++
		if op.Tombstone {
			tombstones[op.DID] = struct{}{}
			delete(endpoints, op.DID)
		} else if op.Endpoint != "" {
			endpoints[op.DID] = op.Endpoint
			delete(tombstones, op.DID)
		}
		// Periodic checkpoint of cursor + flush.
		if opCount%int64(flushEvery) == 0 {
			if err := flush(true); err != nil {
				return err
			}
			if err := plc.SaveCursorAtomic(cursorPath, plc.CursorState{Cursor: op.RawCursor}); err != nil {
				logger.Warn("plc cursor save failed", "err", err)
			}
		}
		return nil
	})
	if err != nil {
		// Persist whatever progress we made before bubbling up.
		_ = flush(true)
		_ = plc.SaveCursorAtomic(cursorPath, plc.CursorState{Cursor: final})
		return err
	}
	if err := flush(true); err != nil {
		return err
	}
	if err := plc.SaveCursorAtomic(cursorPath, plc.CursorState{Cursor: final}); err != nil {
		logger.Warn("plc cursor save failed", "err", err)
	}
	logger.Info("phase 1: complete",
		"ops_consumed", opCount,
		"endpoints_written", flushedRows,
		"final_cursor", final,
	)
	return nil
}

// ---------- Phase 2: bucket DIDs by host ----------

// hostBucket carries the sorted DID list for one PDS host.
type hostBucket struct {
	host string
	dids []string
}

// bucketByHost partitions the (did → endpoint) map by URL host. DIDs
// that already appear in `processed` are skipped. If didLimit > 0, the
// total returned across all hosts is capped at didLimit.
func bucketByHost(endpoints map[string]string, processed map[string]struct{}, didLimit int) ([]*hostBucket, int) {
	byHost := map[string][]string{}
	for did, ep := range endpoints {
		if _, done := processed[did]; done {
			continue
		}
		host := normalizeHost(ep)
		if host == "" {
			continue
		}
		byHost[host] = append(byHost[host], did)
	}
	hosts := make([]*hostBucket, 0, len(byHost))
	for h, dids := range byHost {
		sort.Strings(dids)
		hosts = append(hosts, &hostBucket{host: h, dids: dids})
	}
	// Larger buckets first — pessimistic scheduler so big hosts start
	// being drained as early as possible.
	sort.Slice(hosts, func(i, j int) bool { return len(hosts[i].dids) > len(hosts[j].dids) })

	total := 0
	if didLimit > 0 {
		// Trim buckets in descending order until we hit the cap.
		out := []*hostBucket{}
		for _, h := range hosts {
			if total >= didLimit {
				break
			}
			room := didLimit - total
			if len(h.dids) > room {
				h.dids = h.dids[:room]
			}
			total += len(h.dids)
			out = append(out, h)
		}
		return out, total
	}
	for _, h := range hosts {
		total += len(h.dids)
	}
	return hosts, total
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

// ---------- Phase 3+4: dispatch + writer ----------

type backfillStats struct {
	follows         int64
	blocks          int64
	registered      int64
	didsDone        int64
	didsInFlight    int64
	hostsActive     int64
	hostsCircuit    int64
	errors429       int64
	errors5xx       int64
	errorsTimeout   int64
	errorsOther     int64
}

func phase3Dispatch(
	ctx context.Context,
	cfg config.Config,
	store *duckdbstore.Store,
	reg *registry.Registry,
	hosts []*hostBucket,
	totalDIDs int,
	logger *slog.Logger,
) (*backfillStats, error) {
	stats := &backfillStats{}

	writer, err := duckdbstore.NewStreamingWriter(filepath.Join(cfg.DataDir, "current_graph.duckdb"))
	if err != nil {
		return stats, fmt.Errorf("open writer: %w", err)
	}

	// Pre-seed dedup sets so resume doesn't double-insert.
	seenRegistry := make(map[int64]struct{})
	for _, e := range reg.Snapshot() {
		seenRegistry[e.ActorID] = struct{}{}
	}
	seenProcessed := make(map[int64]struct{})
	if processedIDs, err := loadProcessedActorIDs(store); err == nil {
		for _, id := range processedIDs {
			seenProcessed[id] = struct{}{}
		}
	} else {
		writer.Close()
		return stats, fmt.Errorf("load processed actor_ids: %w", err)
	}

	records := make(chan *parse.Records, cfg.PDS.PerHostWorkers*4)
	writeDone := make(chan error, 1)
	go writerLoop(ctx, writer, store, reg, records, seenRegistry, seenProcessed, stats, logger, writeDone)

	// Progress logger.
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
					"dids_total", totalDIDs,
					"dids_done", atomic.LoadInt64(&stats.didsDone),
					"dids_in_flight", atomic.LoadInt64(&stats.didsInFlight),
					"hosts_active", atomic.LoadInt64(&stats.hostsActive),
					"hosts_circuit_open", atomic.LoadInt64(&stats.hostsCircuit),
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

	// One errgroup per host. Total in-flight = len(hosts) * PerHostWorkers.
	g, gctx := errgroup.WithContext(ctx)
	for _, hb := range hosts {
		hb := hb
		atomic.AddInt64(&stats.hostsActive, 1)
		client := pds.New(hb.host, pds.Options{
			HTTPTimeout:      cfg.PDS.HTTPTimeout,
			RPS:              cfg.PDS.PerHostRPS,
			MaxRetries:       cfg.PDS.MaxRetries,
			BreakerThreshold: cfg.PDS.BreakerThreshold,
			BreakerCooldown:  cfg.PDS.BreakerCooldown,
		})
		g.Go(func() error {
			defer atomic.AddInt64(&stats.hostsActive, -1)
			return runHostPool(gctx, hb, client, cfg.PDS.PerHostWorkers, records, stats, logger)
		})
	}

	werr := g.Wait()
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
	return stats, werr
}

// runHostPool fans out PerHostWorkers goroutines for a single host's DID
// list. Each worker pulls from a channel, calls listRecords for the
// three target collections, and publishes a parse.Records on `out`.
func runHostPool(
	ctx context.Context,
	hb *hostBucket,
	client *pds.Client,
	workers int,
	out chan<- *parse.Records,
	stats *backfillStats,
	logger *slog.Logger,
) error {
	if workers <= 0 {
		workers = 1
	}
	in := make(chan string, workers*2)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for did := range in {
				atomic.AddInt64(&stats.didsInFlight, 1)
				rec, err := fetchOne(ctx, client, did, stats, logger)
				atomic.AddInt64(&stats.didsInFlight, -1)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					// fetchOne already accounted error counters; just move on.
				}
				if rec != nil {
					select {
					case out <- rec:
					case <-ctx.Done():
						return
					}
				}
				atomic.AddInt64(&stats.didsDone, 1)
			}
		}()
	}
	// Producer: feed DIDs.
	for _, did := range hb.dids {
		select {
		case in <- did:
		case <-ctx.Done():
			close(in)
			wg.Wait()
			if client.CircuitOpen() {
				atomic.AddInt64(&stats.hostsCircuit, 1)
			}
			return ctx.Err()
		}
	}
	close(in)
	wg.Wait()
	if client.CircuitOpen() {
		atomic.AddInt64(&stats.hostsCircuit, 1)
	}
	return nil
}

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
			// Skippable error — still want to mark this DID processed
			// with a bare actor row (no profile, no edges). Return rec so
			// the writer registers the row.
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
		// Already accounted by the design — don't double-count.
		return true
	case errors.Is(err, pds.ErrCircuitOpen):
		// Treat as skip; the per-host breaker handles cooldown.
		return true
	case errors.Is(err, context.DeadlineExceeded):
		atomic.AddInt64(&stats.errorsTimeout, 1)
		return true
	}
	// Best-effort string sniff for the remaining buckets.
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
		ok      int64
		failed  int64
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
		// Follower count overwrites the recomputed value.
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

// writerStats accumulates per-collection write counts for progress logging.
// Wraps the relevant counters in backfillStats for backward-compat with
// writeOne's signature.
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
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()

	checkpoint := func() {
		if err := w.FlushAll(); err != nil {
			logger.Warn("periodic flush failed", "err", err)
			return
		}
		if err := s.Checkpoint(); err != nil {
			logger.Warn("periodic checkpoint failed", "err", err)
			return
		}
	}

	for {
		select {
		case <-tick.C:
			checkpoint()
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
// Append order per record:
//  1. Author registry entry (if new) + author actors row (if new).
//  2. For each follow / block: target registry entry (if new) + edge row.
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

// removeIfExists deletes path if it's there. Used by other build paths
// that intentionally rebuild the graph file from scratch.
func removeIfExists(path string) error {
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
