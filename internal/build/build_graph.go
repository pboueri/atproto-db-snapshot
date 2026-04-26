package build

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/duckdbstore"
	"github.com/pboueri/atproto-db-snapshot/internal/parse"
	"github.com/pboueri/atproto-db-snapshot/internal/registry"
	"github.com/pboueri/atproto-db-snapshot/internal/relay"
)

// runGraphBackfill executes a streaming CAR-backfill targeting
// current_graph.duckdb (actors + follows + blocks only). Posts / likes /
// reposts live in the parquet archive + current_all.duckdb — those are
// produced by `run` (Jetstream consumer) and the incremental `build` path,
// NOT by CAR backfill per spec §7.
//
// Resume model (spec §7 addendum):
//
//   - actors_registry is written incrementally and checkpointed every
//     ~30s, so DID → actor_id assignments survive crashes.
//   - actors.repo_processed is set TRUE when a DID's CAR has been
//     successfully parsed; on resume those DIDs are skipped from the
//     listRepos enumeration.
//   - listRepos and getRepo are pipelined: workers begin pulling repos
//     as soon as the first page arrives, instead of waiting for the
//     full DID enumeration to finish.
//
// Returns the on-disk path of the produced current_graph.duckdb.
func runGraphBackfill(ctx context.Context, cfg config.Config, logger *slog.Logger) (string, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return "", err
	}
	outPath := filepath.Join(cfg.DataDir, "current_graph.duckdb")

	// -------- open store; rehydrate from prior run if present --------
	store, err := duckdbstore.Open(outPath, "4GB", 4)
	if err != nil {
		return "", fmt.Errorf("open duckdb: %w", err)
	}
	defer store.Close()

	complete, err := store.IsComplete()
	if err != nil {
		return "", fmt.Errorf("check completion: %w", err)
	}

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
	if len(processed) > 0 || len(prior) > 0 {
		logger.Info("resume detected",
			"registry_actors", len(prior),
			"processed_dids", len(processed),
			"complete_meta_present", complete,
		)
	}

	client := relay.New(cfg.RelayHost, cfg.RateLimitRPS, cfg.HTTPTimeout)

	writer, err := duckdbstore.NewStreamingWriter(outPath)
	if err != nil {
		return "", fmt.Errorf("open writer: %w", err)
	}

	// -------- writer goroutine --------
	records := make(chan *parse.Records, cfg.Workers*2)
	writeDone := make(chan error, 1)
	var stats writerStats

	// In-memory dedup sets for the writer. Pre-seeded from disk so
	// resume doesn't try to re-insert rows that already exist.
	seenRegistry := make(map[int64]struct{}, len(prior)+1024)
	for _, e := range prior {
		seenRegistry[e.ActorID] = struct{}{}
	}
	seenProcessedActors := make(map[int64]struct{}, len(processed)+1024)
	// We can't know the actor_ids of the processed DIDs without an
	// actors-table scan, so do that scan once (it's cheap relative to
	// the crawl).
	if processedIDs, err := loadProcessedActorIDs(store); err == nil {
		for _, id := range processedIDs {
			seenProcessedActors[id] = struct{}{}
		}
	} else {
		return "", fmt.Errorf("load processed actor_ids: %w", err)
	}

	go writerLoop(ctx, writer, store, reg, records,
		seenRegistry, seenProcessedActors, &stats, logger, writeDone)

	// -------- pipelined listRepos + getRepo --------
	dids := make(chan string, 4096) // buffer ~4 pages of listRepos
	enumDone := make(chan error, 1)
	var (
		enumerated int64
		skippedDup int64
	)

	go func() {
		defer close(dids)
		err := client.ListRepos(ctx, 1000, func(page []relay.ListReposItem) error {
			for _, item := range page {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if item.DID == "" {
					continue
				}
				if !item.Active && item.Status != "" {
					continue
				}
				if _, done := processed[item.DID]; done {
					atomic.AddInt64(&skippedDup, 1)
					continue
				}
				atomic.AddInt64(&enumerated, 1)
				select {
				case dids <- item.DID:
				case <-ctx.Done():
					return ctx.Err()
				}
				if cfg.DIDLimit > 0 && atomic.LoadInt64(&enumerated) >= int64(cfg.DIDLimit) {
					return errStopIter
				}
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStopIter) {
			enumDone <- err
			return
		}
		enumDone <- nil
	}()

	// -------- fan-out workers (start before enumerate completes) --------
	var (
		fetched  int64
		parsed   int64
		skipped  int64
		fetchErr int64
	)

	wg, wctx := errgroup.WithContext(ctx)
	wg.SetLimit(cfg.Workers)

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
					"enumerated", atomic.LoadInt64(&enumerated),
					"resumed_skipped", atomic.LoadInt64(&skippedDup),
					"fetched", atomic.LoadInt64(&fetched),
					"parsed", atomic.LoadInt64(&parsed),
					"skipped_404", atomic.LoadInt64(&skipped),
					"errors", atomic.LoadInt64(&fetchErr),
					"follows", atomic.LoadInt64(&stats.follows),
					"blocks", atomic.LoadInt64(&stats.blocks),
					"actors_registered", atomic.LoadInt64(&stats.registered),
				)
			}
		}
	}()

	// Worker pool consumes dids as enumeration produces them.
	for did := range dids {
		did := did
		wg.Go(func() error {
			car, err := client.GetRepo(wctx, did)
			if err != nil {
				if errors.Is(err, relay.ErrNotFound) {
					atomic.AddInt64(&skipped, 1)
					return nil
				}
				atomic.AddInt64(&fetchErr, 1)
				logger.Warn("getRepo failed", "did", did, "err", err)
				return nil
			}
			atomic.AddInt64(&fetched, 1)
			recs, err := parse.Parse(wctx, did, car)
			if err != nil {
				atomic.AddInt64(&fetchErr, 1)
				logger.Warn("parse failed", "did", did, "err", err)
				return nil
			}
			atomic.AddInt64(&parsed, 1)
			select {
			case records <- recs:
			case <-wctx.Done():
				return wctx.Err()
			}
			return nil
		})
	}

	werr := wg.Wait()
	close(records)
	close(progDone)

	if eerr := <-enumDone; eerr != nil && werr == nil {
		werr = fmt.Errorf("listRepos: %w", eerr)
	}
	if werr != nil {
		<-writeDone // drain
		return "", werr
	}
	if err := <-writeDone; err != nil {
		return "", fmt.Errorf("writer: %w", err)
	}

	logger.Info("crawl complete",
		"enumerated", enumerated,
		"resumed_skipped", skippedDup,
		"fetched", fetched,
		"parsed", parsed,
		"skipped_404", skipped,
		"errors", fetchErr,
		"follows", stats.follows,
		"blocks", stats.blocks,
		"actors_registered", stats.registered,
	)

	// -------- final flush + materialize target-only actors --------
	if err := writer.FlushAll(); err != nil {
		return "", fmt.Errorf("final flush: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("close writer: %w", err)
	}

	logger.Info("materializing target-only actors")
	if err := store.MaterializeTargetOnlyActors(time.Now().UTC()); err != nil {
		return "", fmt.Errorf("materialize target-only: %w", err)
	}

	logger.Info("recomputing counts")
	if err := store.RecomputeCounts(); err != nil {
		return "", fmt.Errorf("recompute counts: %w", err)
	}
	if err := store.WriteMeta("backfill-graph", int64(cfg.DIDLimit), int64(reg.Len())); err != nil {
		return "", fmt.Errorf("write meta: %w", err)
	}
	logger.Info("checkpointing")
	if err := store.Checkpoint(); err != nil {
		return "", fmt.Errorf("checkpoint: %w", err)
	}

	logger.Info("graph backfill complete", "path", outPath)
	return outPath, nil
}

// writerStats accumulates per-collection write counts for progress logging.
type writerStats struct {
	follows    int64
	blocks     int64
	registered int64
}

// writerLoop drains the records channel into DuckDB. It periodically
// flushes appenders and checkpoints the WAL so a crash doesn't lose
// more than ~30s of crawl work.
func writerLoop(
	ctx context.Context,
	w *duckdbstore.StreamingWriter,
	s *duckdbstore.Store,
	reg *registry.Registry,
	in <-chan *parse.Records,
	seenRegistry map[int64]struct{},
	seenProcessed map[int64]struct{},
	stats *writerStats,
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
			if err := writeOne(w, reg, rec, seenRegistry, seenProcessed, stats); err != nil {
				done <- err
				// drain remaining records so workers don't block.
				for range in {
				}
				return
			}
		case <-ctx.Done():
			done <- ctx.Err()
			return
		}
	}
}

// writeOne resolves DIDs and streams graph rows from one repo to DuckDB.
// Posts / likes / reposts parsed out of the CAR are intentionally ignored
// here — they belong to the parquet event log path, not the graph snapshot.
//
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
		// Defensive: pre-existing registry entry from a previous run we
		// didn't pre-populate. Skip the append — would violate PK.
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
// already been processed in a prior run. Used to seed the writer's
// in-memory dedup set on resume.
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

var errStopIter = errors.New("stop iteration")

// removeIfExists deletes path if it's there. Used by other build paths
// that intentionally rebuild the graph file from scratch.
func removeIfExists(path string) error {
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
