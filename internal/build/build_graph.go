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
// Returns the on-disk path of the produced current_graph.duckdb.
func runGraphBackfill(ctx context.Context, cfg config.Config, logger *slog.Logger) (string, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return "", err
	}
	outPath := filepath.Join(cfg.DataDir, "current_graph.duckdb")
	for _, p := range []string{outPath, outPath + ".wal"} {
		if err := removeIfExists(p); err != nil {
			return "", err
		}
	}

	client := relay.New(cfg.RelayHost, cfg.RateLimitRPS, cfg.HTTPTimeout)

	// -------- enumerate DIDs --------
	logger.Info("listing repos", "relay", cfg.RelayHost, "limit", cfg.DIDLimit)
	dids, err := enumerateDIDs(ctx, client, cfg.DIDLimit)
	if err != nil {
		return "", fmt.Errorf("list repos: %w", err)
	}
	if len(dids) == 0 {
		return "", errors.New("listRepos returned no DIDs")
	}
	logger.Info("enumerated DIDs", "count", len(dids))

	// Pre-assign actor_ids for the DIDs we'll crawl (stable numbering).
	reg := registry.New()
	for _, d := range dids {
		reg.GetOrAssign(d)
	}

	// -------- open DuckDB --------
	store, err := duckdbstore.Open(outPath, "4GB", 4)
	if err != nil {
		return "", fmt.Errorf("open duckdb: %w", err)
	}
	defer store.Close()

	writer, err := duckdbstore.NewStreamingWriter(outPath)
	if err != nil {
		return "", fmt.Errorf("open writer: %w", err)
	}

	// -------- writer goroutine --------
	records := make(chan *parse.Records, cfg.Workers*2)
	writeDone := make(chan error, 1)
	var stats writerStats
	profiles := make(map[int64]*parse.ProfileRec)

	go func() {
		defer close(writeDone)
		defer writer.FlushAll()
		for rec := range records {
			if err := writeOne(writer, reg, rec, &stats, profiles); err != nil {
				writeDone <- err
				for range records {
				}
				return
			}
		}
		writeDone <- nil
	}()

	// -------- fan out getRepo + parse --------
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
					"fetched", atomic.LoadInt64(&fetched),
					"parsed", atomic.LoadInt64(&parsed),
					"skipped", atomic.LoadInt64(&skipped),
					"errors", atomic.LoadInt64(&fetchErr),
					"of", len(dids),
					"follows", atomic.LoadInt64(&stats.follows),
					"blocks", atomic.LoadInt64(&stats.blocks),
				)
			}
		}
	}()

	for _, did := range dids {
		did := did
		wg.Go(func() error {
			car, err := client.GetRepo(wctx, did)
			if err != nil {
				if errors.Is(err, relay.ErrNotFound) {
					atomic.AddInt64(&skipped, 1)
					return nil
				}
				atomic.AddInt64(&fetchErr, 1)
				logger.Debug("getRepo failed", "did", did, "err", err)
				return nil
			}
			atomic.AddInt64(&fetched, 1)
			recs, err := parse.Parse(wctx, did, car)
			if err != nil {
				atomic.AddInt64(&fetchErr, 1)
				logger.Debug("parse failed", "did", did, "err", err)
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
	if werr != nil {
		<-writeDone
		return "", werr
	}
	if err := <-writeDone; err != nil {
		return "", fmt.Errorf("writer: %w", err)
	}
	logger.Info("crawl complete",
		"fetched", fetched,
		"parsed", parsed,
		"skipped", skipped,
		"errors", fetchErr,
		"follows", stats.follows,
		"blocks", stats.blocks,
	)

	// -------- write actors from registry --------
	entries := reg.Snapshot()
	logger.Info("writing actors", "count", len(entries))
	now := time.Now().UTC()
	for _, e := range entries {
		row := duckdbstore.ActorRow{
			ActorID:   e.ActorID,
			DID:       e.DID,
			IndexedAt: now,
		}
		if p, ok := profiles[e.ActorID]; ok && p != nil {
			if p.DisplayName != nil {
				row.DisplayName = *p.DisplayName
			}
			if p.Description != nil {
				row.Description = *p.Description
			}
			if p.AvatarCID != nil {
				row.AvatarCID = *p.AvatarCID
			}
			if p.CreatedAt != nil && !p.CreatedAt.IsZero() {
				row.CreatedAt = *p.CreatedAt
			}
		}
		if err := writer.AppendActor(row); err != nil {
			return "", fmt.Errorf("append actor %d: %w", e.ActorID, err)
		}
	}
	if err := writer.FlushAll(); err != nil {
		return "", fmt.Errorf("final flush: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("close writer: %w", err)
	}

	logger.Info("recomputing counts")
	if err := store.RecomputeCounts(); err != nil {
		return "", fmt.Errorf("recompute counts: %w", err)
	}
	if err := store.WriteMeta("backfill-graph", int64(cfg.DIDLimit), int64(len(dids))); err != nil {
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
	follows, blocks int64
}

// writeOne resolves DIDs and streams graph rows from one repo to DuckDB.
// Posts / likes / reposts parsed out of the CAR are intentionally ignored
// here — they belong to the parquet event log path, not the graph snapshot.
func writeOne(
	w *duckdbstore.StreamingWriter,
	reg *registry.Registry,
	r *parse.Records,
	stats *writerStats,
	profiles map[int64]*parse.ProfileRec,
) error {
	authorID := reg.GetOrAssign(r.AuthorDID)
	if r.Profile != nil {
		profiles[authorID] = r.Profile
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
		err := w.AppendFollow(duckdbstore.FollowRow{
			SrcID:     authorID,
			DstID:     reg.GetOrAssign(f.TargetDID),
			Rkey:      f.Rkey,
			CreatedAt: f.CreatedAt,
		})
		if err != nil {
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
		err := w.AppendBlock(duckdbstore.BlockRow{
			SrcID:     authorID,
			DstID:     reg.GetOrAssign(b.TargetDID),
			Rkey:      b.Rkey,
			CreatedAt: b.CreatedAt,
		})
		if err != nil {
			return fmt.Errorf("block %d/%s: %w", authorID, b.Rkey, err)
		}
		stats.blocks++
	}
	return nil
}

// enumerateDIDs paginates listRepos until we have `limit` active DIDs.
func enumerateDIDs(ctx context.Context, c *relay.Client, limit int) ([]string, error) {
	var out []string
	err := c.ListRepos(ctx, 1000, func(page []relay.ListReposItem) error {
		for _, item := range page {
			if item.DID == "" {
				continue
			}
			if !item.Active && item.Status != "" {
				continue
			}
			out = append(out, item.DID)
			if limit > 0 && len(out) >= limit {
				return errStopIter
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopIter) {
		return nil, err
	}
	return out, nil
}

var errStopIter = errors.New("stop iteration")

func removeIfExists(path string) error {
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
