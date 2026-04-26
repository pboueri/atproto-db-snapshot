// Package runcmd implements the `at-snapshot run` subcommand: a long-running
// jetstream consumer that writes date-partitioned parquet to object storage.
//
// The package is named runcmd (not run) to keep the import path
// non-conflicting with helper functions named "run" elsewhere in the binary.
//
// Design:
//
//   - One jetstream subscription at a time, with cursor microseconds
//     checkpointed to a local sqlite (and mirrored to object storage so a
//     fresh host can rebuild). On startup we resume from the local cursor;
//     if absent we read the mirror from objstore; if both absent we accept
//     the gap and start at "now".
//
//   - Decoded events fan out by collection into the rawio sink. Posts also
//     drop derived PostMedia rows. Filtering by language / label is applied
//     to posts only — engagement records (like/follow/repost/block) carry
//     no such metadata and are kept verbatim.
//
//   - Events whose timestamp is more than 2 days outside the current wall
//     clock are dropped (the spec calls these out as suspect / unreliable).
//
//   - A flush ticker rotates parquet shards every cfg.StatsInterval (a soft
//     bound). On each successful flush the cursor is advanced so we never
//     persist a cursor ahead of records actually in object storage.
package runcmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/atrecord"
	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/jetstream"
	"github.com/pboueri/atproto-db-snapshot/internal/model"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
	"github.com/pboueri/atproto-db-snapshot/internal/rawio"
)

// cursorObjectKey is where the cursor mirror lives in object storage. We use
// a single global key (not date-partitioned) because the cursor is a moving
// scalar, not an event we'd want to query historically.
const cursorObjectKey = "run/cursor.json"

// Deps lets tests inject a fake Subscriber, Sink, ObjStore, and clock.
type Deps struct {
	Subscriber jetstream.Subscriber
	Sink       rawio.Sink
	ObjStore   objstore.Store
	Now        func() time.Time
}

// Run wires production deps from cfg.
func Run(ctx context.Context, cfg config.Config) error {
	obj, err := objstore.FromConfig(cfg)
	if err != nil {
		return err
	}
	deps := Deps{
		Subscriber: jetstream.NewWS(cfg.JetstreamEndpoints),
		ObjStore:   obj,
		Now:        func() time.Time { return time.Now().UTC() },
	}
	return RunWith(ctx, cfg, deps)
}

// RunWith is the testable entrypoint with all deps injected.
func RunWith(ctx context.Context, cfg config.Config, deps Deps) error {
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Now().UTC() }
	}
	if deps.Sink == nil {
		s, err := rawio.New(filepath.Join(cfg.DataDir, "run-staging"), deps.ObjStore)
		if err != nil {
			return err
		}
		deps.Sink = s
	}
	// Recover any leftover staging files from a previous crash before we open
	// the cursor — re-uploading is idempotent and gets the data into objstore
	// before we advance the cursor past those events.
	if err := rawio.Recover(ctx, filepath.Join(cfg.DataDir, "run-staging"), deps.ObjStore); err != nil {
		slog.Warn("rawio recover", "err", err)
	}

	cur, err := openCursor(filepath.Join(cfg.DataDir, "run-state", "cursor.sqlite"))
	if err != nil {
		return err
	}
	defer cur.Close()

	micros, err := resumeMicros(ctx, cur, deps.ObjStore)
	if err != nil {
		return err
	}
	if micros > 0 {
		slog.Info("run resume", "cursor_micros", micros, "at", time.UnixMicro(micros).UTC())
	} else {
		slog.Info("run start at now")
	}

	collections := make([]string, 0, len(model.AllCollections))
	for _, c := range model.AllCollections {
		collections = append(collections, string(c))
	}
	events, errc := deps.Subscriber.Subscribe(ctx, jetstream.SubscribeOptions{
		Cursor:      micros,
		Collections: collections,
	})

	flt := newFilter(cfg.Languages, cfg.IncludeLabels, cfg.ExcludeLabels)

	var (
		seen        atomic.Int64
		written     atomic.Int64
		decodeErrs  atomic.Int64
		latestMicro atomic.Int64
		flushMu     sync.Mutex
	)

	// flushAndCheckpoint drains the sink and advances the cursor. Serialized
	// against itself via flushMu so a slow upload doesn't run twice.
	flushAndCheckpoint := func(ctx context.Context) error {
		flushMu.Lock()
		defer flushMu.Unlock()
		if err := deps.Sink.Flush(ctx); err != nil {
			return err
		}
		m := latestMicro.Load()
		if m == 0 {
			return nil
		}
		if err := cur.Save(ctx, m); err != nil {
			return err
		}
		return mirrorCursor(ctx, deps.ObjStore, m, deps.Now())
	}

	// Periodic flush ticker. We respect cfg.StatsInterval as the rotation
	// interval; stats logging happens on the same cadence.
	tickInterval := cfg.StatsInterval
	if tickInterval <= 0 {
		tickInterval = 30 * time.Second
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if err := flushAndCheckpoint(context.Background()); err != nil {
				slog.Warn("final flush", "err", err)
			}
			slog.Info("run shutdown",
				"events_seen", seen.Load(),
				"events_written", written.Load(),
				"decode_errors", decodeErrs.Load(),
			)
			return ctx.Err()

		case err := <-errc:
			// Stream ended (clean or with error). Either way, drain what we have.
			if ferr := flushAndCheckpoint(context.Background()); ferr != nil {
				slog.Warn("final flush", "err", ferr)
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil

		case <-ticker.C:
			slog.Info("run stats",
				"events_seen", seen.Load(),
				"events_written", written.Load(),
				"decode_errors", decodeErrs.Load(),
				"cursor_micros", latestMicro.Load(),
			)
			if err := flushAndCheckpoint(ctx); err != nil {
				slog.Warn("periodic flush", "err", err)
			}

		case e, ok := <-events:
			if !ok {
				if err := flushAndCheckpoint(context.Background()); err != nil {
					slog.Warn("final flush", "err", err)
				}
				return nil
			}
			seen.Add(1)
			if !timestampInWindow(e, deps.Now()) {
				continue
			}
			if e.Kind != jetstream.KindCommit || e.Commit == nil {
				continue
			}
			if err := dispatch(deps.Sink, flt, e); err != nil {
				decodeErrs.Add(1)
				slog.Debug("decode err", "did", e.DID, "collection", e.Commit.Collection, "err", err)
				continue
			}
			written.Add(1)
			if e.TimeUS > latestMicro.Load() {
				latestMicro.Store(e.TimeUS)
			}
		}
	}
}

// resumeMicros consults the local cursor first, then the objstore mirror.
// Both empty → 0 (start at "now").
func resumeMicros(ctx context.Context, cur *cursor, obj objstore.Store) (int64, error) {
	if m, ok, err := cur.Load(ctx); err != nil {
		return 0, err
	} else if ok {
		return m, nil
	}
	rc, err := obj.Get(ctx, cursorObjectKey)
	if err != nil {
		if errors.Is(err, objstore.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer rc.Close()
	var mirror cursorMirror
	if err := json.NewDecoder(rc).Decode(&mirror); err != nil {
		return 0, fmt.Errorf("runcmd: decode cursor mirror: %w", err)
	}
	return mirror.Micros, nil
}

type cursorMirror struct {
	Micros    int64     `json:"micros"`
	UpdatedAt time.Time `json:"updated_at"`
}

func mirrorCursor(ctx context.Context, obj objstore.Store, micros int64, now time.Time) error {
	body, err := json.Marshal(cursorMirror{Micros: micros, UpdatedAt: now.UTC()})
	if err != nil {
		return err
	}
	return obj.Put(ctx, cursorObjectKey, bytes.NewReader(body), "application/json")
}

// timestampInWindow drops events whose timestamp is more than two days from now.
func timestampInWindow(e jetstream.Event, now time.Time) bool {
	t := time.UnixMicro(e.TimeUS).UTC()
	delta := now.Sub(t)
	if delta < 0 {
		delta = -delta
	}
	return delta <= 48*time.Hour
}

// dispatch routes one commit event to the right Sink method.
func dispatch(sink rawio.Sink, flt filter, e jetstream.Event) error {
	c := e.Commit
	indexedAt := jetstream.EventTime(e)
	collection := model.Collection(c.Collection)
	switch collection {
	case model.CollectionPost:
		if c.Operation == jetstream.OpDelete {
			return sink.AppendPosts([]model.Post{tombstonePost(e)})
		}
		post, media, err := atrecord.DecodePost(c.Record, e.DID, c.RKey, c.CID, indexedAt, model.SourceFirehose)
		if err != nil {
			return err
		}
		if !flt.keepPost(post) {
			return nil
		}
		if err := sink.AppendPosts([]model.Post{post}); err != nil {
			return err
		}
		if len(media) > 0 {
			if err := sink.AppendPostMedia(media); err != nil {
				return err
			}
		}
		return nil
	case model.CollectionLike:
		if c.Operation == jetstream.OpDelete {
			return sink.AppendLikes([]model.Like{{
				ActorDID: e.DID, RKey: c.RKey, IndexedAt: indexedAt, Op: model.OpDelete, Source: model.SourceFirehose,
			}})
		}
		like, err := atrecord.DecodeLike(c.Record, e.DID, c.RKey, indexedAt, model.SourceFirehose)
		if err != nil {
			return err
		}
		return sink.AppendLikes([]model.Like{like})
	case model.CollectionRepost:
		if c.Operation == jetstream.OpDelete {
			return sink.AppendReposts([]model.Repost{{
				ActorDID: e.DID, RKey: c.RKey, IndexedAt: indexedAt, Op: model.OpDelete, Source: model.SourceFirehose,
			}})
		}
		rp, err := atrecord.DecodeRepost(c.Record, e.DID, c.RKey, indexedAt, model.SourceFirehose)
		if err != nil {
			return err
		}
		return sink.AppendReposts([]model.Repost{rp})
	case model.CollectionFollow:
		if c.Operation == jetstream.OpDelete {
			return sink.AppendFollows([]model.Follow{{
				SrcDID: e.DID, RKey: c.RKey, IndexedAt: indexedAt, Op: model.OpDelete, Source: model.SourceFirehose,
			}})
		}
		f, err := atrecord.DecodeFollow(c.Record, e.DID, c.RKey, indexedAt, model.SourceFirehose)
		if err != nil {
			return err
		}
		return sink.AppendFollows([]model.Follow{f})
	case model.CollectionBlock:
		if c.Operation == jetstream.OpDelete {
			return sink.AppendBlocks([]model.Block{{
				SrcDID: e.DID, RKey: c.RKey, IndexedAt: indexedAt, Op: model.OpDelete, Source: model.SourceFirehose,
			}})
		}
		b, err := atrecord.DecodeBlock(c.Record, e.DID, c.RKey, indexedAt, model.SourceFirehose)
		if err != nil {
			return err
		}
		return sink.AppendBlocks([]model.Block{b})
	case model.CollectionProfile:
		if c.Operation == jetstream.OpDelete {
			return sink.AppendProfiles([]model.Profile{{
				DID: e.DID, IndexedAt: indexedAt, Op: model.OpDelete, Source: model.SourceFirehose,
			}})
		}
		p, err := atrecord.DecodeProfile(c.Record, e.DID, indexedAt, model.SourceFirehose)
		if err != nil {
			return err
		}
		return sink.AppendProfiles([]model.Profile{p})
	}
	return nil
}

func tombstonePost(e jetstream.Event) model.Post {
	uri := fmt.Sprintf("at://%s/%s/%s", e.DID, model.CollectionPost, e.Commit.RKey)
	return model.Post{
		URI:       uri,
		DID:       e.DID,
		RKey:      e.Commit.RKey,
		IndexedAt: jetstream.EventTime(e),
		Op:        model.OpDelete,
		Source:    model.SourceFirehose,
	}
}
