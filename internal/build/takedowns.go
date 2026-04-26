package build

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Takedowns is the in-memory representation of takedowns.yaml. See
// specs/001_bootstrap.md §19.
type Takedowns struct {
	URIs []TakedownEntry
}

// TakedownEntry is one (uri, reason, date) tuple. URI is an at:// reference
// of the form `at://<did>/<collection>/<rkey>`. Reason is free text recorded
// in the audit table; Date is informational.
type TakedownEntry struct {
	URI    string
	Reason string
	Date   string
}

// takedownsYAML matches the on-disk YAML schema:
//
//	takedowns:
//	  - uri: at://did:plc:xxx/app.bsky.feed.post/3abc
//	    reason: CSAM report 2026-03-14
//	    date: 2026-03-14
type takedownsYAML struct {
	Takedowns []takedownEntryYAML `yaml:"takedowns"`
}

type takedownEntryYAML struct {
	URI    string `yaml:"uri"`
	Reason string `yaml:"reason"`
	Date   string `yaml:"date"`
}

// LoadTakedowns reads YAML at path and returns the parsed list. A missing
// file returns an empty Takedowns struct with no error so callers can
// treat the file as optional. All other read / parse errors are returned.
func LoadTakedowns(path string) (Takedowns, error) {
	if path == "" {
		return Takedowns{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Takedowns{}, nil
		}
		return Takedowns{}, fmt.Errorf("read %s: %w", path, err)
	}
	var raw takedownsYAML
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return Takedowns{}, fmt.Errorf("parse %s: %w", path, err)
	}
	out := Takedowns{URIs: make([]TakedownEntry, 0, len(raw.Takedowns))}
	for _, e := range raw.Takedowns {
		uri := strings.TrimSpace(e.URI)
		if uri == "" {
			continue
		}
		out.URIs = append(out.URIs, TakedownEntry{
			URI:    uri,
			Reason: e.Reason,
			Date:   e.Date,
		})
	}
	return out, nil
}

// parseTakedownURI splits an at://<did>/<collection>/<rkey> URI into its
// three components. Returns an error on malformed input.
func parseTakedownURI(uri string) (did, collection, rkey string, err error) {
	const prefix = "at://"
	if !strings.HasPrefix(uri, prefix) {
		return "", "", "", fmt.Errorf("takedown uri %q: missing at:// prefix", uri)
	}
	rest := uri[len(prefix):]
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("takedown uri %q: expected at://<did>/<collection>/<rkey>", uri)
	}
	if parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", fmt.Errorf("takedown uri %q: empty did/collection/rkey", uri)
	}
	return parts[0], parts[1], parts[2], nil
}

// EnsureTakedownsAppliedTable creates the audit / idempotence sidecar
// table inside current_all.duckdb if it does not yet exist. Stores the
// URIs that have already been processed so a second build does not
// double-process them.
func EnsureTakedownsAppliedTable(ctx context.Context, db *sql.DB) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS takedowns_applied (
  uri        VARCHAR PRIMARY KEY,
  reason     VARCHAR,
  applied_at TIMESTAMP
);`
	_, err := db.ExecContext(ctx, ddl)
	return err
}

// ApplyTakedowns nullifies / deletes rows in current_all.duckdb for each
// entry in t. URIs already present in `takedowns_applied` are skipped so
// repeated builds are idempotent. Per §19 strategy:
//
//   - app.bsky.feed.post: nullify content fields, keep the row for
//     referential integrity (replies / quotes referencing it stay valid).
//   - app.bsky.actor.profile: nullify mutable profile fields on the actor.
//   - app.bsky.feed.like / app.bsky.feed.repost: delete the row outright
//     (content-less; no value in retaining it).
//   - app.bsky.graph.follow / app.bsky.graph.block: delete outright.
//
// Counts of rows touched per collection are logged at INFO. Missing actors
// or rows are silent no-ops; takedowns are advisory and should not fail
// builds.
func ApplyTakedowns(ctx context.Context, db *sql.DB, t Takedowns, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if len(t.URIs) == 0 {
		return nil
	}

	// Tally rows touched per collection for logging.
	type stat struct {
		processed int
		skipped   int
		touched   int64
	}
	stats := map[string]*stat{}
	statFor := func(c string) *stat {
		s, ok := stats[c]
		if !ok {
			s = &stat{}
			stats[c] = s
		}
		return s
	}

	for _, e := range t.URIs {
		did, coll, rkey, err := parseTakedownURI(e.URI)
		if err != nil {
			return err
		}
		s := statFor(coll)

		// Idempotence guard.
		var seen int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM takedowns_applied WHERE uri = ?`, e.URI,
		).Scan(&seen); err != nil {
			return fmt.Errorf("check takedowns_applied: %w", err)
		}
		if seen > 0 {
			s.skipped++
			continue
		}

		var n int64
		switch coll {
		case "app.bsky.feed.post":
			res, err := db.ExecContext(ctx, `
				UPDATE posts SET
				  text = NULL,
				  cid = NULL,
				  embed_type = NULL,
				  lang = NULL,
				  reply_root_rkey = NULL,
				  reply_parent_rkey = NULL,
				  quote_rkey = NULL
				 WHERE author_id = (SELECT actor_id FROM actors_registry WHERE did = ?)
				   AND rkey = ?`,
				did, rkey,
			)
			if err != nil {
				return fmt.Errorf("takedown post %s: %w", e.URI, err)
			}
			n, _ = res.RowsAffected()

		case "app.bsky.actor.profile":
			res, err := db.ExecContext(ctx, `
				UPDATE actors SET
				  display_name = NULL,
				  description = NULL,
				  avatar_cid = NULL
				 WHERE did = ?`,
				did,
			)
			if err != nil {
				return fmt.Errorf("takedown profile %s: %w", e.URI, err)
			}
			n, _ = res.RowsAffected()

		case "app.bsky.feed.like":
			res, err := db.ExecContext(ctx, `
				DELETE FROM likes_current
				 WHERE liker_id = (SELECT actor_id FROM actors_registry WHERE did = ?)
				   AND rkey = ?`,
				did, rkey,
			)
			if err != nil {
				return fmt.Errorf("takedown like %s: %w", e.URI, err)
			}
			n, _ = res.RowsAffected()

		case "app.bsky.feed.repost":
			res, err := db.ExecContext(ctx, `
				DELETE FROM reposts_current
				 WHERE reposter_id = (SELECT actor_id FROM actors_registry WHERE did = ?)
				   AND rkey = ?`,
				did, rkey,
			)
			if err != nil {
				return fmt.Errorf("takedown repost %s: %w", e.URI, err)
			}
			n, _ = res.RowsAffected()

		case "app.bsky.graph.follow":
			res, err := db.ExecContext(ctx, `
				DELETE FROM follows_current
				 WHERE src_id = (SELECT actor_id FROM actors_registry WHERE did = ?)
				   AND rkey = ?`,
				did, rkey,
			)
			if err != nil {
				return fmt.Errorf("takedown follow %s: %w", e.URI, err)
			}
			n, _ = res.RowsAffected()

		case "app.bsky.graph.block":
			res, err := db.ExecContext(ctx, `
				DELETE FROM blocks_current
				 WHERE src_id = (SELECT actor_id FROM actors_registry WHERE did = ?)
				   AND rkey = ?`,
				did, rkey,
			)
			if err != nil {
				return fmt.Errorf("takedown block %s: %w", e.URI, err)
			}
			n, _ = res.RowsAffected()

		default:
			logger.Warn("takedown: unknown collection; recorded but no rows touched",
				"uri", e.URI, "collection", coll)
		}

		s.processed++
		s.touched += n

		if _, err := db.ExecContext(ctx,
			`INSERT INTO takedowns_applied (uri, reason, applied_at) VALUES (?, ?, now())`,
			e.URI, e.Reason,
		); err != nil {
			return fmt.Errorf("record takedowns_applied %s: %w", e.URI, err)
		}
	}

	for coll, s := range stats {
		logger.Info("takedowns applied",
			"collection", coll,
			"processed", s.processed,
			"skipped_already_applied", s.skipped,
			"rows_touched", s.touched,
		)
	}
	return nil
}
