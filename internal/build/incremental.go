package build

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/duckdbstore"
)

// remoteAllKey is the canonical object-store key for current_all.duckdb.
const remoteAllKey = "current_all-v1.duckdb"

// remoteGraphKey is the canonical object-store key for current_graph.duckdb.
const remoteGraphKey = "current_graph-v1.duckdb"

// remoteRegistryKey is where the published actors registry lives.
const remoteRegistryKey = "registry/actors.parquet"

// resolveBaseSnapshot ensures `allPath` exists locally before the
// incremental replay begins. Per spec §9 resolution order:
//
//  1. local file → use as-is
//  2. local missing, object-store has snapshot → download → use
//  3. neither → leave path absent (caller initializes empty schema)
func resolveBaseSnapshot(ctx context.Context, allPath string, opts Options, logger *slog.Logger) error {
	if _, err := os.Stat(allPath); err == nil {
		logger.Info("using local current_all.duckdb", "path", allPath)
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	if opts.Store == nil {
		logger.Info("no current_all.duckdb locally and no object store; starting empty")
		return nil
	}
	exists, err := opts.Store.Exists(ctx, remoteAllKey)
	if err != nil {
		return fmt.Errorf("check object store for %s: %w", remoteAllKey, err)
	}
	if !exists {
		logger.Info("no remote snapshot found; starting empty", "key", remoteAllKey)
		return nil
	}
	logger.Info("downloading base snapshot", "key", remoteAllKey, "to", allPath)
	if err := os.MkdirAll(filepath.Dir(allPath), 0o755); err != nil {
		return err
	}
	tmp := allPath + ".dl"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := opts.Store.Get(ctx, remoteAllKey, f); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("download %s: %w", remoteAllKey, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, allPath); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// openOrInitAll opens current_all.duckdb at allPath (creating + applying
// schema if necessary).
func openOrInitAll(allPath string) (*duckdbstore.AllStore, error) {
	if err := os.MkdirAll(filepath.Dir(allPath), 0o755); err != nil {
		return nil, err
	}
	return duckdbstore.OpenAll(allPath, "8GB", 4)
}

// dayDirInfo describes one ./data/daily/YYYY-MM-DD directory.
type dayDirInfo struct {
	day  string // "YYYY-MM-DD"
	path string
}

// listDailyDirs lists every YYYY-MM-DD directory under root, sorted
// chronologically.
func listDailyDirs(root string) ([]dayDirInfo, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []dayDirInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if !looksLikeYMD(name) {
			continue
		}
		out = append(out, dayDirInfo{day: name, path: filepath.Join(root, name)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].day < out[j].day })
	return out, nil
}

// looksLikeYMD checks for the "YYYY-MM-DD" shape with cheap validation.
func looksLikeYMD(s string) bool {
	if len(s) != 10 || s[4] != '-' || s[7] != '-' {
		return false
	}
	for i, r := range s {
		if i == 4 || i == 7 {
			continue
		}
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// replayParquet replays every daily/YYYY-MM-DD directory whose date is >
// the base snapshot's last built_at. Returns the number of days actually
// replayed and the largest jetstream cursor observed across them.
//
// Replay is in chronological order so deletes of past days flow correctly.
func replayParquet(ctx context.Context, store *duckdbstore.AllStore, dailyDir, buildDate string, filters config.FilterConfig, logger *slog.Logger) (int, int64, error) {
	dirs, err := listDailyDirs(dailyDir)
	if err != nil {
		return 0, 0, fmt.Errorf("list daily dirs: %w", err)
	}
	if len(dirs) == 0 {
		return 0, 0, nil
	}

	lastBuilt, err := store.LastBuiltAt()
	if err != nil {
		return 0, 0, fmt.Errorf("read last built_at: %w", err)
	}
	cutoff := lastBuilt.Format("2006-01-02")
	logger.Info("planning replay",
		"daily_dir", dailyDir,
		"days_present", len(dirs),
		"last_built_at", lastBuilt,
		"cutoff", cutoff,
		"build_date", buildDate,
	)

	// §14.2 scoped-recompute scratch tables. Populated per-day in
	// replayOneDay; consumed by recomputeAllCounts. DROP OR REPLACE resets
	// them to a clean slate every build.
	if _, err := store.DB.ExecContext(ctx, `
		CREATE OR REPLACE TABLE touched_actors_scratch (actor_id BIGINT);
		CREATE OR REPLACE TABLE touched_posts_scratch  (author_id BIGINT, rkey VARCHAR);
	`); err != nil {
		return 0, 0, fmt.Errorf("init scoped recompute scratch: %w", err)
	}

	var replayed int
	var maxCursor int64

	for _, d := range dirs {
		// If we've never built before, replay everything we can find.
		// Otherwise: replay days strictly newer than the cutoff.
		if !lastBuilt.IsZero() && d.day <= cutoff {
			continue
		}
		// Don't replay days from the future relative to opts.Date — protects
		// against accidental backfill from another machine's daily dir.
		if d.day > buildDate {
			continue
		}
		cursor, err := replayOneDay(ctx, store, d, filters, logger)
		if err != nil {
			return replayed, maxCursor, fmt.Errorf("replay %s: %w", d.day, err)
		}
		if cursor > maxCursor {
			maxCursor = cursor
		}
		replayed++
	}
	return replayed, maxCursor, nil
}

// replayOneDay applies a single day's parquet shards into the AllStore. It
// also returns the manifest's `jetstream_cursor_end` if present (0 otherwise).
func replayOneDay(ctx context.Context, store *duckdbstore.AllStore, d dayDirInfo, filters config.FilterConfig, logger *slog.Logger) (int64, error) {
	logger.Info("replaying day", "day", d.day)

	tx, err := store.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// Step 1: build a stage_dids set across all shards, intern via
	// actors_registry.
	if err := internDIDs(ctx, tx, d.path); err != nil {
		return 0, fmt.Errorf("intern dids: %w", err)
	}

	// Refresh the public `actors` table from the registry so newly seen
	// DIDs have a row to be referenced by FK-style joins. Pure additive.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO actors (actor_id, did, indexed_at)
		SELECT r.actor_id, r.did, r.first_seen
		  FROM actors_registry r
		  LEFT JOIN actors a ON a.actor_id = r.actor_id
		 WHERE a.actor_id IS NULL
	`); err != nil {
		return 0, fmt.Errorf("seed actors from registry: %w", err)
	}

	// Step 2: apply deletions FIRST (§14.1). Read deletions.parquet once.
	if err := applyDeletions(ctx, tx, d.path); err != nil {
		return 0, fmt.Errorf("apply deletions: %w", err)
	}

	excluded := excludedCollectionSet(filters.ExcludeCollections)

	// Step 3: upsert each collection. If a collection is in
	// filters.exclude_collections we skip its parquet shard entirely
	// (cheaper than DELETE-after-load).
	type step struct {
		coll string
		fn   func(context.Context, *sql.Tx, string, config.FilterConfig) error
	}
	steps := []step{
		{"app.bsky.actor.profile", applyProfileUpdates},
		{"app.bsky.feed.post", applyPosts},
		{"app.bsky.feed.like", applyLikes},
		{"app.bsky.feed.repost", applyReposts},
		{"app.bsky.graph.follow", applyFollows},
		{"app.bsky.graph.block", applyBlocks},
	}
	for _, s := range steps {
		if _, skip := excluded[s.coll]; skip {
			logger.Info("skipping collection (filtered out)", "collection", s.coll, "day", d.day)
			continue
		}
		if err := s.fn(ctx, tx, d.path, filters); err != nil {
			return 0, err
		}
	}

	// Populate §14.2 touched sets for the scoped recompute.
	if err := populateTouchedSets(ctx, tx, d.path, excluded); err != nil {
		return 0, fmt.Errorf("populate touched sets: %w", err)
	}

	// Read manifest cursor before commit so we surface it whether or not
	// the file is present.
	cursor := readManifestCursor(filepath.Join(d.path, "_manifest.json"))

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit %s: %w", d.day, err)
	}
	return cursor, nil
}

// readManifestCursor returns _manifest.json's `jetstream_cursor_end` value,
// or 0 if absent / unparseable. Soft failure: this only feeds latest.json.
func readManifestCursor(path string) int64 {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	var m struct {
		End int64 `json:"jetstream_cursor_end"`
	}
	_ = json.Unmarshal(b, &m)
	return m.End
}

// parquetGlob returns the absolute path to a per-collection parquet file.
// If the file doesn't exist it returns "" so callers can skip cleanly.
func parquetGlob(dayDir, name string) string {
	p := filepath.Join(dayDir, name)
	if _, err := os.Stat(p); err != nil {
		return ""
	}
	return p
}

// sqlEscapeSingle replaces single quotes for use inside SQL literals.
func sqlEscapeSingle(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// excludedCollectionSet returns a set form of cfg.Filters.ExcludeCollections
// for O(1) lookup. Empty input → empty set.
func excludedCollectionSet(list []string) map[string]struct{} {
	out := make(map[string]struct{}, len(list))
	for _, c := range list {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		out[c] = struct{}{}
	}
	return out
}

// postsLangPredicate returns the WHERE-fragment that filters posts.parquet
// rows by language per spec §11. The fragment does NOT include the leading
// "AND" — callers stitch it in. Returns "" when no filter is configured.
//
// Shapes:
//   - langs nil/empty → "" (no filter)
//   - langs=["en"], exclude_no_lang=false →
//     `(p.lang IN ('en') OR p.lang IS NULL OR p.lang = '')`
//   - langs=["en","ja"], exclude_no_lang=true →
//     `p.lang IN ('en', 'ja')`
//
// `colRef` is the qualified column reference (e.g. "p.lang") so the same
// helper works inside any CTE.
func postsLangPredicate(colRef string, pf config.PostFilter) string {
	if len(pf.Langs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(pf.Langs))
	for _, l := range pf.Langs {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		parts = append(parts, "'"+sqlEscapeSingle(l)+"'")
	}
	if len(parts) == 0 {
		return ""
	}
	in := fmt.Sprintf("%s IN (%s)", colRef, strings.Join(parts, ", "))
	if pf.ExcludeNoLang {
		return in
	}
	return fmt.Sprintf("(%s OR %s IS NULL OR %s = '')", in, colRef, colRef)
}

// internDIDs builds a UNION-ALL temp table of every DID referenced in the
// day's parquet files, then bulk-inserts new entries into actors_registry
// using a sequential id allocator.
//
// SQL pattern:
//
//	CREATE TEMP TABLE stage_new_dids AS
//	  SELECT DISTINCT did FROM (... unions of all parquet dids ...);
//	INSERT INTO actors_registry
//	  SELECT (max_id + ROW_NUMBER() OVER (ORDER BY did)) AS actor_id,
//	         did, now()
//	    FROM stage_new_dids
//	   WHERE did NOT IN (SELECT did FROM actors_registry);
func internDIDs(ctx context.Context, tx *sql.Tx, dayDir string) error {
	parts := []string{}
	add := func(file, col string) {
		path := parquetGlob(dayDir, file)
		if path == "" {
			return
		}
		parts = append(parts,
			fmt.Sprintf(`SELECT %s AS did FROM read_parquet('%s')`,
				col, sqlEscapeSingle(path)))
	}
	// posts.parquet ships did + reply_root_uri/reply_parent_uri/quote_uri
	// which contain DIDs we want resolvable. We extract the `did://` slice
	// from each URI (everything between "at://" and the next "/").
	addURI := func(file, col string) {
		path := parquetGlob(dayDir, file)
		if path == "" {
			return
		}
		expr := fmt.Sprintf(
			`split_part(replace(%s, 'at://', ''), '/', 1)`,
			col,
		)
		parts = append(parts,
			fmt.Sprintf(`SELECT %s AS did FROM read_parquet('%s') WHERE %s IS NOT NULL`,
				expr, sqlEscapeSingle(path), col))
	}

	add("posts.parquet", "did")
	addURI("posts.parquet", "reply_root_uri")
	addURI("posts.parquet", "reply_parent_uri")
	addURI("posts.parquet", "quote_uri")

	add("likes.parquet", "liker_did")
	addURI("likes.parquet", "subject_uri")

	add("reposts.parquet", "reposter_did")
	addURI("reposts.parquet", "subject_uri")

	add("follows.parquet", "src_did")
	add("follows.parquet", "dst_did")
	add("blocks.parquet", "src_did")
	add("blocks.parquet", "dst_did")

	add("profile_updates.parquet", "did")
	add("deletions.parquet", "did")

	if len(parts) == 0 {
		return nil
	}
	union := strings.Join(parts, " UNION ALL ")
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS stage_new_dids`); err != nil {
		return err
	}
	create := fmt.Sprintf(`CREATE TEMP TABLE stage_new_dids AS
	  SELECT DISTINCT did FROM (%s) WHERE did IS NOT NULL AND did <> ''`, union)
	if _, err := tx.ExecContext(ctx, create); err != nil {
		return fmt.Errorf("stage_new_dids: %w", err)
	}

	insert := `
	INSERT INTO actors_registry
	  SELECT
	    (SELECT COALESCE(MAX(actor_id), 0) FROM actors_registry)
	      + ROW_NUMBER() OVER (ORDER BY did) AS actor_id,
	    did,
	    now() AS first_seen
	  FROM stage_new_dids
	 WHERE did NOT IN (SELECT did FROM actors_registry)
	`
	if _, err := tx.ExecContext(ctx, insert); err != nil {
		return fmt.Errorf("insert actors_registry: %w", err)
	}
	return nil
}

// applyDeletions removes rows from posts/likes_current/reposts_current/
// follows_current/blocks_current/actors based on deletions.parquet.
//
// The deletions.parquet schema is: collection, uri, did, event_ts.
// We split `uri = at://<did>/<collection>/<rkey>` and delete by
// (author_id, rkey) for posts and likes-style tables, or by (src_id, rkey)
// for follows/blocks. did → actor_id resolution rides on actors_registry.
func applyDeletions(ctx context.Context, tx *sql.Tx, dayDir string) error {
	path := parquetGlob(dayDir, "deletions.parquet")
	if path == "" {
		return nil
	}
	src := sqlEscapeSingle(path)

	// Stage with parsed rkey so the JOINs are simple equality.
	stage := fmt.Sprintf(`
		CREATE TEMP TABLE stage_deletes AS
		  SELECT
		    collection,
		    did AS author_did,
		    split_part(replace(uri, 'at://', ''), '/', 3) AS rkey
		  FROM read_parquet('%s')
		  WHERE uri IS NOT NULL AND did IS NOT NULL`,
		src,
	)
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS stage_deletes`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, stage); err != nil {
		return err
	}

	deletes := []struct {
		coll  string
		stmt  string
	}{
		{"app.bsky.feed.post", `
			DELETE FROM posts
			 WHERE (author_id, rkey) IN (
			   SELECT r.actor_id, s.rkey
			     FROM stage_deletes s
			     JOIN actors_registry r ON r.did = s.author_did
			    WHERE s.collection = 'app.bsky.feed.post'
			 )`},
		{"app.bsky.feed.like", `
			DELETE FROM likes_current
			 WHERE (liker_id, rkey) IN (
			   SELECT r.actor_id, s.rkey
			     FROM stage_deletes s
			     JOIN actors_registry r ON r.did = s.author_did
			    WHERE s.collection = 'app.bsky.feed.like'
			 )`},
		{"app.bsky.feed.repost", `
			DELETE FROM reposts_current
			 WHERE (reposter_id, rkey) IN (
			   SELECT r.actor_id, s.rkey
			     FROM stage_deletes s
			     JOIN actors_registry r ON r.did = s.author_did
			    WHERE s.collection = 'app.bsky.feed.repost'
			 )`},
		{"app.bsky.graph.follow", `
			DELETE FROM follows_current
			 WHERE (src_id, rkey) IN (
			   SELECT r.actor_id, s.rkey
			     FROM stage_deletes s
			     JOIN actors_registry r ON r.did = s.author_did
			    WHERE s.collection = 'app.bsky.graph.follow'
			 )`},
		{"app.bsky.graph.block", `
			DELETE FROM blocks_current
			 WHERE (src_id, rkey) IN (
			   SELECT r.actor_id, s.rkey
			     FROM stage_deletes s
			     JOIN actors_registry r ON r.did = s.author_did
			    WHERE s.collection = 'app.bsky.graph.block'
			 )`},
	}
	for _, d := range deletes {
		if _, err := tx.ExecContext(ctx, d.stmt); err != nil {
			return fmt.Errorf("delete %s: %w", d.coll, err)
		}
	}
	return nil
}

// applyProfileUpdates merges profile_updates.parquet rows into actors,
// using ON CONFLICT DO UPDATE on actor_id. Only mutable profile fields
// are touched.
func applyProfileUpdates(ctx context.Context, tx *sql.Tx, dayDir string, _ config.FilterConfig) error {
	path := parquetGlob(dayDir, "profile_updates.parquet")
	if path == "" {
		return nil
	}
	src := sqlEscapeSingle(path)

	upsert := fmt.Sprintf(`
		INSERT INTO actors (
		  actor_id, did, handle, display_name, description,
		  avatar_cid, indexed_at,
		  follower_count, following_count, post_count,
		  likes_given_count, likes_received_count,
		  reposts_received_count, blocks_given_count
		)
		SELECT
		  r.actor_id, p.did, p.handle, p.display_name, p.description,
		  p.avatar_cid, p.event_ts,
		  0, 0, 0, 0, 0, 0, 0
		FROM read_parquet('%s') p
		JOIN actors_registry r ON r.did = p.did
		ON CONFLICT (actor_id) DO UPDATE SET
		  handle       = COALESCE(EXCLUDED.handle, actors.handle),
		  display_name = COALESCE(EXCLUDED.display_name, actors.display_name),
		  description  = COALESCE(EXCLUDED.description, actors.description),
		  avatar_cid   = COALESCE(EXCLUDED.avatar_cid, actors.avatar_cid),
		  indexed_at   = EXCLUDED.indexed_at`,
		src,
	)
	if _, err := tx.ExecContext(ctx, upsert); err != nil {
		return fmt.Errorf("upsert profiles: %w", err)
	}
	return nil
}

// applyPosts merges posts.parquet into the posts table. When filters.posts
// is configured the lang predicate is attached to the read_parquet WHERE
// so the filter runs during the parquet scan (cheap) rather than as a
// post-hoc DELETE.
func applyPosts(ctx context.Context, tx *sql.Tx, dayDir string, filters config.FilterConfig) error {
	path := parquetGlob(dayDir, "posts.parquet")
	if path == "" {
		return nil
	}
	src := sqlEscapeSingle(path)
	langClause := ""
	if pred := postsLangPredicate("p.lang", filters.Posts); pred != "" {
		langClause = " AND " + pred
	}
	// We pre-compute (did, rkey) pairs from URIs in a CTE for the soft
	// references reply_root / reply_parent / quote.
	upsert := fmt.Sprintf(`
		WITH src AS (
		  SELECT
		    p.did,
		    p.rkey,
		    p.cid,
		    p.record_ts AS created_at,
		    p.text,
		    p.lang,
		    nullif(split_part(replace(p.reply_root_uri,   'at://', ''), '/', 1), '') AS reply_root_did,
		    nullif(split_part(replace(p.reply_root_uri,   'at://', ''), '/', 3), '') AS reply_root_rkey,
		    nullif(split_part(replace(p.reply_parent_uri, 'at://', ''), '/', 1), '') AS reply_parent_did,
		    nullif(split_part(replace(p.reply_parent_uri, 'at://', ''), '/', 3), '') AS reply_parent_rkey,
		    nullif(split_part(replace(p.quote_uri,        'at://', ''), '/', 1), '') AS quote_did,
		    nullif(split_part(replace(p.quote_uri,        'at://', ''), '/', 3), '') AS quote_rkey,
		    p.embed_type
		  FROM read_parquet('%s') p
		  WHERE p.event_type IN ('create','update')%s
		)
		INSERT INTO posts (
		  author_id, rkey, cid, created_at, text, lang,
		  reply_root_author_id, reply_root_rkey,
		  reply_parent_author_id, reply_parent_rkey,
		  quote_author_id, quote_rkey,
		  embed_type, like_count, repost_count, reply_count
		)
		SELECT
		  r.actor_id, src.rkey, src.cid, src.created_at, src.text, src.lang,
		  rr.actor_id, src.reply_root_rkey,
		  rp.actor_id, src.reply_parent_rkey,
		  rq.actor_id, src.quote_rkey,
		  src.embed_type, 0, 0, 0
		  FROM src
		  JOIN actors_registry r  ON r.did  = src.did
		  LEFT JOIN actors_registry rr ON rr.did = src.reply_root_did
		  LEFT JOIN actors_registry rp ON rp.did = src.reply_parent_did
		  LEFT JOIN actors_registry rq ON rq.did = src.quote_did
		ON CONFLICT (author_id, rkey) DO UPDATE SET
		  cid                    = EXCLUDED.cid,
		  created_at             = EXCLUDED.created_at,
		  text                   = EXCLUDED.text,
		  lang                   = EXCLUDED.lang,
		  reply_root_author_id   = EXCLUDED.reply_root_author_id,
		  reply_root_rkey        = EXCLUDED.reply_root_rkey,
		  reply_parent_author_id = EXCLUDED.reply_parent_author_id,
		  reply_parent_rkey      = EXCLUDED.reply_parent_rkey,
		  quote_author_id        = EXCLUDED.quote_author_id,
		  quote_rkey             = EXCLUDED.quote_rkey,
		  embed_type             = EXCLUDED.embed_type`,
		src, langClause,
	)
	if _, err := tx.ExecContext(ctx, upsert); err != nil {
		return fmt.Errorf("upsert posts: %w", err)
	}
	if err := applyPostEmbeds(ctx, tx, dayDir); err != nil {
		return fmt.Errorf("apply post embeds: %w", err)
	}
	return nil
}

// applyPostEmbeds merges post_embeds.parquet into the post_embeds sidecar
// table (002 §3). Runs inside the same per-day transaction as applyPosts,
// joining on the `posts` table that was just written so any post the lang
// filter dropped also drops its sidecar row — no duplicate filter logic.
//
// No-op when post_embeds.parquet is absent (older archive day, or a day
// with no embeds at all).
func applyPostEmbeds(ctx context.Context, tx *sql.Tx, dayDir string) error {
	path := parquetGlob(dayDir, "post_embeds.parquet")
	if path == "" {
		return nil
	}
	src := sqlEscapeSingle(path)
	upsert := fmt.Sprintf(`
		WITH src AS (
		  SELECT
		    pe.did,
		    pe.rkey,
		    pe.kind,
		    pe.external_uri,
		    pe.external_domain,
		    pe.external_title,
		    pe.image_count,
		    pe.image_with_alt,
		    pe.video_has_alt
		  FROM read_parquet('%s') pe
		)
		INSERT INTO post_embeds (
		  author_id, rkey, kind,
		  external_uri, external_domain, external_title,
		  image_count, image_with_alt_count, video_has_alt
		)
		SELECT
		  r.actor_id, src.rkey, src.kind,
		  src.external_uri, src.external_domain, src.external_title,
		  src.image_count, src.image_with_alt, src.video_has_alt
		FROM src
		JOIN actors_registry r ON r.did = src.did
		JOIN posts p ON p.author_id = r.actor_id AND p.rkey = src.rkey
		ON CONFLICT (author_id, rkey) DO UPDATE SET
		  kind                 = EXCLUDED.kind,
		  external_uri         = EXCLUDED.external_uri,
		  external_domain      = EXCLUDED.external_domain,
		  external_title       = EXCLUDED.external_title,
		  image_count          = EXCLUDED.image_count,
		  image_with_alt_count = EXCLUDED.image_with_alt_count,
		  video_has_alt        = EXCLUDED.video_has_alt`,
		src,
	)
	if _, err := tx.ExecContext(ctx, upsert); err != nil {
		return fmt.Errorf("upsert post_embeds: %w", err)
	}
	return nil
}

// applyLikes merges likes.parquet into likes_current.
func applyLikes(ctx context.Context, tx *sql.Tx, dayDir string, _ config.FilterConfig) error {
	path := parquetGlob(dayDir, "likes.parquet")
	if path == "" {
		return nil
	}
	src := sqlEscapeSingle(path)
	upsert := fmt.Sprintf(`
		WITH src AS (
		  SELECT
		    l.liker_did,
		    split_part(replace(l.uri, 'at://', ''), '/', 3) AS rkey,
		    nullif(split_part(replace(l.subject_uri, 'at://', ''), '/', 1), '') AS subject_did,
		    nullif(split_part(replace(l.subject_uri, 'at://', ''), '/', 3), '') AS subject_rkey,
		    l.record_ts AS created_at
		  FROM read_parquet('%s') l
		  WHERE l.event_type IN ('create','update')
		)
		INSERT INTO likes_current (
		  liker_id, rkey, subject_author_id, subject_rkey, created_at
		)
		SELECT
		  r.actor_id, src.rkey, sr.actor_id, src.subject_rkey, src.created_at
		  FROM src
		  JOIN actors_registry r  ON r.did  = src.liker_did
		  JOIN actors_registry sr ON sr.did = src.subject_did
		ON CONFLICT (liker_id, rkey) DO UPDATE SET
		  subject_author_id = EXCLUDED.subject_author_id,
		  subject_rkey      = EXCLUDED.subject_rkey,
		  created_at        = EXCLUDED.created_at`,
		src,
	)
	if _, err := tx.ExecContext(ctx, upsert); err != nil {
		return fmt.Errorf("upsert likes: %w", err)
	}
	return nil
}

// applyReposts merges reposts.parquet into reposts_current.
func applyReposts(ctx context.Context, tx *sql.Tx, dayDir string, _ config.FilterConfig) error {
	path := parquetGlob(dayDir, "reposts.parquet")
	if path == "" {
		return nil
	}
	src := sqlEscapeSingle(path)
	upsert := fmt.Sprintf(`
		WITH src AS (
		  SELECT
		    r.reposter_did,
		    split_part(replace(r.uri, 'at://', ''), '/', 3) AS rkey,
		    nullif(split_part(replace(r.subject_uri, 'at://', ''), '/', 1), '') AS subject_did,
		    nullif(split_part(replace(r.subject_uri, 'at://', ''), '/', 3), '') AS subject_rkey,
		    r.record_ts AS created_at
		  FROM read_parquet('%s') r
		  WHERE r.event_type IN ('create','update')
		)
		INSERT INTO reposts_current (
		  reposter_id, rkey, subject_author_id, subject_rkey, created_at
		)
		SELECT
		  rg.actor_id, src.rkey, sr.actor_id, src.subject_rkey, src.created_at
		  FROM src
		  JOIN actors_registry rg ON rg.did = src.reposter_did
		  JOIN actors_registry sr ON sr.did = src.subject_did
		ON CONFLICT (reposter_id, rkey) DO UPDATE SET
		  subject_author_id = EXCLUDED.subject_author_id,
		  subject_rkey      = EXCLUDED.subject_rkey,
		  created_at        = EXCLUDED.created_at`,
		src,
	)
	if _, err := tx.ExecContext(ctx, upsert); err != nil {
		return fmt.Errorf("upsert reposts: %w", err)
	}
	return nil
}

// applyFollows merges follows.parquet into follows_current. follows are
// create-or-delete (never updated) so use DO NOTHING (§14.1).
func applyFollows(ctx context.Context, tx *sql.Tx, dayDir string, _ config.FilterConfig) error {
	path := parquetGlob(dayDir, "follows.parquet")
	if path == "" {
		return nil
	}
	src := sqlEscapeSingle(path)
	upsert := fmt.Sprintf(`
		WITH src AS (
		  SELECT
		    f.src_did,
		    f.dst_did,
		    split_part(replace(f.uri, 'at://', ''), '/', 3) AS rkey,
		    f.record_ts AS created_at
		  FROM read_parquet('%s') f
		  WHERE f.event_type IN ('create','update')
		)
		INSERT INTO follows_current (src_id, dst_id, rkey, created_at)
		SELECT s.actor_id, d.actor_id, src.rkey, src.created_at
		  FROM src
		  JOIN actors_registry s ON s.did = src.src_did
		  JOIN actors_registry d ON d.did = src.dst_did
		ON CONFLICT (src_id, rkey) DO NOTHING`,
		src,
	)
	if _, err := tx.ExecContext(ctx, upsert); err != nil {
		return fmt.Errorf("upsert follows: %w", err)
	}
	return nil
}

// applyBlocks merges blocks.parquet into blocks_current with DO NOTHING.
func applyBlocks(ctx context.Context, tx *sql.Tx, dayDir string, _ config.FilterConfig) error {
	path := parquetGlob(dayDir, "blocks.parquet")
	if path == "" {
		return nil
	}
	src := sqlEscapeSingle(path)
	upsert := fmt.Sprintf(`
		WITH src AS (
		  SELECT
		    b.src_did,
		    b.dst_did,
		    split_part(replace(b.uri, 'at://', ''), '/', 3) AS rkey,
		    b.record_ts AS created_at
		  FROM read_parquet('%s') b
		  WHERE b.event_type IN ('create','update')
		)
		INSERT INTO blocks_current (src_id, dst_id, rkey, created_at)
		SELECT s.actor_id, d.actor_id, src.rkey, src.created_at
		  FROM src
		  JOIN actors_registry s ON s.did = src.src_did
		  JOIN actors_registry d ON d.did = src.dst_did
		ON CONFLICT (src_id, rkey) DO NOTHING`,
		src,
	)
	if _, err := tx.ExecContext(ctx, upsert); err != nil {
		return fmt.Errorf("upsert blocks: %w", err)
	}
	return nil
}

// populateTouchedSets adds this day's actor_ids and post keys to the
// scratch tables used by the §14.2 scoped recompute. A day's "touched"
// actors are anyone who created/updated/deleted any event in the day
// plus anyone referenced as the subject of a like/repost/reply — the
// full set of rows whose aggregates could have shifted. The same goes
// for posts: authors of created/updated posts, targets of likes/reposts,
// and parents of replies.
//
// We read each parquet file directly with read_parquet(). Raw DIDs are
// joined back to actor_ids via actors_registry (populated by step 1 of
// replayOneDay).
func populateTouchedSets(ctx context.Context, tx *sql.Tx, dayDir string, excluded map[string]struct{}) error {
	skip := func(coll string) bool { _, ok := excluded[coll]; return ok }
	exists := func(name string) bool {
		p := filepath.Join(dayDir, name)
		_, err := os.Stat(p)
		return err == nil
	}
	g := func(name string) string { return parquetGlob(dayDir, name) }

	// Actors: authors of posts/profiles/reposts/likes/follows/blocks, plus
	// DST of follows (follower_count shifts), plus subject authors of
	// likes and reposts (likes_received/reposts_received shift). We just
	// collect DIDs first, then translate to actor_ids in one pass.
	var didUnions []string
	add := func(s string) { didUnions = append(didUnions, s) }

	if !skip("app.bsky.actor.profile") && exists("profile_updates.parquet") {
		add(fmt.Sprintf(`SELECT did FROM read_parquet('%s')`, g("profile_updates.parquet")))
	}
	if !skip("app.bsky.feed.post") && exists("posts.parquet") {
		// Post author.
		add(fmt.Sprintf(`SELECT did FROM read_parquet('%s')`, g("posts.parquet")))
	}
	if !skip("app.bsky.feed.like") && exists("likes.parquet") {
		// Liker + subject author (extracted from at-URI).
		add(fmt.Sprintf(`SELECT liker_did AS did FROM read_parquet('%s')`, g("likes.parquet")))
		add(fmt.Sprintf(`SELECT split_part(replace(subject_uri, 'at://', ''), '/', 1) AS did FROM read_parquet('%s') WHERE subject_uri IS NOT NULL`, g("likes.parquet")))
	}
	if !skip("app.bsky.feed.repost") && exists("reposts.parquet") {
		add(fmt.Sprintf(`SELECT reposter_did AS did FROM read_parquet('%s')`, g("reposts.parquet")))
		add(fmt.Sprintf(`SELECT split_part(replace(subject_uri, 'at://', ''), '/', 1) AS did FROM read_parquet('%s') WHERE subject_uri IS NOT NULL`, g("reposts.parquet")))
	}
	if !skip("app.bsky.graph.follow") && exists("follows.parquet") {
		add(fmt.Sprintf(`SELECT src_did AS did FROM read_parquet('%s')`, g("follows.parquet")))
		add(fmt.Sprintf(`SELECT dst_did AS did FROM read_parquet('%s')`, g("follows.parquet")))
	}
	if !skip("app.bsky.graph.block") && exists("blocks.parquet") {
		add(fmt.Sprintf(`SELECT src_did AS did FROM read_parquet('%s')`, g("blocks.parquet")))
	}
	if exists("deletions.parquet") {
		// Deletions of any collection touch the DID's counts.
		add(fmt.Sprintf(`SELECT did FROM read_parquet('%s')`, g("deletions.parquet")))
	}

	if len(didUnions) > 0 {
		q := fmt.Sprintf(`
			INSERT INTO touched_actors_scratch (actor_id)
			SELECT DISTINCT r.actor_id
			  FROM actors_registry r
			  JOIN (%s) u ON u.did = r.did
			 WHERE r.actor_id NOT IN (SELECT actor_id FROM touched_actors_scratch)
		`, strings.Join(didUnions, " UNION ALL "))
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("touched_actors insert: %w", err)
		}
	}

	// Posts: authored this day, received likes, received reposts, and
	// reply parents. All sourced from parquets via at-URI decoding.
	var postUnions []string
	addP := func(s string) { postUnions = append(postUnions, s) }
	extractAuthorRkey := func(uriCol, src string) string {
		return fmt.Sprintf(
			`SELECT (SELECT actor_id FROM actors_registry WHERE did = split_part(replace(%s, 'at://', ''), '/', 1)) AS author_id,
			        split_part(replace(%s, 'at://', ''), '/', 3) AS rkey
			   FROM read_parquet('%s')
			  WHERE %s IS NOT NULL`, uriCol, uriCol, src, uriCol)
	}

	if !skip("app.bsky.feed.post") && exists("posts.parquet") {
		addP(fmt.Sprintf(
			`SELECT (SELECT actor_id FROM actors_registry WHERE did = p.did) AS author_id, rkey
			   FROM read_parquet('%s') p`, g("posts.parquet")))
		// Reply parents — posts that gained a reply today, so their reply_count shifts.
		addP(extractAuthorRkey("reply_parent_uri", g("posts.parquet")))
	}
	if !skip("app.bsky.feed.like") && exists("likes.parquet") {
		addP(extractAuthorRkey("subject_uri", g("likes.parquet")))
	}
	if !skip("app.bsky.feed.repost") && exists("reposts.parquet") {
		addP(extractAuthorRkey("subject_uri", g("reposts.parquet")))
	}

	if len(postUnions) > 0 {
		q := fmt.Sprintf(`
			INSERT INTO touched_posts_scratch (author_id, rkey)
			SELECT DISTINCT author_id, rkey FROM (%s)
			 WHERE author_id IS NOT NULL AND rkey IS NOT NULL AND rkey <> ''
			   AND NOT EXISTS (
			     SELECT 1 FROM touched_posts_scratch t
			      WHERE t.author_id = author_id AND t.rkey = rkey
			   )
		`, strings.Join(postUnions, " UNION ALL "))
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("touched_posts insert: %w", err)
		}
	}
	return nil
}

// recomputeAllCounts runs §14.2 recompute scoped to the touched actors
// and posts accumulated across all replayed days. Falls back to a full
// recompute if the scratch tables are empty (cold path — nothing to scope
// from means we're rebuilding from scratch and scoping is incorrect).
func recomputeAllCounts(ctx context.Context, store *duckdbstore.AllStore) error {
	scope, err := hasScope(ctx, store)
	if err != nil {
		return fmt.Errorf("check scope: %w", err)
	}

	actorsScope := ""
	postsScope := ""
	if scope {
		actorsScope = ` WHERE actor_id IN (SELECT actor_id FROM touched_actors_scratch)`
		postsScope = ` WHERE (author_id, rkey) IN (SELECT author_id, rkey FROM touched_posts_scratch)`
	}

	stmts := []string{
		// actors graph counts
		`UPDATE actors SET following_count = COALESCE(c.n, 0)
		   FROM (SELECT src_id, COUNT(*) AS n FROM follows_current GROUP BY 1) c
		  WHERE actors.actor_id = c.src_id` + andScope(actorsScope, "actors.actor_id"),
		`UPDATE actors SET following_count = 0 WHERE following_count IS NULL` + andScope(actorsScope, "actor_id"),

		`UPDATE actors SET follower_count = COALESCE(c.n, 0)
		   FROM (SELECT dst_id, COUNT(*) AS n FROM follows_current GROUP BY 1) c
		  WHERE actors.actor_id = c.dst_id` + andScope(actorsScope, "actors.actor_id"),
		`UPDATE actors SET follower_count = 0 WHERE follower_count IS NULL` + andScope(actorsScope, "actor_id"),

		`UPDATE actors SET blocks_given_count = COALESCE(c.n, 0)
		   FROM (SELECT src_id, COUNT(*) AS n FROM blocks_current GROUP BY 1) c
		  WHERE actors.actor_id = c.src_id` + andScope(actorsScope, "actors.actor_id"),
		`UPDATE actors SET blocks_given_count = 0 WHERE blocks_given_count IS NULL` + andScope(actorsScope, "actor_id"),

		// actors content counts
		`UPDATE actors SET post_count = COALESCE(c.n, 0)
		   FROM (SELECT author_id, COUNT(*) AS n FROM posts GROUP BY 1) c
		  WHERE actors.actor_id = c.author_id` + andScope(actorsScope, "actors.actor_id"),
		`UPDATE actors SET post_count = 0 WHERE post_count IS NULL` + andScope(actorsScope, "actor_id"),

		`UPDATE actors SET likes_given_count = COALESCE(c.n, 0)
		   FROM (SELECT liker_id, COUNT(*) AS n FROM likes_current GROUP BY 1) c
		  WHERE actors.actor_id = c.liker_id` + andScope(actorsScope, "actors.actor_id"),
		`UPDATE actors SET likes_given_count = 0 WHERE likes_given_count IS NULL` + andScope(actorsScope, "actor_id"),

		`UPDATE actors SET likes_received_count = COALESCE(c.n, 0)
		   FROM (SELECT subject_author_id, COUNT(*) AS n FROM likes_current GROUP BY 1) c
		  WHERE actors.actor_id = c.subject_author_id` + andScope(actorsScope, "actors.actor_id"),
		`UPDATE actors SET likes_received_count = 0 WHERE likes_received_count IS NULL` + andScope(actorsScope, "actor_id"),

		`UPDATE actors SET reposts_received_count = COALESCE(c.n, 0)
		   FROM (SELECT subject_author_id, COUNT(*) AS n FROM reposts_current GROUP BY 1) c
		  WHERE actors.actor_id = c.subject_author_id` + andScope(actorsScope, "actors.actor_id"),
		`UPDATE actors SET reposts_received_count = 0 WHERE reposts_received_count IS NULL` + andScope(actorsScope, "actor_id"),

		// posts denormalized counts
		`UPDATE posts SET like_count = COALESCE(c.n, 0)
		   FROM (SELECT subject_author_id, subject_rkey, COUNT(*) AS n FROM likes_current GROUP BY 1, 2) c
		  WHERE posts.author_id = c.subject_author_id AND posts.rkey = c.subject_rkey` +
			andPostsScope(postsScope, "posts.author_id", "posts.rkey"),
		`UPDATE posts SET like_count = 0 WHERE like_count IS NULL` + andPostsScope(postsScope, "author_id", "rkey"),

		`UPDATE posts SET repost_count = COALESCE(c.n, 0)
		   FROM (SELECT subject_author_id, subject_rkey, COUNT(*) AS n FROM reposts_current GROUP BY 1, 2) c
		  WHERE posts.author_id = c.subject_author_id AND posts.rkey = c.subject_rkey` +
			andPostsScope(postsScope, "posts.author_id", "posts.rkey"),
		`UPDATE posts SET repost_count = 0 WHERE repost_count IS NULL` + andPostsScope(postsScope, "author_id", "rkey"),

		`UPDATE posts SET reply_count = COALESCE(c.n, 0)
		   FROM (
		     SELECT reply_parent_author_id AS pa, reply_parent_rkey AS pr, COUNT(*) AS n
		       FROM posts WHERE reply_parent_author_id IS NOT NULL GROUP BY 1, 2
		   ) c
		  WHERE posts.author_id = c.pa AND posts.rkey = c.pr` +
			andPostsScope(postsScope, "posts.author_id", "posts.rkey"),
		`UPDATE posts SET reply_count = 0 WHERE reply_count IS NULL` + andPostsScope(postsScope, "author_id", "rkey"),
	}
	for _, q := range stmts {
		if _, err := store.DB.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("recompute: %w (sql: %s)", err, firstLine(q))
		}
	}
	// Scratch tables served their purpose; drop to keep the file tidy.
	_, _ = store.DB.ExecContext(ctx, `DROP TABLE IF EXISTS touched_actors_scratch`)
	_, _ = store.DB.ExecContext(ctx, `DROP TABLE IF EXISTS touched_posts_scratch`)
	return nil
}

// hasScope returns true when the scratch tables exist AND have rows. A
// missing table (e.g. graph-backfill path that never called replay)
// means "full recompute".
func hasScope(ctx context.Context, store *duckdbstore.AllStore) (bool, error) {
	var n int64
	row := store.DB.QueryRowContext(ctx, `
		SELECT
		  COALESCE((SELECT COUNT(*) FROM touched_actors_scratch), 0)
		+ COALESCE((SELECT COUNT(*) FROM touched_posts_scratch),  0)
	`)
	if err := row.Scan(&n); err != nil {
		// DuckDB errors on missing table — that's our "no scope" signal.
		return false, nil
	}
	return n > 0, nil
}

// andScope appends a scoping predicate to an UPDATE. If the base statement
// already has a WHERE, "AND (…)" is injected; otherwise "WHERE …".
func andScope(scope, col string) string {
	if scope == "" {
		return ""
	}
	return fmt.Sprintf(" AND %s IN (SELECT actor_id FROM touched_actors_scratch)", col)
}

func andPostsScope(scope, authorCol, rkeyCol string) string {
	if scope == "" {
		return ""
	}
	return fmt.Sprintf(" AND (%s, %s) IN (SELECT author_id, rkey FROM touched_posts_scratch)", authorCol, rkeyCol)
}

func firstLine(s string) string {
	if i := strings.Index(s, "\n"); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

// copyGraphSubset emits a fresh current_graph.duckdb containing actors +
// follows_current + blocks_current + _meta, reading from the just-built
// current_all.duckdb. We rebuild the graph file from scratch each run.
func copyGraphSubset(ctx context.Context, allPath, graphPath string, jetstreamCursor int64) error {
	for _, p := range []string{graphPath, graphPath + ".wal"} {
		if err := removeIfExists(p); err != nil {
			return err
		}
	}
	// Open a third DuckDB instance, ATTACH both files, and copy the
	// graph-only tables across.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`ATTACH '%s' AS src (READ_ONLY)`, sqlEscapeSingle(allPath))); err != nil {
		return fmt.Errorf("attach src: %w", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`ATTACH '%s' AS dst`, sqlEscapeSingle(graphPath))); err != nil {
		return fmt.Errorf("attach dst: %w", err)
	}
	// Apply the graph-only schema in dst.
	for _, q := range strings.Split(graphSchemaForCopy, ";") {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, "USE dst; "+q); err != nil {
			return fmt.Errorf("dst schema %q: %w", firstLine(q), err)
		}
	}
	// Now copy.
	for _, q := range []string{
		`INSERT INTO dst.actors          SELECT * FROM src.actors`,
		`INSERT INTO dst.follows_current SELECT * FROM src.follows_current`,
		`INSERT INTO dst.blocks_current  SELECT * FROM src.blocks_current`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("copy: %s: %w", firstLine(q), err)
		}
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO dst._meta (schema_version, built_at, build_mode, did_limit, source_did_count)
		 VALUES ('v1', now(), 'incremental', ?, ?)`, jetstreamCursor, 0); err != nil {
		return fmt.Errorf("dst _meta: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CHECKPOINT dst`); err != nil {
		return fmt.Errorf("checkpoint dst: %w", err)
	}
	return nil
}

// graphSchemaForCopy mirrors store.go's schemaSQL but lives separately so
// we can apply it to an attached database (USE dst). DuckDB's ATTACH does
// not auto-apply CREATE-IF-NOT-EXISTS guards from another schema string.
const graphSchemaForCopy = `
CREATE TABLE IF NOT EXISTS actors (
  actor_id   BIGINT PRIMARY KEY,
  did        VARCHAR NOT NULL UNIQUE,
  handle     VARCHAR,
  display_name VARCHAR,
  description  VARCHAR,
  avatar_cid   VARCHAR,
  created_at   TIMESTAMP,
  indexed_at   TIMESTAMP,
  follower_count       BIGINT,
  following_count      BIGINT,
  post_count           BIGINT,
  likes_given_count    BIGINT,
  likes_received_count BIGINT,
  reposts_received_count BIGINT,
  blocks_given_count   BIGINT
);
CREATE TABLE IF NOT EXISTS follows_current (
  src_id     BIGINT NOT NULL,
  dst_id     BIGINT NOT NULL,
  rkey       VARCHAR NOT NULL,
  created_at TIMESTAMP,
  PRIMARY KEY (src_id, rkey)
);
CREATE TABLE IF NOT EXISTS blocks_current (
  src_id     BIGINT NOT NULL,
  dst_id     BIGINT NOT NULL,
  rkey       VARCHAR NOT NULL,
  created_at TIMESTAMP,
  PRIMARY KEY (src_id, rkey)
);
CREATE TABLE IF NOT EXISTS _meta (
  schema_version   VARCHAR,
  built_at         TIMESTAMP,
  build_mode       VARCHAR,
  did_limit        BIGINT,
  source_did_count BIGINT
);
`

// marshalFilters serializes config.FilterConfig to the JSON shape used
// in latest.json filter_config + _meta.filter_config.
func marshalFilters(f config.FilterConfig) (string, error) {
	type postFilter struct {
		Langs         []string `json:"langs,omitempty"`
		ExcludeNoLang bool     `json:"exclude_no_lang"`
	}
	out := struct {
		Posts              postFilter `json:"posts"`
		ExcludeCollections []string   `json:"exclude_collections"`
	}{
		Posts: postFilter{
			Langs:         f.Posts.Langs,
			ExcludeNoLang: f.Posts.ExcludeNoLang,
		},
		ExcludeCollections: f.ExcludeCollections,
	}
	if out.ExcludeCollections == nil {
		out.ExcludeCollections = []string{}
	}
	b, err := json.Marshal(out)
	return string(b), err
}

