package build

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"

	// modernc.org/sqlite is pure Go; cgo-free coexistence with go-duckdb.
	_ "modernc.org/sqlite"
)

// labelerTakedownVals lists the label values that we auto-apply as
// takedowns. Bluesky's moderation team emits `!takedown` for the hardest
// action (account or content fully hidden) and `!hide` as a softer
// content-hide. For the snapshot we treat them identically — both mean
// "content should not be shown."
var labelerTakedownVals = []string{"!takedown", "!hide"}

// LoadLabelerTakedowns reads labels.db (written by `at-snapshotter labels`)
// and returns a Takedowns list for every URI that currently has an
// unnegated `!takedown` or `!hide` label. A missing file yields an empty
// Takedowns with no error so callers can treat it as optional.
//
// Semantics:
//
//   - val IN ('!takedown', '!hide')
//   - neg = 0  — drop retractions from consideration
//   - For a given (src, uri, val), if a later row with neg=1 exists we
//     drop the positive label too. (We filter by the MAX(seq) row per
//     key; if that row has neg=1 the URI is currently unlabeled.)
//
// Retractions do NOT un-apply a previously applied takedown — the spec
// explicitly notes that restoring nullified rows requires a rebuild from
// parquet, not a negate-aware reverse operation. This function only
// produces the current *positive* takedown set.
func LoadLabelerTakedowns(labelsDBPath string) (Takedowns, error) {
	if labelsDBPath == "" {
		return Takedowns{}, nil
	}
	if _, err := os.Stat(labelsDBPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Takedowns{}, nil
		}
		return Takedowns{}, fmt.Errorf("stat %s: %w", labelsDBPath, err)
	}

	// READ_ONLY DSN with WAL pragmas matching the writer's so the
	// subscriber holding the DB open cannot be blocked by a build.
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", labelsDBPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return Takedowns{}, fmt.Errorf("open %s: %w", labelsDBPath, err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	// Per (src, uri, val) pick the row with the largest seq. If that row
	// has neg=1 we skip; otherwise we emit a takedown entry. This
	// collapses supersede/retract sequences correctly — the latest-seen
	// state per subject is what matters.
	const query = `
SELECT uri, val, cts
  FROM labels l1
 WHERE val IN (?, ?)
   AND neg = 0
   AND NOT EXISTS (
     SELECT 1 FROM labels l2
      WHERE l2.src = l1.src
        AND l2.uri = l1.uri
        AND l2.val = l1.val
        AND l2.seq >  l1.seq
        AND l2.neg =  1
   )
`
	args := make([]any, 0, len(labelerTakedownVals))
	for _, v := range labelerTakedownVals {
		args = append(args, v)
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return Takedowns{}, fmt.Errorf("query labels: %w", err)
	}
	defer rows.Close()

	out := Takedowns{}
	seen := make(map[string]struct{})
	for rows.Next() {
		var uri, val, cts string
		if err := rows.Scan(&uri, &val, &cts); err != nil {
			return Takedowns{}, fmt.Errorf("scan: %w", err)
		}
		// The same URI might be labeled with both !takedown and !hide.
		// Dedupe by URI; the first wins for Reason purposes. Idempotence
		// still protects the apply step regardless.
		if _, ok := seen[uri]; ok {
			continue
		}
		seen[uri] = struct{}{}
		out.URIs = append(out.URIs, TakedownEntry{
			URI:    uri,
			Reason: fmt.Sprintf("bsky-labeler:%s@%s", val, cts),
			Date:   cts,
		})
	}
	if err := rows.Err(); err != nil {
		return Takedowns{}, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}

// mergeTakedowns concatenates two Takedowns lists, preserving order and
// skipping duplicate URIs (the first occurrence wins for Reason/Date).
func mergeTakedowns(a, b Takedowns) Takedowns {
	out := Takedowns{URIs: make([]TakedownEntry, 0, len(a.URIs)+len(b.URIs))}
	seen := make(map[string]struct{}, len(a.URIs)+len(b.URIs))
	for _, src := range [][]TakedownEntry{a.URIs, b.URIs} {
		for _, e := range src {
			if _, ok := seen[e.URI]; ok {
				continue
			}
			seen[e.URI] = struct{}{}
			out.URIs = append(out.URIs, e)
		}
	}
	return out
}
