package archive

import (
	"context"
	"fmt"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// WriteProfileUpdates emits profile_updates.parquet (§6):
//
//	date, did, handle, display_name, description, avatar_cid, event_ts
//
// Sort within row groups: by `did` (§6).
func (w *Writer) WriteProfileUpdates(ctx context.Context, day Day, rows []ProfileUpdateEvent) (int64, int64, error) {
	const stage = "stage_profile_updates"
	createSQL := `CREATE TEMP TABLE ` + stage + ` (
  did          VARCHAR,
  handle       VARCHAR,
  display_name VARCHAR,
  description  VARCHAR,
  avatar_cid   VARCHAR,
  event_ts     TIMESTAMP
)`
	selectSQL := fmt.Sprintf(`SELECT
    %s AS date,
    did, handle, display_name, description, avatar_cid, event_ts
  FROM %s
  ORDER BY did`, dateLit(day), stage)

	return w.stageAndCopy(ctx, stage, createSQL, selectSQL, w.outPath("profile_updates.parquet"),
		func(app *duckdb.Appender) (int64, error) {
			var n int64
			for i := range rows {
				r := &rows[i]
				if err := app.AppendRow(
					nilIfEmpty(r.DID),
					nilIfEmpty(r.Handle),
					nilIfEmpty(r.DisplayName),
					nilIfEmpty(r.Description),
					nilIfEmpty(r.AvatarCID),
					tsOrNil(r.EventTS),
				); err != nil {
					return n, err
				}
				n++
			}
			return n, nil
		})
}

// WriteDeletions emits deletions.parquet (§6):
//
//	date, collection, uri, did, event_ts
//
// Sort within row groups: by `did` (§6).
func (w *Writer) WriteDeletions(ctx context.Context, day Day, rows []DeleteEvent) (int64, int64, error) {
	const stage = "stage_deletions"
	createSQL := `CREATE TEMP TABLE ` + stage + ` (
  collection VARCHAR,
  uri        VARCHAR,
  did        VARCHAR,
  event_ts   TIMESTAMP
)`
	selectSQL := fmt.Sprintf(`SELECT
    %s AS date,
    collection, uri, did, event_ts
  FROM %s
  ORDER BY did`, dateLit(day), stage)

	return w.stageAndCopy(ctx, stage, createSQL, selectSQL, w.outPath("deletions.parquet"),
		func(app *duckdb.Appender) (int64, error) {
			var n int64
			for i := range rows {
				r := &rows[i]
				if err := app.AppendRow(
					nilIfEmpty(r.Collection),
					nilIfEmpty(r.URI),
					nilIfEmpty(r.DID),
					tsOrNil(r.EventTS),
				); err != nil {
					return n, err
				}
				n++
			}
			return n, nil
		})
}
