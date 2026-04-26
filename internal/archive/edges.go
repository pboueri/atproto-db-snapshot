package archive

import (
	"context"
	"fmt"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// WriteLikes emits likes.parquet (§6):
//
//	date, uri, liker_did, subject_uri, subject_cid, event_ts, record_ts, event_type
//
// Sort within row groups: by `liker_did` (§6).
func (w *Writer) WriteLikes(ctx context.Context, day Day, rows []LikeEvent) (int64, int64, error) {
	const stage = "stage_likes"
	createSQL := `CREATE TEMP TABLE ` + stage + ` (
  uri          VARCHAR,
  liker_did    VARCHAR,
  subject_uri  VARCHAR,
  subject_cid  VARCHAR,
  event_ts     TIMESTAMP,
  record_ts    TIMESTAMP,
  event_type   VARCHAR
)`
	selectSQL := fmt.Sprintf(`SELECT
    %s AS date,
    uri, liker_did, subject_uri, subject_cid, event_ts, record_ts, event_type
  FROM %s
  ORDER BY liker_did`, dateLit(day), stage)

	return w.stageAndCopy(ctx, stage, createSQL, selectSQL, w.outPath("likes.parquet"),
		func(app *duckdb.Appender) (int64, error) {
			var n int64
			for i := range rows {
				r := &rows[i]
				if err := app.AppendRow(
					nilIfEmpty(r.URI),
					nilIfEmpty(r.LikerDID),
					nilIfEmpty(r.SubjectURI),
					nilIfEmpty(r.SubjectCID),
					tsOrNil(r.EventTS),
					tsOrNil(r.RecordTS),
					nilIfEmpty(r.EventType),
				); err != nil {
					return n, err
				}
				n++
			}
			return n, nil
		})
}

// WriteReposts emits reposts.parquet (§6):
//
//	date, uri, reposter_did, subject_uri, subject_cid, event_ts, record_ts, event_type
//
// Sort within row groups: by `reposter_did` (§6).
func (w *Writer) WriteReposts(ctx context.Context, day Day, rows []RepostEvent) (int64, int64, error) {
	const stage = "stage_reposts"
	createSQL := `CREATE TEMP TABLE ` + stage + ` (
  uri          VARCHAR,
  reposter_did VARCHAR,
  subject_uri  VARCHAR,
  subject_cid  VARCHAR,
  event_ts     TIMESTAMP,
  record_ts    TIMESTAMP,
  event_type   VARCHAR
)`
	selectSQL := fmt.Sprintf(`SELECT
    %s AS date,
    uri, reposter_did, subject_uri, subject_cid, event_ts, record_ts, event_type
  FROM %s
  ORDER BY reposter_did`, dateLit(day), stage)

	return w.stageAndCopy(ctx, stage, createSQL, selectSQL, w.outPath("reposts.parquet"),
		func(app *duckdb.Appender) (int64, error) {
			var n int64
			for i := range rows {
				r := &rows[i]
				if err := app.AppendRow(
					nilIfEmpty(r.URI),
					nilIfEmpty(r.ReposterDID),
					nilIfEmpty(r.SubjectURI),
					nilIfEmpty(r.SubjectCID),
					tsOrNil(r.EventTS),
					tsOrNil(r.RecordTS),
					nilIfEmpty(r.EventType),
				); err != nil {
					return n, err
				}
				n++
			}
			return n, nil
		})
}

// WriteFollows emits follows.parquet (§6):
//
//	date, uri, src_did, dst_did, event_ts, record_ts, event_type
//
// Sort within row groups: by `src_did` (§6).
func (w *Writer) WriteFollows(ctx context.Context, day Day, rows []FollowEvent) (int64, int64, error) {
	return w.writeBinaryEdge(ctx, day, "stage_follows", "follows.parquet", rows)
}

// WriteBlocks emits blocks.parquet (§6):
//
//	date, uri, src_did, dst_did, event_ts, record_ts, event_type
//
// Sort within row groups: by `src_did` (§6).
func (w *Writer) WriteBlocks(ctx context.Context, day Day, rows []BlockEvent) (int64, int64, error) {
	// FollowEvent and BlockEvent share the same shape; convert in place.
	conv := make([]FollowEvent, len(rows))
	for i, r := range rows {
		conv[i] = FollowEvent{
			EventType: r.EventType,
			URI:       r.URI,
			SrcDID:    r.SrcDID,
			DstDID:    r.DstDID,
			EventTS:   r.EventTS,
			RecordTS:  r.RecordTS,
		}
	}
	return w.writeBinaryEdge(ctx, day, "stage_blocks", "blocks.parquet", conv)
}

// writeBinaryEdge handles the shared shape of follows + blocks.
func (w *Writer) writeBinaryEdge(
	ctx context.Context, day Day, stage, parquet string, rows []FollowEvent,
) (int64, int64, error) {
	createSQL := `CREATE TEMP TABLE ` + stage + ` (
  uri        VARCHAR,
  src_did    VARCHAR,
  dst_did    VARCHAR,
  event_ts   TIMESTAMP,
  record_ts  TIMESTAMP,
  event_type VARCHAR
)`
	selectSQL := fmt.Sprintf(`SELECT
    %s AS date,
    uri, src_did, dst_did, event_ts, record_ts, event_type
  FROM %s
  ORDER BY src_did`, dateLit(day), stage)

	return w.stageAndCopy(ctx, stage, createSQL, selectSQL, w.outPath(parquet),
		func(app *duckdb.Appender) (int64, error) {
			var n int64
			for i := range rows {
				r := &rows[i]
				if err := app.AppendRow(
					nilIfEmpty(r.URI),
					nilIfEmpty(r.SrcDID),
					nilIfEmpty(r.DstDID),
					tsOrNil(r.EventTS),
					tsOrNil(r.RecordTS),
					nilIfEmpty(r.EventType),
				); err != nil {
					return n, err
				}
				n++
			}
			return n, nil
		})
}
