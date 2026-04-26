package archive

import (
	"context"
	"fmt"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// WritePosts emits posts.parquet for the day. Schema (§6):
//
//	date, event_type, uri, did, rkey, cid, event_ts, record_ts,
//	text, lang, reply_root_uri, reply_parent_uri, quote_uri, embed_type
//
// Sort within row groups: by `did` (§6).
func (w *Writer) WritePosts(ctx context.Context, day Day, rows []PostEvent) (int64, int64, error) {
	const stage = "stage_posts"
	createSQL := `CREATE TEMP TABLE ` + stage + ` (
  event_type       VARCHAR,
  uri              VARCHAR,
  did              VARCHAR,
  rkey             VARCHAR,
  cid              VARCHAR,
  event_ts         TIMESTAMP,
  record_ts        TIMESTAMP,
  text             VARCHAR,
  lang             VARCHAR,
  reply_root_uri   VARCHAR,
  reply_parent_uri VARCHAR,
  quote_uri        VARCHAR,
  embed_type       VARCHAR
)`
	selectSQL := fmt.Sprintf(`SELECT
    %s AS date,
    event_type, uri, did, rkey, cid, event_ts, record_ts,
    text, lang, reply_root_uri, reply_parent_uri, quote_uri, embed_type
  FROM %s
  ORDER BY did`, dateLit(day), stage)

	return w.stageAndCopy(ctx, stage, createSQL, selectSQL, w.outPath("posts.parquet"),
		func(app *duckdb.Appender) (int64, error) {
			var n int64
			for i := range rows {
				r := &rows[i]
				if err := app.AppendRow(
					nilIfEmpty(r.EventType),
					nilIfEmpty(r.URI),
					nilIfEmpty(r.DID),
					nilIfEmpty(r.Rkey),
					nilIfEmpty(r.CID),
					tsOrNil(r.EventTS),
					tsOrNil(r.RecordTS),
					nilIfEmpty(r.Text),
					nilIfEmpty(r.Lang),
					nilIfEmpty(r.ReplyRootURI),
					nilIfEmpty(r.ReplyParentURI),
					nilIfEmpty(r.QuoteURI),
					nilIfEmpty(r.EmbedType),
				); err != nil {
					return n, err
				}
				n++
			}
			return n, nil
		})
}

// tsOrNil returns nil for a zero time so DuckDB persists NULL.
func tsOrNil(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
