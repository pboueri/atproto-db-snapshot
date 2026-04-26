package archive

import (
	"context"
	"fmt"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// WritePostEmbeds emits post_embeds.parquet for the day. Schema (002 §1.2):
//
//	date, uri, did, rkey, event_ts, kind,
//	external_uri, external_domain, external_title,
//	image_count, image_with_alt, video_has_alt
//
// Empty input → no file (consistent with the other Write* helpers).
// Sort within row groups: by `did`.
func (w *Writer) WritePostEmbeds(ctx context.Context, day Day, rows []PostEmbedEvent) (int64, int64, error) {
	const stage = "stage_post_embeds"
	createSQL := `CREATE TEMP TABLE ` + stage + ` (
  uri              VARCHAR,
  did              VARCHAR,
  rkey             VARCHAR,
  event_ts         TIMESTAMP,
  kind             VARCHAR,
  external_uri     VARCHAR,
  external_domain  VARCHAR,
  external_title   VARCHAR,
  image_count      SMALLINT,
  image_with_alt   SMALLINT,
  video_has_alt    BOOLEAN
)`
	selectSQL := fmt.Sprintf(`SELECT
    %s AS date,
    uri, did, rkey, event_ts, kind,
    external_uri, external_domain, external_title,
    image_count, image_with_alt, video_has_alt
  FROM %s
  ORDER BY did, rkey`, dateLit(day), stage)

	return w.stageAndCopy(ctx, stage, createSQL, selectSQL, w.outPath("post_embeds.parquet"),
		func(app *duckdb.Appender) (int64, error) {
			var n int64
			for i := range rows {
				r := &rows[i]
				if err := app.AppendRow(
					nilIfEmpty(r.URI),
					nilIfEmpty(r.DID),
					nilIfEmpty(r.Rkey),
					tsOrNil(r.EventTS),
					nilIfEmpty(r.Kind),
					nilIfEmpty(r.ExternalURI),
					nilIfEmpty(r.ExternalDomain),
					nilIfEmpty(r.ExternalTitle),
					int16OrNil(r.ImageCount, r.Kind, "images"),
					int16OrNil(r.ImageWithAltCount, r.Kind, "images"),
					boolOrNil(r.VideoHasAlt, r.HasVideo),
				); err != nil {
					return n, err
				}
				n++
			}
			return n, nil
		})
}

// int16OrNil returns nil when the row's kind doesn't carry images at all,
// so empty (no images) and absent (not an image embed) are distinguishable
// in the output. Counts are clamped to int16 — the lexicon caps images per
// post at 4, so any overflow indicates a parser bug, not real data.
func int16OrNil(n int, kind, want string) any {
	switch kind {
	case "images", "recordWithMedia:images":
	default:
		return nil
	}
	_ = want
	if n < 0 {
		return int16(0)
	}
	if n > 32767 {
		return int16(32767)
	}
	return int16(n)
}

// boolOrNil returns nil when the row isn't a video embed at all, so the
// downstream BOOLEAN column carries the three states (NULL / false / true)
// the analyst needs to tell "no video" apart from "video without alt".
func boolOrNil(hasAlt, hasVideo bool) any {
	if !hasVideo {
		return nil
	}
	return hasAlt
}
