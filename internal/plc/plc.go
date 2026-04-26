// Package plc fetches the canonical DID list from a PLC directory.
//
// This snapshotter only needs the DID column from the export — handle history
// and key rotation aren't part of the schema — so the interface yields one
// (did, createdAt) pair at a time and lets implementations decide whether to
// page through HTTP, replay a local file, or invent fixtures.
package plc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Entry is one row from the export — only the bits the snapshotter needs.
type Entry struct {
	DID       string
	CreatedAt time.Time
}

// Directory is the abstract source of DIDs.
//
// Stream calls yield once per DID in chronological (createdAt) order. since
// is exclusive — pass the zero time to start from the beginning. When yield
// returns false the implementation should stop.
type Directory interface {
	Stream(ctx context.Context, since time.Time, yield func(Entry) bool) error
}

// HTTPDirectory talks to the canonical https://plc.directory/export.
//
// The export is paginated by createdAt cursor. Each page is NDJSON. We track
// the last seen createdAt to advance the cursor; on a page that returns
// nothing new we treat the export as caught up.
type HTTPDirectory struct {
	BaseURL string
	Client  *http.Client
	// PageSize is the per-request count; PLC caps at 1000.
	PageSize int
}

// NewHTTP returns a production HTTPDirectory pointed at base.
func NewHTTP(base string) *HTTPDirectory {
	return &HTTPDirectory{
		BaseURL:  base,
		Client:   &http.Client{Timeout: 60 * time.Second},
		PageSize: 1000,
	}
}

// rawOp is the subset of fields we read from the PLC export. The export
// contains many other fields (operation, sig, prev, etc.) that we don't need.
type rawOp struct {
	DID       string    `json:"did"`
	CreatedAt time.Time `json:"createdAt"`
}

func (h *HTTPDirectory) Stream(ctx context.Context, since time.Time, yield func(Entry) bool) error {
	if h.PageSize == 0 {
		h.PageSize = 1000
	}
	cursor := since
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		u, err := url.Parse(h.BaseURL)
		if err != nil {
			return fmt.Errorf("plc: parse base url: %w", err)
		}
		u.Path = "/export"
		q := u.Query()
		q.Set("count", strconv.Itoa(h.PageSize))
		if !cursor.IsZero() {
			q.Set("after", cursor.UTC().Format(time.RFC3339Nano))
		}
		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/jsonlines")

		resp, err := h.Client.Do(req)
		if err != nil {
			return fmt.Errorf("plc: GET %s: %w", u, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return fmt.Errorf("plc: GET %s: status %d", u, resp.StatusCode)
		}

		count, lastSeen, stop, err := scanPage(resp.Body, cursor, yield)
		resp.Body.Close()
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
		// PLC returns the cursor row inclusive of `after`; if we got no new
		// rows past the cursor, we've caught up.
		if count == 0 || !lastSeen.After(cursor) {
			return nil
		}
		cursor = lastSeen
	}
}

// scanPage decodes NDJSON from r, calls yield for each row whose createdAt
// strictly exceeds cursor, and reports the count seen, the latest createdAt,
// and whether yield asked us to stop.
func scanPage(r io.Reader, cursor time.Time, yield func(Entry) bool) (int, time.Time, bool, error) {
	br := bufio.NewScanner(r)
	br.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var (
		count    int
		lastSeen = cursor
	)
	for br.Scan() {
		line := br.Bytes()
		if len(line) == 0 {
			continue
		}
		var op rawOp
		if err := json.Unmarshal(line, &op); err != nil {
			return count, lastSeen, false, fmt.Errorf("plc: decode line: %w", err)
		}
		if op.DID == "" {
			continue
		}
		if !op.CreatedAt.After(cursor) {
			// Already-seen rows; PLC includes the cursor row inclusive.
			lastSeen = op.CreatedAt
			continue
		}
		if !yield(Entry{DID: op.DID, CreatedAt: op.CreatedAt}) {
			return count, lastSeen, true, nil
		}
		count++
		lastSeen = op.CreatedAt
	}
	return count, lastSeen, false, br.Err()
}
