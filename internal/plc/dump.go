// Package plc enumerates DIDs from the PLC directory's /export endpoint
// and replays operations to derive the current PDS endpoint per DID.
//
// The export endpoint returns JSONL of operations. A DID may appear in
// many operations (key rotation, PDS migration, handle change,
// tombstone). The caller is responsible for replaying ops in order and
// retaining only the last non-nullified op per DID — see Op.Tombstone.
package plc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// UserAgent is sent on every outbound request per spec §10.
const UserAgent = "atproto-db-snapshot/0.1 (+https://github.com/pboueri/atproto-db-snapshot)"

// Client paginates against plc.directory/export.
type Client struct {
	BaseURL   string
	HTTP      *http.Client
	Limiter   *rate.Limiter
	UserAgent string
	PageSize  int
}

// New returns a Client paced at rps requests per second against base.
func New(base string, rps float64, timeout time.Duration) *Client {
	if rps <= 0 {
		rps = 5
	}
	burst := int(rps) + 1
	if burst < 1 {
		burst = 1
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		BaseURL:   base,
		HTTP:      &http.Client{Timeout: timeout},
		Limiter:   rate.NewLimiter(rate.Limit(rps), burst),
		UserAgent: UserAgent,
		PageSize:  1000,
	}
}

// Op is a normalized PLC operation row. Endpoint is "" for tombstones or
// ops whose services.atproto_pds.endpoint is missing.
type Op struct {
	DID        string
	Endpoint   string
	Tombstone  bool
	CreatedAt  time.Time
	RawCursor  string
}

// rawOp is the wire shape of a single line from /export.
type rawOp struct {
	DID       string          `json:"did"`
	Operation rawOperation    `json:"operation"`
	CID       string          `json:"cid"`
	Nullified bool            `json:"nullified"`
	CreatedAt string          `json:"createdAt"`
}

type rawOperation struct {
	Type     string                 `json:"type"`
	Services map[string]rawService  `json:"services"`
	// fall-through fields ignored
}

type rawService struct {
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

// Stream paginates /export starting at the given cursor (empty = beginning),
// invoking cb for every non-nullified op. Returns the cursor string of the
// last successfully consumed page, suitable for persistence and resume.
func (c *Client) Stream(ctx context.Context, cursor string, cb func(Op) error) (string, error) {
	if c.PageSize <= 0 {
		c.PageSize = 1000
	}
	lastCursor := cursor
	for {
		if err := c.Limiter.Wait(ctx); err != nil {
			return lastCursor, err
		}
		u, err := c.buildURL(cursor)
		if err != nil {
			return lastCursor, err
		}
		req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
		if err != nil {
			return lastCursor, err
		}
		req.Header.Set("User-Agent", c.UserAgent)
		req.Header.Set("Accept", "application/jsonl, text/plain")
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return lastCursor, err
		}
		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			return lastCursor, fmt.Errorf("plc /export %d: %s", resp.StatusCode, string(body))
		}

		// Parse JSONL.
		scanner := bufio.NewScanner(resp.Body)
		// Allow long lines (operations can be large with multiple services).
		scanner.Buffer(make([]byte, 1<<16), 1<<24)
		var (
			rowCount     int
			pageMaxStamp string
		)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			var ro rawOp
			if err := json.Unmarshal(line, &ro); err != nil {
				resp.Body.Close()
				return lastCursor, fmt.Errorf("plc decode line: %w", err)
			}
			rowCount++
			if ro.CreatedAt > pageMaxStamp {
				pageMaxStamp = ro.CreatedAt
			}
			if ro.Nullified {
				continue
			}
			op := Op{
				DID:       ro.DID,
				CreatedAt: parseISO(ro.CreatedAt),
				RawCursor: ro.CreatedAt,
			}
			switch ro.Operation.Type {
			case "plc_tombstone":
				op.Tombstone = true
			default:
				if svc, ok := ro.Operation.Services["atproto_pds"]; ok {
					op.Endpoint = svc.Endpoint
				}
			}
			if err := cb(op); err != nil {
				resp.Body.Close()
				return lastCursor, err
			}
		}
		if scerr := scanner.Err(); scerr != nil {
			resp.Body.Close()
			return lastCursor, fmt.Errorf("plc scan: %w", scerr)
		}
		resp.Body.Close()

		// Empty page → enumeration complete.
		if rowCount == 0 {
			return lastCursor, nil
		}
		// Advance cursor to the largest createdAt seen on this page.
		// PLC's /export uses the createdAt of the last item as the
		// `after` cursor. If nothing advanced, stop to avoid spinning.
		if pageMaxStamp == "" || pageMaxStamp == cursor {
			return lastCursor, nil
		}
		cursor = pageMaxStamp
		lastCursor = cursor
	}
}

func (c *Client) buildURL(cursor string) (string, error) {
	base, err := url.Parse(c.BaseURL)
	if err != nil {
		return "", fmt.Errorf("plc base url: %w", err)
	}
	base.Path = "/export"
	q := base.Query()
	q.Set("count", strconv.Itoa(c.PageSize))
	if cursor != "" {
		q.Set("after", cursor)
	}
	base.RawQuery = q.Encode()
	return base.String(), nil
}

func parseISO(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, l := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

// ----- cursor persistence (atomic rename, mirrors internal/run) -----

// CursorState is the on-disk shape persisted between runs.
type CursorState struct {
	Cursor        string    `json:"cursor"`
	UpdatedAt     time.Time `json:"updated_at"`
	SchemaVersion string    `json:"schema_version"`
}

// LoadCursor reads the cursor file. A missing file returns the zero state.
func LoadCursor(path string) (CursorState, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return CursorState{SchemaVersion: "v1"}, nil
		}
		return CursorState{}, err
	}
	var c CursorState
	if err := json.Unmarshal(b, &c); err != nil {
		return CursorState{}, fmt.Errorf("plc cursor: %w", err)
	}
	if c.SchemaVersion == "" {
		c.SchemaVersion = "v1"
	}
	return c, nil
}

// SaveCursorAtomic writes the cursor file via tempfile + os.Rename.
func SaveCursorAtomic(path string, c CursorState) error {
	c.SchemaVersion = "v1"
	c.UpdatedAt = time.Now().UTC()
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if f, err := os.OpenFile(tmp, os.O_RDWR, 0o644); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
