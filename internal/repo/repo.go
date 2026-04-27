// Package repo lists ATProto records from a given DID's home PDS.
//
// The bootstrap pipeline calls this once per DID (with the PDS URL extracted
// from the PLC export) for the three collections we care about:
// app.bsky.actor.profile, app.bsky.graph.follow, app.bsky.graph.block.
//
// Earlier drafts of this code talked to https://constellation.microcosm.blue,
// but Constellation is a backlinks index — it tells you who links to a
// record, not what records a DID owns — so the bootstrap pipeline now
// dispatches XRPC listRecords directly to each repo's PDS instead. The PDS
// URL travels alongside the DID in plc.Entry.
//
// The big shared PDSes (notably bsky.social) rate-limit aggressively, so
// HTTPClient honors 429 Retry-After and applies an exponential backoff on
// 5xx — see do() for the policy.
package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/model"
)

// Record is one ATProto record returned by com.atproto.repo.listRecords.
//
// Value is the raw record payload; bootstrap fetches typed model values
// out of it via internal/atrecord.
type Record struct {
	URI        string             `json:"uri"`
	CID        string             `json:"cid"`
	DID        string             `json:"-"`
	Collection model.Collection   `json:"-"`
	RKey       string             `json:"-"`
	Value      json.RawMessage    `json:"value"`
}

// Client lists records by (PDS, DID, collection).
type Client interface {
	// ListRecords returns every record in collection for did.
	//
	// pds is the absolute URL of the DID's home Personal Data Server (e.g.
	// https://bsky.social or https://shimeji.us-east.host.bsky.network); it
	// is required because there is no global listRecords endpoint.
	ListRecords(ctx context.Context, pds, did string, collection model.Collection) ([]Record, error)
}

// HTTPClient hits standard com.atproto.repo.listRecords on the supplied PDS.
type HTTPClient struct {
	HTTP *http.Client
	// PageSize is the per-request limit; the XRPC server-side cap is 100.
	PageSize int
	// MaxRetries caps retry attempts on 429 / 5xx. Default 5.
	MaxRetries int
	// MinBackoff and MaxBackoff bracket the exponential schedule. Retry-After
	// from the server overrides the schedule when present.
	MinBackoff, MaxBackoff time.Duration
}

// NewHTTP returns a production HTTPClient with a sensible 30s timeout and
// retry policy tuned for shared-host rate limits (bsky.social).
func NewHTTP() *HTTPClient {
	return &HTTPClient{
		HTTP:       &http.Client{Timeout: 30 * time.Second},
		PageSize:   100,
		MaxRetries: 5,
		MinBackoff: 1 * time.Second,
		MaxBackoff: 30 * time.Second,
	}
}

// listRecordsResp matches the XRPC response shape.
type listRecordsResp struct {
	Cursor  string   `json:"cursor"`
	Records []Record `json:"records"`
}

func (c *HTTPClient) ListRecords(ctx context.Context, pds, did string, collection model.Collection) ([]Record, error) {
	if pds == "" {
		// A DID without a PDS in PLC (tombstone, malformed op log) has no
		// records to fetch — surface an empty slice rather than constructing
		// a malformed URL the call would 4xx on anyway.
		return nil, nil
	}
	if c.PageSize == 0 {
		c.PageSize = 100
	}
	var all []Record
	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		u, err := url.Parse(pds)
		if err != nil {
			return nil, fmt.Errorf("repo: parse pds %q: %w", pds, err)
		}
		u.Path = "/xrpc/com.atproto.repo.listRecords"
		q := u.Query()
		q.Set("repo", did)
		q.Set("collection", string(collection))
		q.Set("limit", strconv.Itoa(c.PageSize))
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		u.RawQuery = q.Encode()

		resp, err := c.do(ctx, u)
		if err != nil {
			return nil, err
		}
		body := resp.Body
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
			body.Close()
			// A repo that doesn't exist (deactivated, mistyped) returns 4xx —
			// treat as empty rather than a fatal error so a bootstrap pass
			// over a stale DID list keeps moving.
			return all, nil
		}
		if resp.StatusCode == http.StatusBadRequest {
			body.Close()
			// Malformed repo / collection — likely an account that was deleted
			// or a DID that doesn't have any records of this collection.
			// Treat as empty rather than blocking the whole bootstrap.
			return all, nil
		}
		if resp.StatusCode != http.StatusOK {
			body.Close()
			return nil, fmt.Errorf("repo: GET %s: status %d", u, resp.StatusCode)
		}
		var page listRecordsResp
		if err := json.NewDecoder(body).Decode(&page); err != nil {
			body.Close()
			return nil, fmt.Errorf("repo: decode: %w", err)
		}
		body.Close()

		for _, rec := range page.Records {
			rec.DID = did
			rec.Collection = collection
			rec.RKey = rkeyFromURI(rec.URI)
			all = append(all, rec)
		}
		if page.Cursor == "" || len(page.Records) == 0 {
			return all, nil
		}
		cursor = page.Cursor
	}
}

// rkeyFromURI returns the trailing path segment of an at:// URI, which is the
// collection-scoped record key.
func rkeyFromURI(uri string) string {
	for i := len(uri) - 1; i >= 0; i-- {
		if uri[i] == '/' {
			return uri[i+1:]
		}
	}
	return ""
}

// do performs the GET with retry-on-rate-limit and retry-on-5xx. On 429 we
// honor the Retry-After header (delta-seconds) when present; otherwise we
// fall back to exponential backoff bounded by MinBackoff / MaxBackoff.
//
// We deliberately do NOT retry on connection errors or 4xx other than 429:
// those are usually structural (DNS, TLS, deactivated repo) and the caller
// will already have classified them.
func (c *HTTPClient) do(ctx context.Context, u *url.URL) (*http.Response, error) {
	maxRetries := c.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}
	min := c.MinBackoff
	if min <= 0 {
		min = time.Second
	}
	max := c.MaxBackoff
	if max <= 0 {
		max = 30 * time.Second
	}

	backoff := min
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "at-snapshot/1.0 (+https://github.com/pboueri/atproto-db-snapshot)")
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return nil, fmt.Errorf("repo: GET %s: %w", u, err)
		}
		retryable := resp.StatusCode == http.StatusTooManyRequests ||
			(resp.StatusCode >= 500 && resp.StatusCode < 600)
		if !retryable || attempt >= maxRetries {
			return resp, nil
		}

		// Drain and close so the connection can be reused.
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		resp.Body.Close()

		wait := retryAfter
		if wait <= 0 {
			wait = backoff
			backoff *= 2
			if backoff > max {
				backoff = max
			}
		}
		slog.Debug("repo retry", "url", u.String(), "status", resp.StatusCode, "wait", wait, "attempt", attempt+1)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

// parseRetryAfter parses delta-seconds form ("30") or HTTP-date form. We
// only handle the integer-seconds case in practice — bsky.social returns
// numeric Retry-After — so date parsing is a one-liner fallback.
func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	if n, err := strconv.Atoi(h); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
