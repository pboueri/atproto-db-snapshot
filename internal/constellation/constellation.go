// Package constellation queries https://constellation.microcosm.blue (or any
// API-compatible mirror) for backlink records. Constellation indexes every
// firehose record and answers "what records link to this target?" — which
// for app.bsky.graph.follow / app.bsky.graph.block corresponds to "who
// follows / blocks this DID?" via the .subject field.
//
// The bootstrap pipeline iterates DIDs as targets and uses Constellation
// to discover incoming follow/block edges, sidestepping the per-PDS
// listRecords rate limits entirely.
//
// Politeness:
//   - Constellation publishes no quantitative rate limit but its docs ask
//     callers to "put your project name and bsky username (or email) in
//     your user-agent header." HTTPClient sets that header from UserAgent.
//   - We self-throttle via a token bucket so a worker pool collectively
//     stays under whatever ceiling we configure, and back off on 429.
package constellation

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// Link is one row from /links: a record that links TO a target. The DID
// owns the linking record; (Collection, RKey) identifies the record on
// that DID's repo. The target itself does not appear in the row — the
// caller already knows it.
type Link struct {
	DID        string `json:"did"`
	Collection string `json:"collection"`
	RKey       string `json:"rkey"`
}

// Client streams backlinks for (target, collection, path) tuples.
//
// path is the JSON pointer-ish field that linking records used to point
// at the target — for app.bsky.graph.follow this is ".subject". yield is
// called per Link; returning false stops the stream early.
type Client interface {
	GetBacklinks(ctx context.Context, target, collection, path string, yield func(Link) bool) error
}

// HTTPClient talks to a Constellation instance over HTTP using the legacy
// /links endpoint (the modern /xrpc/blue.microcosm.links.getBacklinks
// expects different query parameter names; /links is stable and ergonomic).
type HTTPClient struct {
	BaseURL string
	HTTP    *http.Client

	// UserAgent is sent verbatim. Constellation asks for project + contact
	// in this header. NewHTTP composes a sensible default; tests can override.
	UserAgent string

	// PageSize is the per-request /links cap; the server enforces a hard
	// max of 100. Default 100.
	PageSize int

	// Limiter gates every request through a single shared bucket because
	// Constellation runs on a single host. nil = unlimited (tests only).
	Limiter *rate.Limiter

	// MaxRetries caps retry attempts on 429 / 5xx. Default 5.
	MaxRetries int
	// MinBackoff and MaxBackoff bracket the exponential backoff schedule;
	// Retry-After from the server overrides when present.
	MinBackoff, MaxBackoff time.Duration
}

// NewHTTP returns a production HTTPClient with polite defaults: a 60s
// timeout, baselineRPS sustained throughput with a small burst, retry on
// 429 / 5xx, and a User-Agent that identifies the project and (if
// supplied) an operator contact string.
func NewHTTP(baseURL, contact string, baselineRPS float64, burst int) *HTTPClient {
	if baselineRPS <= 0 {
		baselineRPS = 10
	}
	if burst <= 0 {
		burst = int(baselineRPS) * 2
		if burst <= 0 {
			burst = 20
		}
	}
	return &HTTPClient{
		BaseURL:    baseURL,
		HTTP:       &http.Client{Timeout: 60 * time.Second},
		UserAgent:  buildUserAgent(contact),
		PageSize:   100,
		Limiter:    rate.NewLimiter(rate.Limit(baselineRPS), burst),
		MaxRetries: 5,
		MinBackoff: 1 * time.Second,
		MaxBackoff: 30 * time.Second,
	}
}

// buildUserAgent returns the standard project UA, optionally with a
// contact suffix per Constellation's "if you want to be nice" guidance.
func buildUserAgent(contact string) string {
	const base = "at-snapshot/1.0 (+https://github.com/pboueri/atproto-db-snapshot)"
	if contact == "" {
		return base
	}
	return fmt.Sprintf("at-snapshot/1.0 (+https://github.com/pboueri/atproto-db-snapshot; %s)", contact)
}

// linksResponse mirrors the legacy /links response shape.
type linksResponse struct {
	Total          int64  `json:"total"`
	LinkingRecords []Link `json:"linking_records"`
	Cursor         string `json:"cursor"`
}

func (c *HTTPClient) GetBacklinks(ctx context.Context, target, collection, path string, yield func(Link) bool) error {
	if c.PageSize == 0 {
		c.PageSize = 100
	}
	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		u, err := url.Parse(c.BaseURL)
		if err != nil {
			return fmt.Errorf("constellation: parse base url: %w", err)
		}
		u.Path = "/links"
		q := u.Query()
		q.Set("target", target)
		q.Set("collection", collection)
		q.Set("path", path)
		q.Set("limit", strconv.Itoa(c.PageSize))
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		u.RawQuery = q.Encode()

		resp, err := c.do(ctx, u)
		if err != nil {
			return err
		}
		var page linksResponse
		dec := json.NewDecoder(resp.Body)
		decErr := dec.Decode(&page)
		resp.Body.Close()
		if decErr != nil {
			return fmt.Errorf("constellation: decode: %w", decErr)
		}

		for _, l := range page.LinkingRecords {
			if !yield(l) {
				return nil
			}
		}
		if page.Cursor == "" || len(page.LinkingRecords) == 0 {
			return nil
		}
		cursor = page.Cursor
	}
}

// do issues a GET with retry-on-429-or-5xx. Cancelled contexts surface
// immediately. 4xx other than 429 are treated as fatal: the caller can
// decide whether they're empty (e.g. unknown target → 404) or programmer
// error.
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
		if c.Limiter != nil {
			if err := c.Limiter.Wait(ctx); err != nil {
				return nil, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		if c.UserAgent != "" {
			req.Header.Set("User-Agent", c.UserAgent)
		}
		req.Header.Set("Accept", "application/json")
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return nil, fmt.Errorf("constellation: GET %s: %w", u, err)
		}
		retryable := resp.StatusCode == http.StatusTooManyRequests ||
			(resp.StatusCode >= 500 && resp.StatusCode < 600)
		if !retryable {
			if resp.StatusCode != http.StatusOK {
				body := readSnippet(resp)
				resp.Body.Close()
				return nil, fmt.Errorf("constellation: GET %s: status %d: %s", u, resp.StatusCode, body)
			}
			return resp, nil
		}
		if attempt >= maxRetries {
			body := readSnippet(resp)
			resp.Body.Close()
			return nil, fmt.Errorf("constellation: GET %s: status %d after %d retries: %s", u, resp.StatusCode, attempt, body)
		}
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
		slog.Debug("constellation retry", "url", u.String(), "status", resp.StatusCode, "wait", wait, "attempt", attempt+1)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

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

// readSnippet returns up to 256 bytes of the body for inclusion in error
// messages. The caller is responsible for closing the body afterward.
func readSnippet(resp *http.Response) string {
	buf := make([]byte, 256)
	n, _ := resp.Body.Read(buf)
	return string(buf[:n])
}
