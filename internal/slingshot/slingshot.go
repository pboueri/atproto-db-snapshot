// Package slingshot fetches single ATProto records from
// https://slingshot.microcosm.blue (or any API-compatible mirror), an edge
// cache that pre-loads firehose records and resolves identities. The
// bootstrap pipeline uses this for the profile leg — profiles are
// singletons at rkey=self, so com.atproto.repo.getRecord covers it
// exactly without listRecords.
//
// On cache miss Slingshot falls back to the upstream PDS, so callers
// don't need to plumb PDS URLs through.
//
// ErrNotFound is returned when the upstream reports RecordNotFound; the
// bootstrap caller treats that as "no profile" rather than fatal.
package slingshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// Record matches the com.atproto.repo.getRecord response shape.
type Record struct {
	URI   string          `json:"uri"`
	CID   string          `json:"cid"`
	Value json.RawMessage `json:"value"`
}

// ErrNotFound signals the upstream reports the record doesn't exist —
// either because the DID has no record at this collection/rkey or the
// repo has been deactivated. Distinct from network / 5xx errors so the
// caller can elide them rather than counting them as fetch failures.
var ErrNotFound = errors.New("slingshot: record not found")

// Client fetches single records by (DID, collection, rkey).
type Client interface {
	GetRecord(ctx context.Context, did, collection, rkey string) (Record, error)
}

// HTTPClient talks to a Slingshot instance. The instance public URL is
// https://slingshot.microcosm.blue but operators can self-host.
type HTTPClient struct {
	BaseURL   string
	HTTP      *http.Client
	UserAgent string

	Limiter    *rate.Limiter
	MaxRetries int
	MinBackoff, MaxBackoff time.Duration
}

// NewHTTP returns a production HTTPClient: 30s timeout, baselineRPS
// shared across all callers, retry on 429 / 5xx, identifying User-Agent.
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
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 2000
	tr.MaxIdleConnsPerHost = 2000
	tr.MaxConnsPerHost = 2000
	tr.IdleConnTimeout = 90 * time.Second
	return &HTTPClient{
		BaseURL:    baseURL,
		HTTP:       &http.Client{Timeout: 30 * time.Second, Transport: tr},
		UserAgent:  buildUserAgent(contact),
		Limiter:    rate.NewLimiter(rate.Limit(baselineRPS), burst),
		MaxRetries: 5,
		MinBackoff: 1 * time.Second,
		MaxBackoff: 30 * time.Second,
	}
}

func buildUserAgent(contact string) string {
	const base = "at-snapshot/1.0 (+https://github.com/pboueri/atproto-db-snapshot)"
	if contact == "" {
		return base
	}
	return fmt.Sprintf("at-snapshot/1.0 (+https://github.com/pboueri/atproto-db-snapshot; %s)", contact)
}

func (c *HTTPClient) GetRecord(ctx context.Context, did, collection, rkey string) (Record, error) {
	u, err := url.Parse(c.BaseURL)
	if err != nil {
		return Record{}, fmt.Errorf("slingshot: parse base url: %w", err)
	}
	u.Path = "/xrpc/com.atproto.repo.getRecord"
	q := u.Query()
	q.Set("repo", did)
	q.Set("collection", collection)
	q.Set("rkey", rkey)
	u.RawQuery = q.Encode()

	resp, err := c.do(ctx, u)
	if err != nil {
		return Record{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusNotFound {
		// Slingshot returns 400 with an XRPC error payload for
		// RecordNotFound; surface as ErrNotFound so the caller can elide
		// it from "fetch error" counters.
		return Record{}, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return Record{}, fmt.Errorf("slingshot: GET %s: status %d", u, resp.StatusCode)
	}
	var rec Record
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return Record{}, fmt.Errorf("slingshot: decode: %w", err)
	}
	return rec, nil
}

// do issues a GET with retry on 429 / 5xx, mirroring the constellation
// client's retry policy.
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
			return nil, fmt.Errorf("slingshot: GET %s: %w", u, err)
		}
		retryable := resp.StatusCode == http.StatusTooManyRequests ||
			(resp.StatusCode >= 500 && resp.StatusCode < 600)
		if !retryable || attempt >= maxRetries {
			return resp, nil
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
		slog.Debug("slingshot retry", "url", u.String(), "status", resp.StatusCode, "wait", wait, "attempt", attempt+1)
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
