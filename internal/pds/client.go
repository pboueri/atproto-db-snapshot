// Package pds talks to a single Personal Data Server via the
// com.atproto.repo.listRecords XRPC endpoint. One Client is created per
// PDS host; the Client holds a tuned http.Transport, a per-host rate
// limiter, and a circuit breaker.
package pds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/pboueri/atproto-db-snapshot/internal/parse"
)

// UserAgent is sent on every outbound request per spec §10.
const UserAgent = "atproto-db-snapshot/0.1 (+https://github.com/pboueri/atproto-db-snapshot)"

// ErrSkip is returned when a DID should be marked processed without
// retry (e.g. RepoNotFound, InvalidRequest, 404). Callers compare with
// errors.Is.
var ErrSkip = errors.New("pds: skip")

// ErrCircuitOpen is returned when the per-host circuit breaker has
// tripped and the cooldown hasn't elapsed.
var ErrCircuitOpen = errors.New("pds: circuit breaker open")

// Options configures a Client.
type Options struct {
	HTTPTimeout      time.Duration
	RPS              float64
	MaxRetries       int
	BreakerThreshold int
	BreakerCooldown  time.Duration
	UserAgent        string
}

func (o *Options) defaults() {
	if o.HTTPTimeout <= 0 {
		o.HTTPTimeout = 10 * time.Second
	}
	if o.RPS <= 0 {
		o.RPS = 9
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = 4
	}
	if o.BreakerThreshold <= 0 {
		o.BreakerThreshold = 5
	}
	if o.BreakerCooldown <= 0 {
		o.BreakerCooldown = 5 * time.Minute
	}
	if o.UserAgent == "" {
		o.UserAgent = UserAgent
	}
}

// Client wraps an http.Client + rate limiter + circuit breaker for one
// PDS host. NOT safe to share across hosts — use one Client per host.
type Client struct {
	Host    string
	HTTP    *http.Client
	Limiter *rate.Limiter
	opts    Options

	mu              sync.Mutex
	consec429       int
	consecFailures  int
	openUntil       time.Time
	probeInProgress bool
}

// New constructs a Client for the given host. host should be a full URL
// (e.g. "https://amanita.us-east.host.bsky.network").
func New(host string, opts Options) *Client {
	opts.defaults()
	transport := &http.Transport{
		MaxIdleConnsPerHost: 16,
		MaxConnsPerHost:     32,
		IdleConnTimeout:     90 * time.Second,
		// HTTP/2 is enabled automatically by net/http for https://.
	}
	burst := int(opts.RPS) + 1
	if burst < 1 {
		burst = 1
	}
	return &Client{
		Host:    host,
		HTTP:    &http.Client{Transport: transport, Timeout: opts.HTTPTimeout},
		Limiter: rate.NewLimiter(rate.Limit(opts.RPS), burst),
		opts:    opts,
	}
}

// listRecordsResp is the wire shape of a single page response.
type listRecordsResp struct {
	Cursor  string             `json:"cursor"`
	Records []listRecordsEntry `json:"records"`
}

type listRecordsEntry struct {
	URI   string          `json:"uri"`
	CID   string          `json:"cid"`
	Value json.RawMessage `json:"value"`
}

// ListRecords paginates com.atproto.repo.listRecords for a single
// (did, collection) pair. cb is invoked once per record with the raw
// JSON value and the rkey extracted from the URI.
func (c *Client) ListRecords(ctx context.Context, did, collection string, cb func(value json.RawMessage, rkey string) error) error {
	if !c.tryEnter() {
		return ErrCircuitOpen
	}
	cursor := ""
	for {
		page, err := c.fetchPage(ctx, did, collection, cursor)
		if err != nil {
			return err
		}
		for _, rec := range page.Records {
			rkey := rkeyFromURI(rec.URI)
			if rkey == "" {
				continue
			}
			if err := cb(rec.Value, rkey); err != nil {
				return err
			}
		}
		if page.Cursor == "" || page.Cursor == cursor {
			return nil
		}
		cursor = page.Cursor
	}
}

func (c *Client) fetchPage(ctx context.Context, did, collection, cursor string) (*listRecordsResp, error) {
	limit := 100
	if collection == "app.bsky.actor.profile" {
		limit = 1
	}
	q := url.Values{}
	q.Set("repo", did)
	q.Set("collection", collection)
	q.Set("limit", strconv.Itoa(limit))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	endpoint := strings.TrimRight(c.Host, "/") + "/xrpc/com.atproto.repo.listRecords?" + q.Encode()

	var (
		body []byte
		resp *http.Response
		err  error
	)
	for attempt := 0; attempt <= c.opts.MaxRetries; attempt++ {
		if err := c.Limiter.Wait(ctx); err != nil {
			return nil, err
		}
		req, rerr := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
		if rerr != nil {
			return nil, rerr
		}
		req.Header.Set("User-Agent", c.opts.UserAgent)
		req.Header.Set("Accept", "application/json")
		resp, err = c.HTTP.Do(req)
		if err != nil {
			c.recordFailure()
			if attempt < c.opts.MaxRetries {
				if !sleepBackoff(ctx, fiveBackoff(attempt+1)) {
					return nil, ctx.Err()
				}
				continue
			}
			return nil, fmt.Errorf("listRecords %s/%s: %w", did, collection, err)
		}

		// 200 OK
		if resp.StatusCode == 200 {
			body, err = io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return nil, err
			}
			c.recordSuccess()
			var page listRecordsResp
			if err := json.Unmarshal(body, &page); err != nil {
				return nil, fmt.Errorf("listRecords decode: %w", err)
			}
			return &page, nil
		}

		// 429: respect Retry-After then 5/10/30; cap at MaxRetries
		if resp.StatusCode == 429 {
			wait := retryAfter(resp.Header.Get("Retry-After"), backoff429(attempt))
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			c.record429()
			if attempt >= c.opts.MaxRetries {
				return nil, fmt.Errorf("listRecords %s: 429 after %d attempts", did, attempt+1)
			}
			if !sleepBackoff(ctx, wait) {
				return nil, ctx.Err()
			}
			continue
		}

		// 5xx: 1s/2s/4s/8s
		if resp.StatusCode >= 500 {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			c.recordFailure()
			if attempt >= c.opts.MaxRetries {
				return nil, fmt.Errorf("listRecords %s: %d after %d attempts", did, resp.StatusCode, attempt+1)
			}
			if !sleepBackoff(ctx, fiveBackoff(attempt+1)) {
				return nil, ctx.Err()
			}
			continue
		}

		// 4xx (other than 429): inspect body.
		body, _ = io.ReadAll(io.LimitReader(resp.Body, 2048))
		resp.Body.Close()
		if resp.StatusCode == 404 {
			return nil, ErrSkip
		}
		if resp.StatusCode == 400 {
			if isSkippable400(body) {
				return nil, ErrSkip
			}
			return nil, fmt.Errorf("listRecords %s/%s: 400: %s", did, collection, string(body))
		}
		// other 4xx: not retryable; not breaker-tripping (it's our fault)
		return nil, fmt.Errorf("listRecords %s/%s: %d: %s", did, collection, resp.StatusCode, string(body))
	}
	return nil, fmt.Errorf("listRecords %s: exceeded retries", did)
}

// errEnvelope captures the AT-Proto XRPC error envelope.
type errEnvelope struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func isSkippable400(body []byte) bool {
	var env errEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return false
	}
	switch env.Error {
	case "RepoNotFound", "RepoTakendown", "RepoSuspended", "RepoDeactivated", "InvalidRequest":
		return true
	}
	return false
}

// retryAfter parses an HTTP Retry-After header (seconds). Falls back to
// def when missing or unparseable. Always returns at least 1s.
func retryAfter(header string, def time.Duration) time.Duration {
	if header == "" {
		if def < time.Second {
			return time.Second
		}
		return def
	}
	if secs, err := strconv.Atoi(header); err == nil {
		d := time.Duration(secs) * time.Second
		if d < time.Second {
			return time.Second
		}
		return d
	}
	if t, err := http.ParseTime(header); err == nil {
		d := time.Until(t)
		if d < time.Second {
			return time.Second
		}
		return d
	}
	if def < time.Second {
		return time.Second
	}
	return def
}

// backoff429 is the 5/10/30 schedule for 429s after Retry-After is absent.
func backoff429(attempt int) time.Duration {
	switch attempt {
	case 0:
		return 5 * time.Second
	case 1:
		return 10 * time.Second
	default:
		return 30 * time.Second
	}
}

// fiveBackoff returns the 1s/2s/4s/8s schedule for 5xx and network errors.
func fiveBackoff(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	if n > 4 {
		n = 4
	}
	return time.Duration(1<<uint(n-1)) * time.Second
}

func sleepBackoff(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// ---- circuit breaker ----

func (c *Client) tryEnter() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if c.openUntil.After(now) {
		// Cooldown still active. Allow exactly one probe through after
		// cooldown elapses; otherwise reject.
		return false
	}
	// If we just emerged from cooldown, schedule a probe.
	if !c.openUntil.IsZero() && !c.openUntil.After(now) {
		c.openUntil = time.Time{}
		c.probeInProgress = true
	}
	return true
}

func (c *Client) record429() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consec429++
	c.probeInProgress = false
	// Note: 429 is rate-limit signal, not a host-down signal — keep it
	// distinct from the breaker's "consecutive non-429 failures" counter.
}

func (c *Client) recordFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consecFailures++
	c.probeInProgress = false
	if c.consecFailures >= c.opts.BreakerThreshold {
		c.openUntil = time.Now().Add(c.opts.BreakerCooldown)
	}
}

func (c *Client) recordSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consec429 = 0
	c.consecFailures = 0
	c.probeInProgress = false
	c.openUntil = time.Time{}
}

// CircuitOpen reports whether the breaker is currently open.
func (c *Client) CircuitOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.openUntil.After(time.Now())
}

// ---- decoding helpers ----

// Decode decodes the raw `value` JSON for one of the supported
// collections into the appropriate parse.* type. Returns nil for
// kinds we don't care about.
func Decode(value json.RawMessage, kind string) (any, error) {
	switch kind {
	case "app.bsky.actor.profile":
		var v struct {
			DisplayName *string `json:"displayName"`
			Description *string `json:"description"`
			Avatar      *struct {
				Ref struct {
					Link string `json:"$link"`
				} `json:"ref"`
			} `json:"avatar"`
			CreatedAt *string `json:"createdAt"`
		}
		if err := json.Unmarshal(value, &v); err != nil {
			return nil, err
		}
		p := &parse.ProfileRec{
			DisplayName: v.DisplayName,
			Description: v.Description,
		}
		if v.Avatar != nil && v.Avatar.Ref.Link != "" {
			s := v.Avatar.Ref.Link
			p.AvatarCID = &s
		}
		if v.CreatedAt != nil {
			t := parse.ParseAtTime(*v.CreatedAt)
			if !t.IsZero() {
				p.CreatedAt = &t
			}
		}
		return p, nil
	case "app.bsky.graph.follow":
		var v struct {
			Subject   string `json:"subject"`
			CreatedAt string `json:"createdAt"`
		}
		if err := json.Unmarshal(value, &v); err != nil {
			return nil, err
		}
		return parse.FollowRec{
			TargetDID: v.Subject,
			CreatedAt: parse.ParseAtTime(v.CreatedAt),
		}, nil
	case "app.bsky.graph.block":
		var v struct {
			Subject   string `json:"subject"`
			CreatedAt string `json:"createdAt"`
		}
		if err := json.Unmarshal(value, &v); err != nil {
			return nil, err
		}
		return parse.BlockRec{
			TargetDID: v.Subject,
			CreatedAt: parse.ParseAtTime(v.CreatedAt),
		}, nil
	}
	return nil, nil
}

// rkeyFromURI extracts the rkey from an at-URI (last path segment).
func rkeyFromURI(uri string) string {
	if !strings.HasPrefix(uri, "at://") {
		return ""
	}
	rest := uri[len("at://"):]
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}
