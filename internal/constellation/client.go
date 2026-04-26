// Package constellation queries the Constellation backlinks service to
// derive received-edge counts for a DID (followers, likes received,
// reposts received, blocks received).
//
// The service is hosted on a Pi at the maintainer's house — be polite.
package constellation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

// UserAgent is sent on every outbound request per spec §10.
const UserAgent = "atproto-db-snapshot/0.1 (+https://github.com/pboueri/atproto-db-snapshot)"

// Counts holds the four totals we extract from /links/all.
type Counts struct {
	FollowerCount   int64
	LikesReceived   int64
	RepostsReceived int64
	BlocksReceived  int64
}

// Client paces against constellation.microcosm.blue.
type Client struct {
	BaseURL   string
	HTTP      *http.Client
	Limiter   *rate.Limiter
	UserAgent string
}

// New returns a Client paced at rps and timed out at timeout.
func New(base string, rps float64, timeout time.Duration) *Client {
	if rps <= 0 {
		rps = 5
	}
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	burst := int(rps) + 1
	if burst < 1 {
		burst = 1
	}
	return &Client{
		BaseURL:   base,
		HTTP:      &http.Client{Timeout: timeout},
		Limiter:   rate.NewLimiter(rate.Limit(rps), burst),
		UserAgent: UserAgent,
	}
}

// rawResp models the {collection: {path: count}} shape.
// Counts are returned as numbers but we tolerate strings as a defense.
type rawResp = map[string]map[string]json.Number

// Counts queries /links/all?target=<did> and extracts the totals we
// care about. Per spec §5: 4xx returns zero-value Counts and nil err
// (treat as "no data"); 5xx / network error bubbles up.
func (c *Client) Counts(ctx context.Context, did string) (Counts, error) {
	if err := c.Limiter.Wait(ctx); err != nil {
		return Counts{}, err
	}
	base, err := url.Parse(c.BaseURL)
	if err != nil {
		return Counts{}, fmt.Errorf("constellation base url: %w", err)
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/links/all"
	q := base.Query()
	q.Set("target", did)
	base.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", base.String(), nil)
	if err != nil {
		return Counts{}, err
	}
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Counts{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Counts{}, err
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		// Treat as "no data".
		return Counts{}, nil
	}
	if resp.StatusCode >= 500 {
		return Counts{}, fmt.Errorf("constellation %s: %d: %s", did, resp.StatusCode, string(body))
	}
	var raw rawResp
	if err := json.Unmarshal(body, &raw); err != nil {
		return Counts{}, fmt.Errorf("constellation decode: %w", err)
	}
	out := Counts{
		FollowerCount:   intOf(raw, "app.bsky.graph.follow", "subject"),
		BlocksReceived:  intOf(raw, "app.bsky.graph.block", "subject"),
		LikesReceived:   intOf(raw, "app.bsky.feed.like", "subject"),
		RepostsReceived: intOf(raw, "app.bsky.feed.repost", "subject"),
	}
	return out, nil
}

func intOf(m rawResp, collection, path string) int64 {
	if inner, ok := m[collection]; ok {
		if v, ok := inner[path]; ok {
			n, err := v.Int64()
			if err == nil {
				return n
			}
		}
	}
	return 0
}
