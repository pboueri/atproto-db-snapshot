package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// Client talks to a Bluesky relay (defaults to https://bsky.network).
// It enforces a global rate limit and retries on 429 / transient errors.
type Client struct {
	Host    string
	HTTP    *http.Client
	Limiter *rate.Limiter
}

func New(host string, rps float64, timeout time.Duration) *Client {
	if rps <= 0 {
		rps = 10
	}
	return &Client{
		Host:    host,
		HTTP:    &http.Client{Timeout: timeout},
		Limiter: rate.NewLimiter(rate.Limit(rps), int(rps)+1),
	}
}

type ListReposItem struct {
	DID    string `json:"did"`
	Head   string `json:"head"`
	Rev    string `json:"rev"`
	Active bool   `json:"active"`
	Status string `json:"status,omitempty"`
}

type listReposResp struct {
	Cursor string          `json:"cursor"`
	Repos  []ListReposItem `json:"repos"`
}

// ListRepos paginates com.atproto.sync.listRepos against the relay, calling
// cb for each page. Returns when cursor is empty, cb returns an error, or
// ctx is canceled.
func (c *Client) ListRepos(ctx context.Context, pageLimit int, cb func([]ListReposItem) error) error {
	if pageLimit <= 0 || pageLimit > 1000 {
		pageLimit = 1000
	}
	cursor := ""
	for {
		if err := c.Limiter.Wait(ctx); err != nil {
			return err
		}
		u := fmt.Sprintf("%s/xrpc/com.atproto.sync.listRepos?limit=%d", c.Host, pageLimit)
		if cursor != "" {
			u += "&cursor=" + cursor
		}
		req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
		if err != nil {
			return err
		}
		resp, err := c.doWithRetry(req, 5)
		if err != nil {
			return err
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return err
		}
		if resp.StatusCode >= 400 {
			return fmt.Errorf("listRepos %d: %s", resp.StatusCode, string(body))
		}
		var page listReposResp
		if err := json.Unmarshal(body, &page); err != nil {
			return fmt.Errorf("listRepos decode: %w", err)
		}
		if err := cb(page.Repos); err != nil {
			return err
		}
		if page.Cursor == "" {
			return nil
		}
		cursor = page.Cursor
	}
}

// GetRepo fetches the raw CAR bytes for a DID. Returns ErrNotFound for
// takendown/deactivated/404 repos so callers can skip quietly.
var ErrNotFound = errors.New("repo not found")

func (c *Client) GetRepo(ctx context.Context, did string) ([]byte, error) {
	if err := c.Limiter.Wait(ctx); err != nil {
		return nil, err
	}
	u := fmt.Sprintf("%s/xrpc/com.atproto.sync.getRepo?did=%s", c.Host, did)
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.doWithRetry(req, 4)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 || resp.StatusCode == 400 || resp.StatusCode == 410 {
		io.Copy(io.Discard, resp.Body)
		return nil, ErrNotFound
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("getRepo %s: %d %s", did, resp.StatusCode, string(body))
	}
	return io.ReadAll(resp.Body)
}

func (c *Client) doWithRetry(req *http.Request, maxAttempts int) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// simple exponential backoff
			delay := time.Duration(1<<uint(attempt-1)) * time.Second
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-time.After(delay):
			}
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == 429 {
			// Respect Retry-After when present.
			wait := 5 * time.Second
			if h := resp.Header.Get("Retry-After"); h != "" {
				if secs, perr := strconv.Atoi(h); perr == nil {
					wait = time.Duration(secs) * time.Second
				}
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-time.After(wait):
			}
			lastErr = fmt.Errorf("429 rate limited")
			continue
		}
		if resp.StatusCode >= 500 {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("upstream %d", resp.StatusCode)
			continue
		}
		return resp, nil
	}
	if lastErr == nil {
		lastErr = errors.New("exceeded retries")
	}
	return nil, lastErr
}
