// Package constellation talks to https://constellation.microcosm.blue (or any
// XRPC-speaking host) to enumerate records of a given collection for a given
// DID during bootstrap.
//
// We separate this out from a generic ATProto SDK because we only need
// listRecords for three specific collections: profiles, follows, blocks.
// Implementing it ourselves keeps the dep tree small and the behavior easy
// to mock for tests.
package constellation

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/model"
)

// Record is one ATProto record returned by listRecords.
//
// Value is the raw payload — the bootstrap fetcher decodes it into the right
// internal/model type based on collection.
type Record struct {
	URI        string             `json:"uri"`
	CID        string             `json:"cid"`
	DID        string             `json:"-"` // populated by the client
	Collection model.Collection   `json:"-"`
	RKey       string             `json:"-"`
	Value      json.RawMessage    `json:"value"`
}

// Client lists records by DID and collection.
type Client interface {
	// ListRecords returns every record in the given collection for did.
	// Pagination, retries, and rate limiting are the implementation's concern.
	ListRecords(ctx context.Context, did string, collection model.Collection) ([]Record, error)
}

// HTTPClient is the production implementation. Constellation exposes the
// standard com.atproto.repo.listRecords XRPC, which is what we hit.
type HTTPClient struct {
	BaseURL string
	HTTP    *http.Client
	// PageSize is the per-request limit; the XRPC default cap is 100.
	PageSize int
}

func NewHTTP(base string) *HTTPClient {
	return &HTTPClient{
		BaseURL:  base,
		HTTP:     &http.Client{Timeout: 30 * time.Second},
		PageSize: 100,
	}
}

// listRecordsResp matches the XRPC response shape.
type listRecordsResp struct {
	Cursor  string   `json:"cursor"`
	Records []Record `json:"records"`
}

func (c *HTTPClient) ListRecords(ctx context.Context, did string, collection model.Collection) ([]Record, error) {
	if c.PageSize == 0 {
		c.PageSize = 100
	}
	var all []Record
	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		u, err := url.Parse(c.BaseURL)
		if err != nil {
			return nil, fmt.Errorf("constellation: parse base url: %w", err)
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

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return nil, fmt.Errorf("constellation: GET %s: %w", u, err)
		}
		body := resp.Body
		if resp.StatusCode == http.StatusNotFound {
			body.Close()
			// A repo that doesn't exist (deactivated, mistyped) returns 404 —
			// treat as empty rather than a fatal error so a bootstrap pass
			// over a stale DID list keeps moving.
			return all, nil
		}
		if resp.StatusCode != http.StatusOK {
			body.Close()
			return nil, fmt.Errorf("constellation: GET %s: status %d", u, resp.StatusCode)
		}
		var page listRecordsResp
		if err := json.NewDecoder(body).Decode(&page); err != nil {
			body.Close()
			return nil, fmt.Errorf("constellation: decode: %w", err)
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
