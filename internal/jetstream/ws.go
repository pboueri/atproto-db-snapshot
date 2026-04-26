package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// WSSubscriber is the production Subscriber that connects to a Bluesky
// jetstream over WebSocket. It supports a fixed list of endpoints and
// fails over to the next on disconnect.
//
// One active connection at a time; on disconnect it walks the endpoint list
// in order, dialing the next one with a small backoff. The Cursor in
// SubscribeOptions is forwarded as the `cursor` query parameter so the
// jetstream replays from that point.
type WSSubscriber struct {
	Endpoints []string
	// Backoff is the initial reconnect delay; doubled up to MaxBackoff on
	// repeated failures. Defaults are sensible for production.
	Backoff    time.Duration
	MaxBackoff time.Duration
}

// NewWS returns a WSSubscriber with sane defaults.
func NewWS(endpoints []string) *WSSubscriber {
	return &WSSubscriber{
		Endpoints:  endpoints,
		Backoff:    1 * time.Second,
		MaxBackoff: 30 * time.Second,
	}
}

// Subscribe streams events. The returned channels are closed when ctx is
// cancelled; the error channel emits at most one terminal error.
func (w *WSSubscriber) Subscribe(ctx context.Context, opts SubscribeOptions) (<-chan Event, <-chan error) {
	out := make(chan Event)
	errc := make(chan error, 1)
	if len(w.Endpoints) == 0 {
		errc <- fmt.Errorf("jetstream: no endpoints configured")
		close(out)
		close(errc)
		return out, errc
	}
	go w.run(ctx, opts, out, errc)
	return out, errc
}

// run loops over endpoints with backoff, advancing opts.Cursor each time we
// successfully read an event so reconnects don't replay events we've already
// processed.
func (w *WSSubscriber) run(ctx context.Context, opts SubscribeOptions, out chan<- Event, errc chan<- error) {
	defer close(out)
	defer close(errc)

	backoff := w.Backoff
	if backoff <= 0 {
		backoff = time.Second
	}
	maxBackoff := w.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	for {
		if err := ctx.Err(); err != nil {
			errc <- err
			return
		}
		for _, endpoint := range w.Endpoints {
			if err := ctx.Err(); err != nil {
				errc <- err
				return
			}
			latest, err := w.readOne(ctx, endpoint, opts, out)
			if latest > opts.Cursor {
				opts.Cursor = latest
			}
			if err == nil || err == context.Canceled {
				return
			}
			slog.Warn("jetstream connection ended", "endpoint", endpoint, "err", err)
		}
		select {
		case <-ctx.Done():
			errc <- ctx.Err()
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// readOne dials a single endpoint and reads until the connection ends or ctx
// is cancelled. It returns the last successfully-read TimeUS so the caller
// can advance the resume cursor.
func (w *WSSubscriber) readOne(ctx context.Context, endpoint string, opts SubscribeOptions, out chan<- Event) (int64, error) {
	u, err := buildURL(endpoint, opts)
	if err != nil {
		return 0, err
	}
	conn, _, err := websocket.Dial(ctx, u, nil)
	if err != nil {
		return 0, fmt.Errorf("dial %s: %w", u, err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Jetstream messages can exceed the default frame limit; raise it.
	conn.SetReadLimit(10 * 1024 * 1024)

	var latest int64
	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			return latest, err
		}
		var ev Event
		if err := json.Unmarshal(msg, &ev); err != nil {
			slog.Warn("jetstream decode", "err", err)
			continue
		}
		select {
		case <-ctx.Done():
			return latest, ctx.Err()
		case out <- ev:
			if ev.TimeUS > latest {
				latest = ev.TimeUS
			}
		}
	}
}

func buildURL(endpoint string, opts SubscribeOptions) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	q := u.Query()
	if opts.Cursor > 0 {
		q.Set("cursor", strconv.FormatInt(opts.Cursor, 10))
	}
	for _, c := range opts.Collections {
		q.Add("wantedCollections", c)
	}
	for _, d := range opts.DIDs {
		q.Add("wantedDids", d)
	}
	u.RawQuery = q.Encode()
	// jetstream URL is wss://...; ensure scheme.
	if !strings.HasPrefix(u.Scheme, "ws") {
		u.Scheme = "wss"
	}
	return u.String(), nil
}
