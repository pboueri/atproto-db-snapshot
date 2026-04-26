package run

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/klauspost/compress/zstd"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

// jetstreamZstdDict is Jetstream's published zstd dictionary, copied verbatim
// from https://github.com/bluesky-social/jetstream/raw/main/pkg/models/zstd_dictionary.
// The server compresses each frame against this dictionary, so the decoder
// MUST be constructed with it loaded — otherwise every frame decodes to
// garbage.
//
//go:embed zstd_dictionary
var jetstreamZstdDict []byte

// jetstreamConsumer is the WebSocket-side state machine: it picks an
// endpoint, opens a connection at the appropriate cursor offset, decodes
// frames into stagingEvents, and reconnects on drop while rotating
// through the failover list.
type jetstreamConsumer struct {
	cfg     config.JetstreamConfig
	dataDir string

	staging *stagingDB
	cursor  cursorState

	logger *slog.Logger

	endpointIdx int
	zstdDecoder *zstd.Decoder

	// metrics — read from /api/jetstream by `serve` someday.
	eventsTotal int64
}

func (c *jetstreamConsumer) run(ctx context.Context) error {
	if c.cfg.Compress {
		dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(jetstreamZstdDict))
		if err != nil {
			return fmt.Errorf("jetstream: init zstd decoder: %w", err)
		}
		c.zstdDecoder = dec
		defer c.zstdDecoder.Close()
	}

	// Seed startEndpoint from cursor.json if it matches an entry in the
	// configured failover list — otherwise start at index 0.
	if c.cursor.Endpoint != "" {
		for i, e := range c.cfg.Endpoints {
			if e == c.cursor.Endpoint {
				c.endpointIdx = i
				break
			}
		}
	}

	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		endpoint := c.cfg.Endpoints[c.endpointIdx%len(c.cfg.Endpoints)]
		startCursor := c.cursorForReconnect()

		if c.diskTooLow() {
			c.logger.Error("disk pressure: pausing consumer", "data_dir", c.dataDir)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(60 * time.Second):
				continue
			}
		}

		c.logger.Info("connecting to jetstream",
			"endpoint", endpoint,
			"cursor", startCursor,
			"compress", c.cfg.Compress,
		)

		err := c.connectAndStream(ctx, endpoint, startCursor)
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			c.logger.Warn("jetstream connection ended", "endpoint", endpoint, "err", err)
		} else {
			c.logger.Info("jetstream connection closed cleanly", "endpoint", endpoint)
		}

		// Rotate to the next endpoint on the failover list.
		c.endpointIdx = (c.endpointIdx + 1) % len(c.cfg.Endpoints)

		// Backoff before re-attempting; reset on a clean cycle.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// cursorForReconnect returns the resume cursor for the next attempt,
// rewound by `rewind_seconds` to cover any in-flight events not yet
// checkpointed (§8.1).
func (c *jetstreamConsumer) cursorForReconnect() int64 {
	cur := c.staging.snapshotCursor()
	live := cur.Cursor
	if live == 0 {
		live = c.cursor.Cursor
	}
	if live == 0 {
		return 0
	}
	rewindUS := int64(c.cfg.RewindSeconds) * 1_000_000
	if rewindUS <= 0 {
		rewindUS = 10 * 1_000_000
	}
	out := live - rewindUS
	if out < 0 {
		return 0
	}
	return out
}

// connectAndStream opens one WebSocket and consumes until it errors or ctx
// is cancelled.
func (c *jetstreamConsumer) connectAndStream(ctx context.Context, endpoint string, cursor int64) error {
	u, err := buildJetstreamURL(endpoint, c.cfg.WantedCollections, cursor, c.cfg.Compress)
	if err != nil {
		return fmt.Errorf("build url: %w", err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(dialCtx, u, &websocket.DialOptions{
		HTTPHeader: nil,
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	c.logger.Info("jetstream connected", "endpoint", endpoint, "compress", c.cfg.Compress)
	// Allow large server frames; default 32 KiB caps small JSON messages
	// but Jetstream packs many records per frame in compress mode.
	conn.SetReadLimit(1 << 24) // 16 MiB
	defer conn.Close(websocket.StatusNormalClosure, "shutdown")

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		msgType, data, err := conn.Read(ctx)
		if err != nil {
			// Translate broken-pipe / closed connection into a generic
			// reconnect signal.
			if errors.Is(err, io.EOF) || errors.Is(err, syscall.EPIPE) {
				return err
			}
			return err
		}
		if msgType == websocket.MessageText {
			if err := c.handleFrame(ctx, data); err != nil {
				return err
			}
			continue
		}
		// Binary frame — zstd if compress=true, else raw.
		payload := data
		if c.cfg.Compress && c.zstdDecoder != nil {
			decoded, derr := c.zstdDecoder.DecodeAll(data, nil)
			if derr != nil {
				c.logger.Warn("zstd decode failed, dropping frame", "err", derr, "len", len(data))
				continue
			}
			payload = decoded
		}
		if err := c.handleFrame(ctx, payload); err != nil {
			return err
		}
	}
}

// handleFrame parses one JSON object (Jetstream sends one event per frame).
// We only act on `kind == "commit"` — `identity` and `account` events are
// ignored for now.
func (c *jetstreamConsumer) handleFrame(ctx context.Context, payload []byte) error {
	// A frame may technically contain multiple newline-delimited events
	// when compression collapses them; tolerate that.
	for _, line := range splitLines(payload) {
		if len(line) == 0 {
			continue
		}
		if err := c.handleOne(ctx, line); err != nil {
			return err
		}
	}
	return nil
}

func splitLines(b []byte) [][]byte {
	if len(b) == 0 {
		return nil
	}
	// Fast path: most frames are a single JSON object.
	if !containsByte(b, '\n') {
		return [][]byte{b}
	}
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == '\n' {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, b[start:])
	}
	return out
}

func containsByte(b []byte, c byte) bool {
	for _, x := range b {
		if x == c {
			return true
		}
	}
	return false
}

// jetstreamEvent is the Jetstream JSON envelope. We only need the fields
// for `commit` events; other event kinds (identity/account) are decoded
// loosely and discarded.
type jetstreamEvent struct {
	DID    string             `json:"did"`
	TimeUS int64              `json:"time_us"`
	Kind   string             `json:"kind"`
	Commit *jetstreamCommit   `json:"commit,omitempty"`
}

type jetstreamCommit struct {
	Rev        string          `json:"rev"`
	Operation  string          `json:"operation"` // create | update | delete
	Collection string          `json:"collection"`
	Rkey       string          `json:"rkey"`
	CID        string          `json:"cid"`
	Record     json.RawMessage `json:"record,omitempty"`
}

func (c *jetstreamConsumer) handleOne(ctx context.Context, payload []byte) error {
	var ev jetstreamEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		c.logger.Debug("jetstream decode failed", "err", err, "len", len(payload))
		return nil // skip; decode errors aren't fatal
	}
	if ev.Kind != "commit" || ev.Commit == nil {
		return nil
	}

	// Lag alarm — emitted at most once per minute even if persistent.
	if c.cfg.LagAlarm > 0 {
		nowUS := time.Now().UTC().UnixMicro()
		if lagUS := nowUS - ev.TimeUS; lagUS > int64(c.cfg.LagAlarm/time.Microsecond) {
			c.maybeLogLag(time.Duration(lagUS) * time.Microsecond)
		}
	}

	day := time.UnixMicro(ev.TimeUS).UTC().Format("2006-01-02")

	se := &stagingEvent{
		DID:        ev.DID,
		Collection: ev.Commit.Collection,
		Rkey:       ev.Commit.Rkey,
		Operation:  ev.Commit.Operation,
		TimeUS:     ev.TimeUS,
		CID:        ev.Commit.CID,
		Day:        day,
		RecordJSON: ev.Commit.Record, // nil for delete; that's fine
		Endpoint:   c.endpointURL(),
	}
	if _, err := c.staging.insert(ctx, se); err != nil {
		// SQLite errors are typically transient (locked) or fatal (disk).
		// Treat them as fatal — the outer loop will reconnect after a
		// backoff.
		return fmt.Errorf("staging insert: %w", err)
	}
	atomic.AddInt64(&c.eventsTotal, 1)
	return nil
}

func (c *jetstreamConsumer) endpointURL() string {
	return c.cfg.Endpoints[c.endpointIdx%len(c.cfg.Endpoints)]
}

// maybeLogLag throttles the lag warning to once per 60s.
var lastLagLog atomic.Int64

func (c *jetstreamConsumer) maybeLogLag(lag time.Duration) {
	now := time.Now().Unix()
	prev := lastLagLog.Load()
	if now-prev < 60 {
		return
	}
	if !lastLagLog.CompareAndSwap(prev, now) {
		return
	}
	c.logger.Warn("jetstream lag exceeds threshold", "lag", lag.Round(time.Second))
}

// buildJetstreamURL constructs the subscribe URL with the right query string
// (wantedCollections repeated, optional cursor, optional compress).
func buildJetstreamURL(endpoint string, wantedCollections []string, cursor int64, compress bool) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	q := u.Query()
	// Reset wantedCollections — config-driven.
	q.Del("wantedCollections")
	for _, c := range wantedCollections {
		q.Add("wantedCollections", c)
	}
	if cursor > 0 {
		q.Set("cursor", fmt.Sprintf("%d", cursor))
	}
	if compress {
		q.Set("compress", "true")
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// diskTooLow checks whether free disk on the data dir is below MinFreeBytes.
// Returns false on stat failure rather than blocking startup.
func (c *jetstreamConsumer) diskTooLow() bool {
	if c.cfg.MinFreeBytes <= 0 {
		return false
	}
	free, err := freeBytes(c.dataDir)
	if err != nil {
		return false
	}
	return free < uint64(c.cfg.MinFreeBytes)
}

// freeBytes returns df-equivalent free bytes for the filesystem
// containing path. Uses syscall.Statfs which is a no-op on plan9; on
// supported Unix this is enough.
func freeBytes(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}

