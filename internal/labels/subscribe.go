package labels

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"path/filepath"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/coder/websocket"
)

// DefaultLabelerURL is Bluesky's moderation-team labeler subscription
// endpoint. Single canonical source; no failover list (there is only one).
const DefaultLabelerURL = "wss://mod.bsky.app/xrpc/com.atproto.label.subscribeLabels"

// DefaultLabelerDID is the DID of Bluesky's moderation team labeler. Per
// spec we filter out labels whose `src` doesn't match this so we only
// auto-takedown on Bluesky's own moderation decisions.
const DefaultLabelerDID = "did:plc:ar7c4by46qjdydhdevvrndac"

// Options configures Run.
type Options struct {
	// DataDir is the local working directory. labels.db and
	// labels_cursor.json live at <DataDir>/labels.db and
	// <DataDir>/labels_cursor.json respectively.
	DataDir string

	// LabelerURL is the wss:// endpoint. Defaults to DefaultLabelerURL.
	LabelerURL string

	// LabelerDID filters incoming labels to only those with src == this
	// DID. Empty string disables the filter. Defaults to DefaultLabelerDID.
	LabelerDID string

	// CheckpointEveryLabels triggers a WAL checkpoint + cursor flush every
	// N labels. Defaults to 1000.
	CheckpointEveryLabels int

	// CheckpointInterval triggers a WAL checkpoint + cursor flush at most
	// this often even if we haven't hit CheckpointEveryLabels. Defaults to
	// 60s.
	CheckpointInterval time.Duration
}

func (o *Options) defaults() {
	if o.DataDir == "" {
		o.DataDir = "./data"
	}
	if o.LabelerURL == "" {
		o.LabelerURL = DefaultLabelerURL
	}
	// LabelerDID intentionally not defaulted — "" means "keep all labels,
	// no src filter". The CLI layer is responsible for populating it with
	// DefaultLabelerDID unless the operator explicitly opts out.
	if o.CheckpointEveryLabels <= 0 {
		o.CheckpointEveryLabels = 1000
	}
	if o.CheckpointInterval <= 0 {
		o.CheckpointInterval = 60 * time.Second
	}
}

// Run is the long-running entry point. It opens the labels.db store and
// connects to the labeler, reconnecting with exponential backoff on drop.
// On ctx cancellation it does a final checkpoint + cursor flush before
// returning.
func Run(ctx context.Context, opts Options, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	opts.defaults()

	dbPath := filepath.Join(opts.DataDir, "labels.db")
	cursorPath := filepath.Join(opts.DataDir, "labels_cursor.json")

	st, err := openStore(dbPath)
	if err != nil {
		return fmt.Errorf("open labels.db: %w", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			logger.Warn("closing labels.db", "err", err)
		}
	}()

	cur, err := loadCursor(cursorPath)
	if err != nil {
		return fmt.Errorf("load labels cursor: %w", err)
	}
	logger.Info("labels subscriber starting",
		"endpoint", opts.LabelerURL,
		"resume_seq", cur.Seq,
		"filter_src", opts.LabelerDID,
		"db_path", dbPath,
	)

	// Flush cursor one last time on exit — unconditional so the cursor
	// matches in-memory state even on a graceful shutdown with no new
	// labels since the last periodic checkpoint.
	defer func() {
		if cur.Seq > 0 {
			if err := saveCursorAtomic(cursorPath, cur); err != nil {
				logger.Warn("final cursor save", "err", err)
			}
		}
		if err := st.Checkpoint(context.Background()); err != nil {
			logger.Warn("final checkpoint", "err", err)
		}
	}()

	backoff := 5 * time.Second
	const maxBackoff = 60 * time.Second

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		c := newConsumer(opts, st, &cur, logger)
		connErr := c.connectAndStream(ctx, cursorPath)
		if errors.Is(connErr, context.Canceled) || ctx.Err() != nil {
			return ctx.Err()
		}
		if connErr != nil {
			logger.Warn("labeler connection ended", "err", connErr)
		} else {
			logger.Info("labeler connection closed cleanly")
		}

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

// consumer is the per-connection state; reconstructed on every reconnect.
type consumer struct {
	opts   Options
	st     *store
	logger *slog.Logger

	// cursorMu guards every access to *cursor. The read loop writes via
	// handleLabels; the checkpoint goroutine reads. Without this lock the
	// race detector flags the cursor.Seq update vs the periodic flush.
	cursorMu sync.Mutex
	cursor   *cursorState
}

func newConsumer(opts Options, st *store, cur *cursorState, logger *slog.Logger) *consumer {
	return &consumer{opts: opts, st: st, cursor: cur, logger: logger}
}

// connectAndStream opens one WebSocket and consumes until it errors or ctx
// is cancelled. The caller is responsible for reconnect/backoff.
func (c *consumer) connectAndStream(ctx context.Context, cursorPath string) error {
	u, err := buildSubscribeURL(c.opts.LabelerURL, c.cursor.Seq)
	if err != nil {
		return fmt.Errorf("build url: %w", err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(dialCtx, u, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", u, err)
	}
	c.cursorMu.Lock()
	startSeq := c.cursor.Seq
	c.cursorMu.Unlock()
	c.logger.Info("labeler connected", "endpoint", c.opts.LabelerURL, "cursor", startSeq)
	// Labeler frames fit easily in 32KiB but bump just in case a batch
	// arrives. The real relay spec puts an upper bound ~1MB.
	conn.SetReadLimit(1 << 22) // 4 MiB
	defer conn.Close(websocket.StatusNormalClosure, "shutdown")

	// Periodic checkpoint ticker runs concurrently with the read loop.
	// We wait for it to finish before returning so the next reconnect
	// (which creates a new consumer with a fresh mutex on the same
	// shared *cursorState) does not race with this iteration's flush.
	ckCtx, ckCancel := context.WithCancel(ctx)
	var ckWG sync.WaitGroup
	ckWG.Add(1)
	go func() {
		defer ckWG.Done()
		c.checkpointLoop(ckCtx, cursorPath)
	}()
	defer func() {
		ckCancel()
		ckWG.Wait()
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		mt, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if mt != websocket.MessageBinary {
			// Ignore text frames — the labeler only speaks binary.
			continue
		}
		if err := c.handleFrame(ctx, data, cursorPath); err != nil {
			return err
		}
	}
}

// handleFrame decodes one WebSocket binary frame: [header CBOR][payload CBOR].
// We hand the same *bytes.Reader to both UnmarshalCBORs — cbor-gen reads
// exactly the bytes it needs, leaving the payload bytes queued up.
func (c *consumer) handleFrame(ctx context.Context, data []byte, cursorPath string) error {
	r := bytes.NewReader(data)

	var hdr events.EventHeader
	if err := hdr.UnmarshalCBOR(r); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}
		c.logger.Debug("labeler header decode failed", "err", err, "len", len(data))
		return nil
	}

	switch hdr.Op {
	case events.EvtKindMessage:
		// fallthrough
	case events.EvtKindErrorFrame:
		// The stream sent an error frame — surface it as a connection
		// error so the outer loop reconnects after backoff.
		return fmt.Errorf("labeler error frame (op=%d, t=%q)", hdr.Op, hdr.MsgType)
	default:
		c.logger.Debug("unknown labeler op", "op", hdr.Op, "t", hdr.MsgType)
		return nil
	}

	switch hdr.MsgType {
	case "#labels":
		var evt comatproto.LabelSubscribeLabels_Labels
		if err := evt.UnmarshalCBOR(r); err != nil {
			c.logger.Debug("labels payload decode failed", "err", err)
			return nil
		}
		return c.handleLabels(ctx, &evt)
	case "#info":
		// Info frames are advisory (OutdatedCursor, etc). Log and
		// continue.
		c.logger.Info("labeler info frame", "t", hdr.MsgType)
		return nil
	default:
		c.logger.Debug("unhandled labeler msg type", "t", hdr.MsgType)
		return nil
	}
}

// handleLabels persists each label row whose src matches our filter (if
// any). Advances the cursor watermark.
func (c *consumer) handleLabels(ctx context.Context, evt *comatproto.LabelSubscribeLabels_Labels) error {
	for _, lbl := range evt.Labels {
		if lbl == nil {
			continue
		}
		if c.opts.LabelerDID != "" && lbl.Src != c.opts.LabelerDID {
			continue
		}
		neg := false
		if lbl.Neg != nil {
			neg = *lbl.Neg
		}
		row := Label{
			Seq: evt.Seq,
			Src: lbl.Src,
			URI: lbl.Uri,
			CID: lbl.Cid,
			Val: lbl.Val,
			CTS: lbl.Cts,
			Exp: lbl.Exp,
			Neg: neg,
		}
		if _, err := c.st.Upsert(ctx, row); err != nil {
			return fmt.Errorf("upsert label: %w", err)
		}
	}
	c.cursorMu.Lock()
	if evt.Seq > c.cursor.Seq {
		c.cursor.Seq = evt.Seq
		c.cursor.Endpoint = c.opts.LabelerURL
	}
	c.cursorMu.Unlock()
	return nil
}

// checkpointLoop drives periodic wal_checkpoint + cursor flush on both a
// time interval and a label-count threshold.
func (c *consumer) checkpointLoop(ctx context.Context, cursorPath string) {
	tick := time.NewTicker(c.opts.CheckpointInterval)
	defer tick.Stop()
	rowsTick := time.NewTicker(500 * time.Millisecond)
	defer rowsTick.Stop()

	flush := func(why string) {
		if err := c.st.Checkpoint(ctx); err != nil {
			c.logger.Warn("labels checkpoint", "why", why, "err", err)
			return
		}
		// Snapshot the cursor under the lock so we don't read torn fields
		// while handleLabels is mid-update.
		c.cursorMu.Lock()
		snap := *c.cursor
		c.cursorMu.Unlock()
		if snap.Seq > 0 {
			if err := saveCursorAtomic(cursorPath, snap); err != nil {
				c.logger.Warn("labels cursor save", "err", err)
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			flush("interval")
		case <-rowsTick.C:
			if c.st.Count() >= int64(c.opts.CheckpointEveryLabels) {
				flush("rows")
			}
		}
	}
}

// buildSubscribeURL appends ?cursor=<seq> to the configured labeler URL if
// seq > 0. Otherwise returns the URL unchanged.
func buildSubscribeURL(endpoint string, seq int64) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	if seq > 0 {
		q := u.Query()
		q.Set("cursor", fmt.Sprintf("%d", seq))
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}
