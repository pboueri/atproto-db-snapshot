package duckdbstore

import (
	"context"
	"database/sql/driver"
	"fmt"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// StreamingWriter holds long-lived Appenders for each graph collection.
// Flat memory footprint — callers push rows as they arrive, DuckDB's
// internal Appender buffer handles batching + flushing to the column store.
//
// Graph-only: follows, blocks, actors. posts / likes / reposts live in
// current_all.duckdb and are built by the incremental path, not by CAR
// backfill.
//
// NOT safe for concurrent use. The intended pattern is a single writer
// goroutine consuming from a channel.
type StreamingWriter struct {
	connector *duckdb.Connector
	conn      driver.Conn

	follows *duckdb.Appender
	blocks  *duckdb.Appender
	actors  *duckdb.Appender
}

func NewStreamingWriter(dbPath string) (*StreamingWriter, error) {
	connector, err := duckdb.NewConnector(dbPath, nil)
	if err != nil {
		return nil, err
	}
	conn, err := connector.Connect(context.Background())
	if err != nil {
		connector.Close()
		return nil, err
	}
	w := &StreamingWriter{connector: connector, conn: conn}
	w.follows, err = duckdb.NewAppenderFromConn(conn, "", "follows_current")
	if err != nil {
		w.closeAll()
		return nil, fmt.Errorf("follows appender: %w", err)
	}
	w.blocks, err = duckdb.NewAppenderFromConn(conn, "", "blocks_current")
	if err != nil {
		w.closeAll()
		return nil, fmt.Errorf("blocks appender: %w", err)
	}
	w.actors, err = duckdb.NewAppenderFromConn(conn, "", "actors")
	if err != nil {
		w.closeAll()
		return nil, fmt.Errorf("actors appender: %w", err)
	}
	return w, nil
}

func (w *StreamingWriter) AppendFollow(r FollowRow) error {
	return w.follows.AppendRow(
		r.SrcID, r.DstID, r.Rkey, nilIfZero(r.CreatedAt),
	)
}

func (w *StreamingWriter) AppendBlock(r BlockRow) error {
	return w.blocks.AppendRow(
		r.SrcID, r.DstID, r.Rkey, nilIfZero(r.CreatedAt),
	)
}

// ActorRow is an actor row to append. Counts start at 0 and are
// recomputed in a separate pass after all data is loaded.
type ActorRow struct {
	ActorID     int64
	DID         string
	Handle      string
	DisplayName string
	Description string
	AvatarCID   string
	CreatedAt   any // time.Time or nil
	IndexedAt   any
}

func (w *StreamingWriter) AppendActor(r ActorRow) error {
	return w.actors.AppendRow(
		r.ActorID, r.DID,
		nilIfEmpty(r.Handle),
		nilIfEmpty(r.DisplayName),
		nilIfEmpty(r.Description),
		nilIfEmpty(r.AvatarCID),
		r.CreatedAt,
		r.IndexedAt,
		int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0),
	)
}

// FlushAll pushes accumulated rows to DuckDB.
func (w *StreamingWriter) FlushAll() error {
	for _, a := range []*duckdb.Appender{w.follows, w.blocks, w.actors} {
		if a != nil {
			if err := a.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close flushes and closes every appender + the underlying connection.
func (w *StreamingWriter) Close() error {
	var firstErr error
	closeOne := func(a *duckdb.Appender) {
		if a == nil {
			return
		}
		if err := a.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	closeOne(w.follows)
	closeOne(w.blocks)
	closeOne(w.actors)
	if w.conn != nil {
		w.conn.Close()
	}
	if w.connector != nil {
		w.connector.Close()
	}
	return firstErr
}

func (w *StreamingWriter) closeAll() {
	if w.follows != nil {
		w.follows.Close()
	}
	if w.blocks != nil {
		w.blocks.Close()
	}
	if w.actors != nil {
		w.actors.Close()
	}
	if w.conn != nil {
		w.conn.Close()
	}
	if w.connector != nil {
		w.connector.Close()
	}
}
