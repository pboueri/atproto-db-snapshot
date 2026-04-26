package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// snapshotMetadata mirrors the on-disk shape of snapshot/snapshot_metadata.json
// written by the snapshot subcommand. We use json.Unmarshal directly into
// the typed fields we care about and ignore any extras the writer adds.
type snapshotMetadata struct {
	SnapshotAt   *time.Time       `json:"snapshot_at"`
	LookbackDays int              `json:"lookback_days"`
	RowCounts    map[string]int64 `json:"row_counts"`
}

// readSnapshot inspects the snapshot/ tree in object storage. It reads the
// metadata sidecar for snapshot timing and row counts, then Stats the two
// duckdb files for sizes. Either source missing degrades to found=false /
// zero values rather than failing the section.
func readSnapshot(ctx context.Context, cfg config.Config, deps Deps) SnapshotStatus {
	st := SnapshotStatus{
		LookbackDays: cfg.LookbackDays,
		RowCounts:    map[string]int64{},
	}

	meta, err := readSnapshotMetadata(ctx, deps.ObjStore)
	switch {
	case err == nil && meta != nil:
		st.Found = true
		if meta.SnapshotAt != nil {
			t := meta.SnapshotAt.UTC()
			st.SnapshotAt = &t
		}
		if meta.LookbackDays > 0 {
			st.LookbackDays = meta.LookbackDays
		}
		for k, v := range meta.RowCounts {
			st.RowCounts[k] = v
		}
	case err == nil && meta == nil:
		// metadata file absent — fall through to file-size probe so we can
		// still report whether the duckdb outputs themselves exist.
	default:
		slog.Warn("monitor: read snapshot metadata", "err", err)
		st.Error = errMessage(err)
	}

	if size, err := statSize(ctx, deps.ObjStore, "snapshot/current_all.duckdb"); err == nil {
		st.CurrentAllSizeBytes = size
		st.Found = true
	} else if !errors.Is(err, objstore.ErrNotExist) {
		slog.Warn("monitor: stat current_all.duckdb", "err", err)
		if st.Error == "" {
			st.Error = errMessage(err)
		}
	}
	if size, err := statSize(ctx, deps.ObjStore, "snapshot/current_graph.duckdb"); err == nil {
		st.CurrentGraphSizeBytes = size
		st.Found = true
	} else if !errors.Is(err, objstore.ErrNotExist) {
		slog.Warn("monitor: stat current_graph.duckdb", "err", err)
		if st.Error == "" {
			st.Error = errMessage(err)
		}
	}
	return st
}

// readSnapshotMetadata returns the parsed metadata JSON, or (nil, nil) if
// the sidecar does not yet exist.
func readSnapshotMetadata(ctx context.Context, obj objstore.Store) (*snapshotMetadata, error) {
	rc, err := obj.Get(ctx, "snapshot/snapshot_metadata.json")
	if err != nil {
		if errors.Is(err, objstore.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var meta snapshotMetadata
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func statSize(ctx context.Context, obj objstore.Store, path string) (int64, error) {
	o, err := obj.Stat(ctx, path)
	if err != nil {
		return 0, err
	}
	return o.Size, nil
}
