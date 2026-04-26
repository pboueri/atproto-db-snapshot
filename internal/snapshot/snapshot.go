// Package snapshot implements the `at-snapshot snapshot` subcommand.
//
// It rolls up the past N days of raw parquet from object storage into the two
// final DuckDB outputs (current_all.duckdb, current_graph.duckdb) plus a
// snapshot_metadata.json sibling.
package snapshot

import (
	"context"
	"errors"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

// Run materializes the snapshot DuckDB files. Real implementation lands later.
func Run(ctx context.Context, cfg config.Config) error {
	return errors.New("snapshot: not yet implemented")
}
