// Package bootstrap implements the `at-snapshot bootstrap` subcommand.
//
// It generates the initial baseline social graph by pulling the full DID list
// from PLC and then per-DID profile/follow/block records from Constellation,
// writing them once to bootstrap/YYYY-MM-DD/social_graph.duckdb.
package bootstrap

import (
	"context"
	"errors"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

// Run executes the bootstrap pipeline. Real implementation lands in a follow-up commit.
func Run(ctx context.Context, cfg config.Config) error {
	return errors.New("bootstrap: not yet implemented")
}
