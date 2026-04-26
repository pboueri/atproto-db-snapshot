// Package monitor implements the `at-snapshot monitor` subcommand: a
// lightweight HTTP server that surfaces job health from the data directory.
package monitor

import (
	"context"
	"errors"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

// Run starts the monitor HTTP server. Real implementation lands later.
func Run(ctx context.Context, cfg config.Config) error {
	return errors.New("monitor: not yet implemented")
}
