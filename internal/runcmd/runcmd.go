// Package runcmd implements the `at-snapshot run` subcommand.
//
// The package is named runcmd (not run) because "run" is a common helper name
// that would shadow the subcommand entrypoint's package import inside main.
package runcmd

import (
	"context"
	"errors"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

// Run subscribes to jetstream and writes date-partitioned parquet to objstore.
func Run(ctx context.Context, cfg config.Config) error {
	return errors.New("run: not yet implemented")
}
