// Package logging configures the process-wide slog handler.
//
// The monitor command parses these JSON lines, so the format must remain stable.
package logging

import (
	"log/slog"
	"os"
	"sync"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

var setupOnce sync.Once

// Setup installs a JSON slog handler at the given level on the default logger.
// Subsequent calls are no-ops so subcommands invoked from tests can call freely.
func Setup(level string) {
	setupOnce.Do(func() {
		lvl, err := config.ParseLogLevel(level)
		if err != nil {
			lvl = slog.LevelInfo
		}
		h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
		slog.SetDefault(slog.New(h))
	})
}
