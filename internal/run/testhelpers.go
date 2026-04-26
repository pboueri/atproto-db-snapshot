package run

import (
	"context"
	"log/slog"
	"path/filepath"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// SealYesterdayForTest is a test-only helper that opens an existing
// staging.db at dataDir/staging.db and seals the given UTC day into
// dataDir/daily/<day>/*.parquet + _manifest.json. It's exposed for the
// internal/e2e pipeline test (see spec §8 rollover) — production code
// uses the rollover.runScheduler path.
//
// Upload is skipped when `store` is nil.
func SealYesterdayForTest(ctx context.Context, dataDir string, cfg config.JetstreamConfig, store objstore.ObjectStore, day string, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	stagingPath := filepath.Join(dataDir, "staging.db")
	dailyDir := filepath.Join(dataDir, "daily")

	st, err := openStaging(stagingPath, cfg, logger)
	if err != nil {
		return err
	}
	defer st.Close()

	r := &rollover{
		dataDir:     dataDir,
		dailyDir:    dailyDir,
		stagingPath: stagingPath,
		store:       store,
		cfg:         cfg,
		staging:     st,
		logger:      logger,
	}
	return r.sealDay(ctx, day)
}
