package objstore

import (
	"fmt"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

// FromConfig constructs the Store backend named by cfg.ObjectStore.
//
// The s3 backend is intentionally not yet wired up here — the production
// deployment commit lands it without churning every caller. For now s3
// returns an error and tests use local or memory.
func FromConfig(cfg config.Config) (Store, error) {
	switch cfg.ObjectStore {
	case "local":
		return NewLocal(cfg.ObjectStoreRoot)
	case "memory":
		return NewMemory(), nil
	case "s3":
		return nil, fmt.Errorf("objstore: s3 backend not yet implemented; use local for now")
	default:
		return nil, fmt.Errorf("objstore: unsupported backend %q", cfg.ObjectStore)
	}
}
