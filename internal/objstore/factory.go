package objstore

import (
	"fmt"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

// FromConfig constructs the Store backend named by cfg.ObjectStore.
func FromConfig(cfg config.Config) (Store, error) {
	switch cfg.ObjectStore {
	case "local":
		return NewLocal(cfg.ObjectStoreRoot)
	case "memory":
		return NewMemory(), nil
	case "s3":
		return NewS3(cfg)
	default:
		return nil, fmt.Errorf("objstore: unsupported backend %q", cfg.ObjectStore)
	}
}
