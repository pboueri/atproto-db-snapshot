package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// resolveStore picks an ObjectStore based on, in priority order:
//
//  1. noUpload  → always nil (no store, no uploads)
//  2. fileStore → local FileStore at that path (CLI flag wins; useful for tests)
//  3. configPath → load YAML, dispatch on object_store.type
//     - "r2"        → NewR2Store from config + OS_ACCESS_KEY/OS_SECRET_KEY env
//     - "" | "file" → nil (file backend in YAML has no path field; use -file-store)
//     - other       → error
//  4. otherwise → nil (matches pre-wiring behavior — no store)
//
// This keeps the YAML loader scoped to just the object_store section. The
// rest of ExtendedConfig (relay, jetstream, paths, …) is not yet plumbed
// into the flat Config used by run/build/serve — that reconciliation is a
// separate change.
func resolveStore(
	ctx context.Context,
	configPath, fileStore string,
	noUpload bool,
	logger *slog.Logger,
) (objstore.ObjectStore, error) {
	if noUpload {
		return nil, nil
	}
	if fileStore != "" {
		logger.Info("file-backed object store enabled", "root", fileStore)
		return objstore.NewFileStore(fileStore, ""), nil
	}
	if configPath == "" {
		return nil, nil
	}
	ext, err := config.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config %s: %w", configPath, err)
	}
	switch ext.ObjectStore.Type {
	case "", "file":
		return nil, nil
	case "r2":
		if ext.ObjectStore.Bucket == "" || ext.ObjectStore.Endpoint == "" {
			return nil, errors.New("object_store.type=r2 requires bucket and endpoint")
		}
		s, err := objstore.NewR2Store(ctx, objstore.R2Config{
			Bucket:        ext.ObjectStore.Bucket,
			Endpoint:      ext.ObjectStore.Endpoint,
			PublicURLBase: ext.ObjectStore.PublicURLBase,
		})
		if err != nil {
			return nil, fmt.Errorf("init r2 store: %w", err)
		}
		logger.Info("r2 object store enabled",
			"bucket", ext.ObjectStore.Bucket,
			"endpoint", ext.ObjectStore.Endpoint,
			"public_url_base", ext.ObjectStore.PublicURLBase,
		)
		return s, nil
	default:
		return nil, fmt.Errorf("unsupported object_store.type %q", ext.ObjectStore.Type)
	}
}
