// Package objstore is the storage abstraction shared by the bootstrap, run,
// and snapshot subcommands.
//
// Two backends ship in this package:
//
//   - local: a directory-rooted backend used as the default for tests and
//     single-host operation. Its URL() returns absolute file paths so DuckDB
//     can read parquet directly without credentials.
//
//   - memory: an in-process map keyed by path, used by unit tests that need
//     hermetic isolation. URL() returns a magic mem:// URL that no external
//     tool can read; consumers that need DuckDB integration should use local.
//
// An s3 backend backs the production R2 deployment and is wired up in a
// follow-up commit; the interface is fixed here so callers can be written
// against it today.
package objstore

import (
	"context"
	"io"
	"time"
)

// Store is the minimal contract every backend must satisfy. Implementations
// are expected to be safe for concurrent use.
type Store interface {
	// Put writes the contents of r to path. If contentType is empty the
	// backend chooses a sensible default.
	Put(ctx context.Context, path string, r io.Reader, contentType string) error

	// Get returns a reader for path. Callers must Close it.
	Get(ctx context.Context, path string) (io.ReadCloser, error)

	// Stat returns metadata for path. Returns ErrNotExist if the object is
	// absent so callers can probe with errors.Is.
	Stat(ctx context.Context, path string) (Object, error)

	// List returns objects whose path starts with prefix. Order is
	// unspecified.
	List(ctx context.Context, prefix string) ([]Object, error)

	// Delete removes path. Removing a missing object is not an error so
	// cleanup is idempotent.
	Delete(ctx context.Context, path string) error

	// URL returns a backend-specific identifier for path, suitable for
	// passing to DuckDB's read_parquet — for local that's an absolute file
	// path, for s3 a fully-qualified s3:// URL.
	URL(path string) string
}

// Object describes a stored item.
type Object struct {
	Path         string
	Size         int64
	LastModified time.Time
}
