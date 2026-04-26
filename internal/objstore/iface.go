// Package objstore provides a portable abstraction over object storage
// backends (Cloudflare R2, S3, local filesystem) used by the snapshotter.
//
// See specs/001_bootstrap.md §12 ("Object store abstraction") for the
// canonical interface contract.
package objstore

import (
	"context"
	"io"
	"time"
)

// ObjectInfo describes a single object returned by List.
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// ObjectStore is the portable interface implemented by every backend.
//
// PutAtomic is the only "interesting" method: implementations must ensure
// that readers never observe a torn or partial value at `key`. On the
// filesystem this is `write-to-tmp + os.Rename`; on S3/R2 it is
// `upload to key.new` + `server-side copy to key` + `delete key.new`.
type ObjectStore interface {
	Exists(ctx context.Context, key string) (bool, error)
	Get(ctx context.Context, key string, w io.Writer) error
	PutAtomic(ctx context.Context, key string, r io.Reader, size int64) error
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	PublicURL(key string) string
}
