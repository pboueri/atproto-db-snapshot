package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// FileStore is a local filesystem ObjectStore implementation.
//
// All keys are interpreted as paths relative to rootDir. Atomic writes use
// the standard tmp-file + rename trick. Useful for tests and offline
// development.
type FileStore struct {
	rootDir       string
	publicBaseURL string
}

// NewFileStore constructs a FileStore rooted at rootDir.
//
// publicBaseURL is the prefix used by PublicURL. If empty, PublicURL falls
// back to a `file://` URL pointing at the absolute path on disk.
func NewFileStore(rootDir string, publicBaseURL string) *FileStore {
	return &FileStore{
		rootDir:       rootDir,
		publicBaseURL: strings.TrimRight(publicBaseURL, "/"),
	}
}

// keyToPath converts a forward-slash key into an OS-specific path beneath
// the root directory.
func (f *FileStore) keyToPath(key string) string {
	parts := strings.Split(key, "/")
	return filepath.Join(append([]string{f.rootDir}, parts...)...)
}

// Exists implements ObjectStore.
func (f *FileStore) Exists(ctx context.Context, key string) (bool, error) {
	_, err := os.Stat(f.keyToPath(key))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// Get implements ObjectStore.
func (f *FileStore) Get(ctx context.Context, key string, w io.Writer) error {
	file, err := os.Open(f.keyToPath(key))
	if err != nil {
		return fmt.Errorf("objstore: open %q: %w", key, err)
	}
	defer file.Close()
	if _, err := io.Copy(w, file); err != nil {
		return fmt.Errorf("objstore: read %q: %w", key, err)
	}
	return nil
}

// PutAtomic writes `r` to `key.new` then atomically renames into place.
//
// `size` is accepted for interface symmetry with the S3-backed
// implementation; it is not enforced for FileStore.
func (f *FileStore) PutAtomic(ctx context.Context, key string, r io.Reader, size int64) error {
	finalPath := f.keyToPath(key)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return fmt.Errorf("objstore: mkdir for %q: %w", key, err)
	}
	tmpPath := finalPath + ".new"

	out, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("objstore: create %q: %w", tmpPath, err)
	}
	if _, err := io.Copy(out, r); err != nil {
		out.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("objstore: write %q: %w", tmpPath, err)
	}
	if err := out.Sync(); err != nil {
		out.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("objstore: fsync %q: %w", tmpPath, err)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("objstore: close %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("objstore: rename %q -> %q: %w", tmpPath, finalPath, err)
	}
	return nil
}

// List implements ObjectStore. Keys are returned with forward-slash
// separators, regardless of the host OS.
func (f *FileStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	root := f.keyToPath(prefix)

	// If the prefix points at a file (not a directory), treat that as a
	// single-element list. If it doesn't exist at all, return an empty list
	// rather than an error — matches S3 ListObjectsV2 semantics.
	info, err := os.Stat(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("objstore: stat %q: %w", prefix, err)
	}

	var out []ObjectInfo
	if !info.IsDir() {
		out = append(out, ObjectInfo{
			Key:          prefix,
			Size:         info.Size(),
			LastModified: info.ModTime(),
		})
		return out, nil
	}

	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(f.rootDir, path)
		if err != nil {
			return err
		}
		out = append(out, ObjectInfo{
			Key:          filepath.ToSlash(rel),
			Size:         fi.Size(),
			LastModified: fi.ModTime(),
		})
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("objstore: walk %q: %w", prefix, walkErr)
	}
	return out, nil
}

// Delete implements ObjectStore. Removing a non-existent key is a no-op.
func (f *FileStore) Delete(ctx context.Context, key string) error {
	if err := os.Remove(f.keyToPath(key)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("objstore: delete %q: %w", key, err)
	}
	return nil
}

// PublicURL implements ObjectStore. If publicBaseURL is empty, returns a
// `file://` URL pointing at the absolute on-disk path.
func (f *FileStore) PublicURL(key string) string {
	if f.publicBaseURL == "" {
		abs, err := filepath.Abs(f.keyToPath(key))
		if err != nil {
			abs = f.keyToPath(key)
		}
		return "file://" + filepath.ToSlash(abs)
	}
	return f.publicBaseURL + "/" + key
}

// Compile-time check.
var _ ObjectStore = (*FileStore)(nil)
