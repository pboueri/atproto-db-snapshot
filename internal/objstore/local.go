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

// Local is a filesystem-backed Store rooted at a directory.
//
// Writes go to a tempfile in the same directory and atomically rename into
// place, so a crashed mid-Put leaves no half-written file for the next run to
// trip over.
type Local struct {
	root string
}

// NewLocal creates the directory if needed and returns a Local store rooted there.
func NewLocal(root string) (*Local, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("objstore.NewLocal: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("objstore.NewLocal: %w", err)
	}
	return &Local{root: abs}, nil
}

// resolve returns the absolute on-disk path for an object path, after rejecting
// traversal attempts ("../foo") that would escape the root.
func (l *Local) resolve(path string) (string, error) {
	clean := filepath.Clean("/" + path) // forces leading slash so .. can't go above
	if clean == "/" || clean == "." {
		return "", fmt.Errorf("invalid empty path")
	}
	full := filepath.Join(l.root, clean)
	rel, err := filepath.Rel(l.root, full)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("path escapes root: %q", path)
	}
	return full, nil
}

func (l *Local) Put(ctx context.Context, path string, r io.Reader, _ string) error {
	full, err := l.resolve(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(full), ".put-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // best-effort cleanup if rename fails

	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, full)
}

func (l *Local) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	full, err := l.resolve(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(full)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNotExist, path)
	}
	return f, err
}

func (l *Local) Stat(ctx context.Context, path string) (Object, error) {
	full, err := l.resolve(path)
	if err != nil {
		return Object{}, err
	}
	info, err := os.Stat(full)
	if errors.Is(err, fs.ErrNotExist) {
		return Object{}, fmt.Errorf("%w: %s", ErrNotExist, path)
	}
	if err != nil {
		return Object{}, err
	}
	return Object{Path: path, Size: info.Size(), LastModified: info.ModTime()}, nil
}

func (l *Local) List(ctx context.Context, prefix string) ([]Object, error) {
	// We list every file under root and filter by prefix string. The dataset
	// is small enough (date-partitioned by day, ~ten files per day) that the
	// simpler approach beats walking only matching subtrees.
	var out []Object
	err := filepath.WalkDir(l.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(l.root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if !strings.HasPrefix(rel, prefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		out = append(out, Object{Path: rel, Size: info.Size(), LastModified: info.ModTime()})
		return nil
	})
	return out, err
}

func (l *Local) Delete(ctx context.Context, path string) error {
	full, err := l.resolve(path)
	if err != nil {
		return err
	}
	if err := os.Remove(full); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// URL returns the absolute filesystem path for use by external tools (DuckDB).
func (l *Local) URL(path string) string {
	full, err := l.resolve(path)
	if err != nil {
		// Resolve only fails on traversal attempts; callers shouldn't pass
		// those, but if they do we want the failure to surface at use rather
		// than silently constructing a wrong path.
		return ""
	}
	return full
}
