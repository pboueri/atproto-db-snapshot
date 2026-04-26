package objstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// Memory is an in-process Store useful for unit tests that don't need to
// exercise external tooling. It is not suitable for DuckDB integration —
// snapshot tests should use Local pointing at t.TempDir() instead.
type Memory struct {
	mu      sync.RWMutex
	objects map[string]memObject
}

type memObject struct {
	data    []byte
	modTime time.Time
}

func NewMemory() *Memory {
	return &Memory{objects: make(map[string]memObject)}
}

func (m *Memory) Put(ctx context.Context, path string, r io.Reader, _ string) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[path] = memObject{data: b, modTime: time.Now()}
	return nil
}

func (m *Memory) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	o, ok := m.objects[path]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotExist, path)
	}
	return io.NopCloser(bytes.NewReader(o.data)), nil
}

func (m *Memory) Stat(ctx context.Context, path string) (Object, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	o, ok := m.objects[path]
	if !ok {
		return Object{}, fmt.Errorf("%w: %s", ErrNotExist, path)
	}
	return Object{Path: path, Size: int64(len(o.data)), LastModified: o.modTime}, nil
}

func (m *Memory) List(ctx context.Context, prefix string) ([]Object, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []Object
	for p, o := range m.objects {
		if strings.HasPrefix(p, prefix) {
			out = append(out, Object{Path: p, Size: int64(len(o.data)), LastModified: o.modTime})
		}
	}
	return out, nil
}

func (m *Memory) Delete(ctx context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, path)
	return nil
}

// URL returns a synthetic mem:// path so callers can log it. External tools
// cannot dereference these — see the package doc.
func (m *Memory) URL(path string) string { return "mem://" + path }
