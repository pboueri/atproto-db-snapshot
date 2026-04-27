package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

// TestStoreConformance runs the same suite against every backend.
// New backends drop in by adding to the table.
func TestStoreConformance(t *testing.T) {
	cases := []struct {
		name string
		make func(t *testing.T) Store
	}{
		{"local", func(t *testing.T) Store {
			s, err := NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return s
		}},
		{"memory", func(t *testing.T) Store { return NewMemory() }},
		{"s3", func(t *testing.T) Store {
			if os.Getenv("AT_SNAPSHOT_S3_TEST") != "1" {
				t.Skip("AT_SNAPSHOT_S3_TEST not set; skipping live S3 conformance")
			}
			cfg := config.Config{
				DataDir:           t.TempDir(),
				ObjectStore:       "s3",
				ObjectStoreRoot:   os.Getenv("AT_SNAPSHOT_OBJECT_STORE_ROOT"),
				S3Endpoint:        os.Getenv("AT_SNAPSHOT_S3_ENDPOINT"),
				S3Region:          envOrDefault("AT_SNAPSHOT_S3_REGION", "auto"),
				S3AccessKeyID:     os.Getenv("AT_SNAPSHOT_S3_ACCESS_KEY_ID"),
				S3SecretAccessKey: os.Getenv("AT_SNAPSHOT_S3_SECRET_ACCESS_KEY"),
			}
			if cfg.ObjectStoreRoot == "" {
				t.Skip("AT_SNAPSHOT_OBJECT_STORE_ROOT (bucket) required")
			}
			s, err := NewS3(cfg)
			if err != nil {
				t.Fatal(err)
			}
			return s
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runStoreSuite(t, tc.make(t))
		})
	}
}

func runStoreSuite(t *testing.T, s Store) {
	ctx := context.Background()

	// Put then Get round-trip.
	body := []byte("hello world")
	if err := s.Put(ctx, "raw/2026-04-26/posts.parquet", bytes.NewReader(body), "application/octet-stream"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, err := s.Get(ctx, "raw/2026-04-26/posts.parquet")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("Get returned %q, want %q", got, body)
	}

	// Stat reports correct size.
	o, err := s.Stat(ctx, "raw/2026-04-26/posts.parquet")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if o.Size != int64(len(body)) {
		t.Errorf("Stat.Size = %d, want %d", o.Size, len(body))
	}

	// Stat on missing object yields ErrNotExist.
	if _, err := s.Stat(ctx, "raw/missing"); !errors.Is(err, ErrNotExist) {
		t.Errorf("Stat(missing) err = %v, want ErrNotExist", err)
	}
	if _, err := s.Get(ctx, "raw/missing"); !errors.Is(err, ErrNotExist) {
		t.Errorf("Get(missing) err = %v, want ErrNotExist", err)
	}

	// Put a few more, then List filters by prefix.
	if err := s.Put(ctx, "raw/2026-04-26/likes.parquet", strings.NewReader("a"), ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "raw/2026-04-27/posts.parquet", strings.NewReader("b"), ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "snapshot/current_all.duckdb", strings.NewReader("c"), ""); err != nil {
		t.Fatal(err)
	}

	listed, err := s.List(ctx, "raw/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	paths := make([]string, len(listed))
	for i, o := range listed {
		paths[i] = o.Path
	}
	sort.Strings(paths)
	want := []string{"raw/2026-04-26/likes.parquet", "raw/2026-04-26/posts.parquet", "raw/2026-04-27/posts.parquet"}
	if !equalSlice(paths, want) {
		t.Errorf("List(raw/) = %v, want %v", paths, want)
	}

	// Delete is idempotent.
	if err := s.Delete(ctx, "raw/2026-04-27/posts.parquet"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete(ctx, "raw/2026-04-27/posts.parquet"); err != nil {
		t.Fatalf("Delete idempotent: %v", err)
	}
	if _, err := s.Stat(ctx, "raw/2026-04-27/posts.parquet"); !errors.Is(err, ErrNotExist) {
		t.Errorf("post-delete stat err = %v, want ErrNotExist", err)
	}
}

// TestLocalContainsTraversal verifies that a malicious-looking path is
// normalized to land inside the root rather than escaping. We don't reject
// these outright because filepath.Clean already neuters them; we just
// guarantee they can't write to /etc/passwd.
func TestLocalContainsTraversal(t *testing.T) {
	root := t.TempDir()
	s, err := NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.Put(ctx, "../../etc/passwd", strings.NewReader("nope"), ""); err != nil {
		t.Fatalf("Put: %v", err)
	}
	url := s.URL("../../etc/passwd")
	if !strings.HasPrefix(url, root) {
		t.Errorf("URL %q escaped root %q", url, root)
	}
}

func equalSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
