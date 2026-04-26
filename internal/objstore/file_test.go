package objstore

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root, "https://example.test/data")

	const key = "daily/2026-04-24/posts.parquet"
	payload := []byte("hello, atproto snapshot world")

	// Initially absent.
	if ok, err := store.Exists(ctx, key); err != nil {
		t.Fatalf("Exists before put: %v", err)
	} else if ok {
		t.Fatalf("Exists before put: got true, want false")
	}

	// Put.
	if err := store.PutAtomic(ctx, key, bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatalf("PutAtomic: %v", err)
	}

	// Exists now true.
	if ok, err := store.Exists(ctx, key); err != nil {
		t.Fatalf("Exists after put: %v", err)
	} else if !ok {
		t.Fatalf("Exists after put: got false, want true")
	}

	// Get round-trips.
	var got bytes.Buffer
	if err := store.Get(ctx, key, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("Get: payload mismatch\n got=%q\nwant=%q", got.Bytes(), payload)
	}

	// List under the day prefix returns exactly the one entry, with a
	// forward-slash key relative to the root.
	infos, err := store.List(ctx, "daily/2026-04-24")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(infos) != 1 {
		t.Fatalf("List: got %d entries, want 1: %+v", len(infos), infos)
	}
	if infos[0].Key != key {
		t.Fatalf("List: got key %q, want %q", infos[0].Key, key)
	}
	if infos[0].Size != int64(len(payload)) {
		t.Fatalf("List: got size %d, want %d", infos[0].Size, len(payload))
	}
	if strings.Contains(infos[0].Key, "\\") {
		t.Fatalf("List: key contains backslash on this platform: %q", infos[0].Key)
	}

	// List over the root walks everything.
	all, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List root: %v", err)
	}
	if len(all) != 1 || all[0].Key != key {
		t.Fatalf("List root: got %+v, want single %q", all, key)
	}

	// PublicURL composes correctly.
	if got, want := store.PublicURL(key), "https://example.test/data/"+key; got != want {
		t.Fatalf("PublicURL: got %q, want %q", got, want)
	}

	// PublicURL with empty base falls back to file:// scheme.
	store2 := NewFileStore(root, "")
	url := store2.PublicURL(key)
	if !strings.HasPrefix(url, "file://") {
		t.Fatalf("PublicURL fallback: got %q, want file:// prefix", url)
	}
	if !strings.HasSuffix(url, "/"+filepath.ToSlash(key)) {
		t.Fatalf("PublicURL fallback: got %q, missing key suffix", url)
	}

	// Delete makes it disappear.
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if ok, err := store.Exists(ctx, key); err != nil {
		t.Fatalf("Exists after delete: %v", err)
	} else if ok {
		t.Fatalf("Exists after delete: got true, want false")
	}

	// Delete of a missing key is a no-op.
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete (missing): want nil, got %v", err)
	}

	// List of a missing prefix is empty, not an error.
	got2, err := store.List(ctx, "daily/2099-01-01")
	if err != nil {
		t.Fatalf("List missing: %v", err)
	}
	if len(got2) != 0 {
		t.Fatalf("List missing: got %d entries, want 0", len(got2))
	}
}

func TestFileStorePutAtomicCleansTmp(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root, "")

	const key = "x/y/z.bin"
	payload := []byte("payload")
	if err := store.PutAtomic(ctx, key, bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatalf("PutAtomic: %v", err)
	}

	// After a successful PutAtomic the .new file must be gone (it was
	// renamed into place).
	tmpPath := filepath.Join(root, "x", "y", "z.bin.new")
	if _, err := os.Stat(tmpPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected %q to be absent after rename, got err=%v", tmpPath, err)
	}
}
