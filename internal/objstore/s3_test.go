package objstore

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeS3 is a tiny in-memory S3-compatible stand-in that implements just the
// PUT / GET / HEAD / DELETE / LIST verbs the S3 backend uses. It is not a
// faithful S3 implementation — payload signing, range requests, and
// multipart uploads are out of scope. The goal is to exercise our wrapper.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	mtimes  map[string]time.Time
}

func newFakeS3() *fakeS3 {
	return &fakeS3{
		objects: make(map[string][]byte),
		mtimes:  make(map[string]time.Time),
	}
}

func (f *fakeS3) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Path-style routing: /{bucket}/{key...}
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		var key string
		if len(parts) == 2 {
			key = parts[1]
		}

		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			f.mu.Lock()
			f.objects[key] = body
			f.mtimes[key] = time.Now().UTC().Truncate(time.Second)
			f.mu.Unlock()
			w.Header().Set("ETag", `"deadbeef"`)
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			if r.URL.Query().Get("list-type") == "2" {
				f.handleList(w, r)
				return
			}
			f.mu.Lock()
			body, ok := f.objects[key]
			mt := f.mtimes[key]
			f.mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
			w.Header().Set("Last-Modified", mt.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)

		case http.MethodHead:
			f.mu.Lock()
			body, ok := f.objects[key]
			mt := f.mtimes[key]
			f.mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
			w.Header().Set("Last-Modified", mt.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)

		case http.MethodDelete:
			f.mu.Lock()
			delete(f.objects, key)
			delete(f.mtimes, key)
			f.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func (f *fakeS3) handleList(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	type respContents struct {
		Key          string `xml:"Key"`
		Size         int64  `xml:"Size"`
		LastModified string `xml:"LastModified"`
	}
	type respBody struct {
		XMLName     xml.Name       `xml:"ListBucketResult"`
		Name        string         `xml:"Name"`
		Prefix      string         `xml:"Prefix"`
		KeyCount    int            `xml:"KeyCount"`
		MaxKeys     int            `xml:"MaxKeys"`
		IsTruncated bool           `xml:"IsTruncated"`
		Contents    []respContents `xml:"Contents"`
	}
	out := respBody{Name: "test", Prefix: prefix, MaxKeys: 1000}

	f.mu.Lock()
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		out.Contents = append(out.Contents, respContents{
			Key:          k,
			Size:         int64(len(f.objects[k])),
			LastModified: f.mtimes[k].Format(time.RFC3339),
		})
	}
	f.mu.Unlock()
	out.KeyCount = len(out.Contents)

	w.Header().Set("Content-Type", "application/xml")
	if err := xml.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// newTestS3 returns an S3 store whose client targets the given test server.
func newTestS3(t *testing.T, srvURL, bucket string) *S3 {
	t.Helper()
	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srvURL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
	cacheDir := filepath.Join(t.TempDir(), "objstore-cache")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatal(err)
	}
	return newS3WithClient(client, bucket, cacheDir)
}

// TestS3Conformance runs the shared store suite against the httptest-backed
// fake. URL semantics differ from local (cache directory rather than the
// S3 keyspace) so the conformance suite's URL-related checks aren't applied
// here — runStoreSuite only exercises Put/Get/Stat/List/Delete.
func TestS3Conformance(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake.handler(t))
	defer srv.Close()

	store := newTestS3(t, srv.URL, "test")
	runStoreSuite(t, store)
}

// TestS3CacheRoundTrip verifies the download-to-cache behavior: the first
// URL() call against a cached prefix returns a populated local file, a
// second call hits the cache without touching the remote, and Refresh
// causes a re-download.
func TestS3CacheRoundTrip(t *testing.T) {
	fake := newFakeS3()
	var gets int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") != "2" {
			gets++
		}
		fake.handler(t).ServeHTTP(w, r)
	}))
	defer srv.Close()

	store := newTestS3(t, srv.URL, "test")
	ctx := context.Background()

	if err := store.Put(ctx, "raw/2026-04-26/posts.parquet", bytes.NewReader([]byte("hello")), ""); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Put(ctx, "raw/2026-04-27/posts.parquet", bytes.NewReader([]byte("world")), ""); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Cache the prefix; both files should land in the local cache directory.
	if err := store.Cache(ctx, "raw/"); err != nil {
		t.Fatalf("Cache: %v", err)
	}
	if gets != 2 {
		t.Errorf("after first Cache, GETs = %d, want 2", gets)
	}

	for _, key := range []string{"raw/2026-04-26/posts.parquet", "raw/2026-04-27/posts.parquet"} {
		path := store.URL(key)
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("expected cached file at %s: %v", path, err)
		}
		if len(body) == 0 {
			t.Errorf("cached file %s is empty", path)
		}
	}

	// Second cache call hits the bookkeeping shortcut and doesn't re-GET.
	if err := store.Cache(ctx, "raw/"); err != nil {
		t.Fatalf("Cache (second): %v", err)
	}
	if gets != 2 {
		t.Errorf("second Cache call re-fetched; GETs = %d, want still 2", gets)
	}

	// Refresh wipes the cache; the next Cache call re-downloads.
	if err := store.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if err := store.Cache(ctx, "raw/"); err != nil {
		t.Fatalf("Cache (after refresh): %v", err)
	}
	if gets != 4 {
		t.Errorf("after Refresh+Cache, GETs = %d, want 4", gets)
	}
}

// TestS3GetMissing confirms ErrNotExist is returned for missing keys.
func TestS3GetMissing(t *testing.T) {
	fake := newFakeS3()
	srv := httptest.NewServer(fake.handler(t))
	defer srv.Close()

	store := newTestS3(t, srv.URL, "test")
	ctx := context.Background()

	if _, err := store.Get(ctx, "no/such/key"); !errors.Is(err, ErrNotExist) {
		t.Errorf("Get(missing) err = %v, want ErrNotExist", err)
	}
	if _, err := store.Stat(ctx, "no/such/key"); !errors.Is(err, ErrNotExist) {
		t.Errorf("Stat(missing) err = %v, want ErrNotExist", err)
	}
	// Delete on missing is not an error.
	if err := store.Delete(ctx, "no/such/key"); err != nil {
		t.Errorf("Delete(missing) err = %v, want nil", err)
	}
}

// envOrDefault is shared with objstore_test.go's table-driven s3 conformance
// case to keep env-name parsing in one spot.
func envOrDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
