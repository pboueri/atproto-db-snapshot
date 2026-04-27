package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

// S3 is an S3-compatible Store. It is the backend used in production against
// Cloudflare R2 but works with any provider that speaks the S3 API.
//
// URL() does not return an s3:// URL — DuckDB cannot ATTACH or read_parquet
// from one without the httpfs extension and credentials wired into its own
// session, which the rest of the snapshot pipeline does not configure.
// Instead URL() returns a path inside a per-instance local cache directory,
// and the snapshot package calls Cache(prefix) before resolving paths so the
// cache is populated. This trades startup latency for a uniform interface
// where the rest of the codebase can treat the store as if it were local.
type S3 struct {
	client *s3.Client
	bucket string
	cache  string

	mu     sync.Mutex
	pulled map[string]struct{} // prefixes whose listings have been resolved into the cache
}

// NewS3 builds an S3-backed store from the resolved config.
//
// Credentials are taken from cfg.S3AccessKeyID / cfg.S3SecretAccessKey when
// present; otherwise the standard AWS chain runs (env, shared config, IMDS).
// cfg.S3Endpoint, when set, points the client at an S3-compatible endpoint
// like an R2 account URL; UsePathStyle is enabled so {endpoint}/{bucket}
// addressing works against providers that do not support virtual-hosted
// style.
func NewS3(cfg config.Config) (*S3, error) {
	if cfg.ObjectStoreRoot == "" {
		return nil, fmt.Errorf("objstore.NewS3: bucket name (object-store-root) is required")
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.S3Region),
	}
	if cfg.S3AccessKeyID != "" && cfg.S3SecretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKeyID, cfg.S3SecretAccessKey, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("objstore.NewS3: load aws config: %w", err)
	}

	clientOpts := []func(*s3.Options){
		func(o *s3.Options) { o.UsePathStyle = true },
	}
	if cfg.S3Endpoint != "" {
		ep := cfg.S3Endpoint
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(ep)
		})
	}
	client := s3.NewFromConfig(awsCfg, clientOpts...)

	cacheDir := filepath.Join(cfg.DataDir, "objstore-cache")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("objstore.NewS3: mkdir cache: %w", err)
	}
	return &S3{
		client: client,
		bucket: cfg.ObjectStoreRoot,
		cache:  cacheDir,
		pulled: make(map[string]struct{}),
	}, nil
}

// newS3WithClient constructs an S3 store around an existing client. It is
// used by tests to inject an httptest-backed client without the SDK loader.
func newS3WithClient(client *s3.Client, bucket, cacheDir string) *S3 {
	return &S3{
		client: client,
		bucket: bucket,
		cache:  cacheDir,
		pulled: make(map[string]struct{}),
	}
}

func (s *S3) Put(ctx context.Context, path string, r io.Reader, contentType string) error {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	in := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(path),
		Body:        r,
		ContentType: aws.String(contentType),
	}
	if _, err := s.client.PutObject(ctx, in); err != nil {
		return fmt.Errorf("objstore.S3.Put %s: %w", path, err)
	}
	return nil
}

func (s *S3) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotExist, path)
		}
		return nil, fmt.Errorf("objstore.S3.Get %s: %w", path, err)
	}
	return out.Body, nil
}

func (s *S3) Stat(ctx context.Context, path string) (Object, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		if isS3NotFound(err) {
			return Object{}, fmt.Errorf("%w: %s", ErrNotExist, path)
		}
		return Object{}, fmt.Errorf("objstore.S3.Stat %s: %w", path, err)
	}
	o := Object{Path: path}
	if out.ContentLength != nil {
		o.Size = *out.ContentLength
	}
	if out.LastModified != nil {
		o.LastModified = *out.LastModified
	}
	return o, nil
}

func (s *S3) List(ctx context.Context, prefix string) ([]Object, error) {
	var out []Object
	var token *string
	for {
		page, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("objstore.S3.List %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			o := Object{}
			if obj.Key != nil {
				o.Path = *obj.Key
			}
			if obj.Size != nil {
				o.Size = *obj.Size
			}
			if obj.LastModified != nil {
				o.LastModified = *obj.LastModified
			}
			out = append(out, o)
		}
		if page.IsTruncated == nil || !*page.IsTruncated {
			break
		}
		token = page.NextContinuationToken
	}
	return out, nil
}

func (s *S3) Delete(ctx context.Context, path string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil && !isS3NotFound(err) {
		return fmt.Errorf("objstore.S3.Delete %s: %w", path, err)
	}
	return nil
}

// URL returns the local cache path for a key. The caller is expected to have
// invoked Cache for the key's enclosing prefix first; if it hasn't, the path
// returned will refer to a non-existent file. We deliberately do not perform
// a synchronous download here so callers can pass directory-like paths
// ("raw") that translate to local trees DuckDB will glob over.
func (s *S3) URL(path string) string {
	return filepath.Join(s.cache, filepath.FromSlash(path))
}

// Cache pulls every key under prefix into the local cache directory. It is
// idempotent: a second call for the same prefix is a no-op. Callers that
// need fresh data should call Refresh first.
func (s *S3) Cache(ctx context.Context, prefix string) error {
	s.mu.Lock()
	if _, ok := s.pulled[prefix]; ok {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	objs, err := s.List(ctx, prefix)
	if err != nil {
		return err
	}
	for _, obj := range objs {
		if err := s.downloadIfMissing(ctx, obj.Path); err != nil {
			return err
		}
	}

	s.mu.Lock()
	s.pulled[prefix] = struct{}{}
	s.mu.Unlock()
	return nil
}

// Refresh wipes the local cache so subsequent Cache calls re-download.
func (s *S3) Refresh() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pulled = make(map[string]struct{})
	if err := os.RemoveAll(s.cache); err != nil {
		return fmt.Errorf("objstore.S3.Refresh: %w", err)
	}
	if err := os.MkdirAll(s.cache, 0o755); err != nil {
		return fmt.Errorf("objstore.S3.Refresh: %w", err)
	}
	return nil
}

// downloadIfMissing pulls path into the cache if no file is already there.
// We treat the cache as immutable per Cache call: anyone wanting fresh data
// calls Refresh first.
func (s *S3) downloadIfMissing(ctx context.Context, path string) error {
	dst := filepath.Join(s.cache, filepath.FromSlash(path))
	if _, err := os.Stat(dst); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("objstore.S3.downloadIfMissing stat %s: %w", path, err)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("objstore.S3.downloadIfMissing mkdir %s: %w", path, err)
	}

	rc, err := s.Get(ctx, path)
	if err != nil {
		return err
	}
	defer rc.Close()

	tmp, err := os.CreateTemp(filepath.Dir(dst), ".dl-*")
	if err != nil {
		return fmt.Errorf("objstore.S3.downloadIfMissing create %s: %w", path, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := io.Copy(tmp, rc); err != nil {
		tmp.Close()
		return fmt.Errorf("objstore.S3.downloadIfMissing copy %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("objstore.S3.downloadIfMissing close %s: %w", path, err)
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return fmt.Errorf("objstore.S3.downloadIfMissing rename %s: %w", path, err)
	}
	slog.Debug("objstore.S3 cached", "key", path, "dst", dst)
	return nil
}

// isS3NotFound returns true for the error variants S3 / R2 use to signal a
// missing key. HeadObject reports NotFound; GetObject reports NoSuchKey;
// some endpoints fall back to a generic 404 without typing.
func isS3NotFound(err error) bool {
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *s3types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound", "404":
			return true
		}
	}
	// Some HTTP-only mocks surface a bare *http.Response; check the status
	// code as a last resort for hermetic tests.
	if strings.Contains(err.Error(), fmt.Sprintf("StatusCode: %d", http.StatusNotFound)) {
		return true
	}
	return false
}
