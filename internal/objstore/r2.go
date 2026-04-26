package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
)

// R2Config configures an R2-backed ObjectStore. It also works for any
// S3-compatible service if you point Endpoint at it.
type R2Config struct {
	Bucket          string
	Endpoint        string // https://<account>.r2.cloudflarestorage.com
	AccessKeyID     string
	SecretAccessKey string
	PublicURLBase   string // https://pub-<hash>.r2.dev
	Region          string // "auto" for R2; default "auto" if empty
}

// R2Store implements ObjectStore against Cloudflare R2 (or any
// S3-compatible service) using the modern aws-sdk-go-v2 client with a
// custom BaseEndpoint and path-style addressing.
type R2Store struct {
	cfg      R2Config
	client   *s3.Client
	uploader *manager.Uploader
}

// uploadPartThreshold is the multipart threshold; objects larger than this
// are uploaded via the SDK's parallel uploader.
const uploadPartThreshold = 5 * 1024 * 1024 // 5 MB

// NewR2Store builds an R2Store. If AccessKeyID/SecretAccessKey are empty,
// the constructor falls back to the env vars OS_ACCESS_KEY / OS_SECRET_KEY
// (the names spelled in the spec's config.yaml). If Region is empty it
// defaults to "auto", which is correct for R2.
func NewR2Store(ctx context.Context, cfg R2Config) (*R2Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("objstore: R2Config.Bucket is required")
	}
	if cfg.Endpoint == "" {
		return nil, errors.New("objstore: R2Config.Endpoint is required")
	}
	if cfg.Region == "" {
		cfg.Region = "auto"
	}
	if cfg.AccessKeyID == "" {
		cfg.AccessKeyID = os.Getenv("OS_ACCESS_KEY")
	}
	if cfg.SecretAccessKey == "" {
		cfg.SecretAccessKey = os.Getenv("OS_SECRET_KEY")
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, errors.New("objstore: R2 credentials missing (set R2Config or OS_ACCESS_KEY/OS_SECRET_KEY)")
	}

	awsCfg := aws.Config{
		Region: cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID, cfg.SecretAccessKey, "",
		),
	}

	endpoint := cfg.Endpoint
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = &endpoint
	})

	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = manager.DefaultUploadPartSize
		u.Concurrency = manager.DefaultUploadConcurrency
	})

	return &R2Store{cfg: cfg, client: client, uploader: uploader}, nil
}

// Exists implements ObjectStore via HeadObject, treating S3's NotFound /
// NoSuchKey responses (and an HTTP 404 wrapped in a smithy ResponseError)
// as a clean false return.
func (r *R2Store) Exists(ctx context.Context, key string) (bool, error) {
	_, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &r.cfg.Bucket,
		Key:    &key,
	})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("objstore: head %q: %w", key, err)
}

// Get streams the object body to w.
func (r *R2Store) Get(ctx context.Context, key string, w io.Writer) error {
	out, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &r.cfg.Bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("objstore: get %q: %w", key, err)
	}
	defer out.Body.Close()
	if _, err := io.Copy(w, out.Body); err != nil {
		return fmt.Errorf("objstore: read body %q: %w", key, err)
	}
	return nil
}

// PutAtomic uploads `r` to `key.new`, then server-side copies to `key`,
// then deletes `key.new`. Readers of `key` therefore always see either
// the previous complete object or the new complete object — never a torn
// upload. Objects are uploaded with the SDK's manager.Uploader, which
// transparently switches to multipart for large payloads.
func (r *R2Store) PutAtomic(ctx context.Context, key string, body io.Reader, size int64) error {
	tmpKey := key + ".new"

	if _, err := r.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &r.cfg.Bucket,
		Key:    &tmpKey,
		Body:   body,
	}); err != nil {
		return fmt.Errorf("objstore: upload %q: %w", tmpKey, err)
	}

	// Server-side copy. The CopySource must be URL-escaped per S3's wire
	// format: "<bucket>/<urlencoded-key>".
	copySource := r.cfg.Bucket + "/" + url.PathEscape(tmpKey)
	if _, err := r.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     &r.cfg.Bucket,
		Key:        &key,
		CopySource: &copySource,
	}); err != nil {
		// Best-effort cleanup of the staged object so we don't leak.
		_, _ = r.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &r.cfg.Bucket, Key: &tmpKey,
		})
		return fmt.Errorf("objstore: copy %q -> %q: %w", tmpKey, key, err)
	}

	if _, err := r.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &r.cfg.Bucket,
		Key:    &tmpKey,
	}); err != nil {
		// The atomic publish still succeeded; surface a soft warning by
		// returning a wrapped error. Callers that care can inspect.
		return fmt.Errorf("objstore: cleanup %q: %w", tmpKey, err)
	}

	return nil
}

// List returns every object under prefix, paginating through ListObjectsV2.
func (r *R2Store) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var out []ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(r.client, &s3.ListObjectsV2Input{
		Bucket: &r.cfg.Bucket,
		Prefix: &prefix,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("objstore: list %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			oi := ObjectInfo{}
			if obj.Key != nil {
				oi.Key = *obj.Key
			}
			if obj.Size != nil {
				oi.Size = *obj.Size
			}
			if obj.LastModified != nil {
				oi.LastModified = *obj.LastModified
			}
			out = append(out, oi)
		}
	}
	return out, nil
}

// Delete removes a single object. Deleting a non-existent key is a no-op
// at the S3 protocol level, matching FileStore semantics.
func (r *R2Store) Delete(ctx context.Context, key string) error {
	_, err := r.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &r.cfg.Bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("objstore: delete %q: %w", key, err)
	}
	return nil
}

// PublicURL composes the public-facing URL using PublicURLBase. It does
// not URL-encode the key so callers can use it verbatim with hierarchical
// keys like "daily/2026-04-24/posts.parquet".
func (r *R2Store) PublicURL(key string) string {
	base := strings.TrimRight(r.cfg.PublicURLBase, "/")
	return base + "/" + key
}

// isNotFound returns true for the various flavors of "object missing"
// that S3/R2 can surface.
func isNotFound(err error) bool {
	var nf *s3types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	// HeadObject returns the generic API error code "NotFound" wrapped in
	// a smithy.GenericAPIError rather than a typed *NotFound, so check by
	// error code as well.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey":
			return true
		}
	}
	return false
}

// Compile-time check.
var _ ObjectStore = (*R2Store)(nil)
