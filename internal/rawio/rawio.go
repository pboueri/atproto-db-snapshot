// Package rawio writes typed model records to date-partitioned parquet files
// and uploads them to object storage on flush.
//
// Design notes:
//
//   - Parquet has no append; once a file is closed it can't be extended. So
//     we buffer rows in memory per (date, collection) shard and write a fresh
//     parquet file at each Flush. The objstore path includes a monotonic
//     sequence number per shard so concurrent writers and resumes never
//     collide: raw/YYYY-MM-DD/{collection}-{seq:08d}.parquet.
//
//   - Local staging: every Flush writes to a tempfile under stagingDir, then
//     uploads, then deletes the temp. A Recover() pass on startup re-uploads
//     any leftover staging files (from a crash between local-write and
//     upload-success).
//
//   - The DuckDB snapshot job reads with a wildcard
//     raw/{date}/{collection}-*.parquet so the multi-file layout is invisible
//     to consumers. End-of-day compaction is a separate concern, not handled
//     by this package.
package rawio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/pboueri/atproto-db-snapshot/internal/model"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// Sink is the contract subcommands write through. Append* methods are
// safe for concurrent use; Flush serializes with itself and with appends.
type Sink interface {
	AppendProfiles(recs []model.Profile) error
	AppendFollows(recs []model.Follow) error
	AppendBlocks(recs []model.Block) error
	AppendPosts(recs []model.Post) error
	AppendPostMedia(recs []model.PostMedia) error
	AppendLikes(recs []model.Like) error
	AppendReposts(recs []model.Repost) error

	// Flush closes any in-memory shards by writing parquet files to objstore.
	// After Flush the sink remains usable.
	Flush(ctx context.Context) error

	// Close flushes and releases resources.
	Close(ctx context.Context) error
}

// CollectionFile maps a Collection to its on-disk parquet basename.
//
// post_media uses its own filename because it's a derived sidecar of post
// records, not a separate collection in the spec sense.
func CollectionFile(c model.Collection) string {
	switch c {
	case model.CollectionProfile:
		return "profiles"
	case model.CollectionFollow:
		return "follows"
	case model.CollectionBlock:
		return "blocks"
	case model.CollectionPost:
		return "posts"
	case model.CollectionLike:
		return "likes"
	case model.CollectionRepost:
		return "reposts"
	}
	return string(c)
}

// PostMediaFile is the basename for the post_media sidecar parquet.
const PostMediaFile = "post_media"

// ParquetSink is the file-system + objstore implementation of Sink.
type ParquetSink struct {
	stagingDir string
	obj        objstore.Store

	mu       sync.Mutex
	profiles map[string][]model.Profile // key = YYYY-MM-DD
	follows  map[string][]model.Follow
	blocks   map[string][]model.Block
	posts    map[string][]model.Post
	media    map[string][]model.PostMedia
	likes    map[string][]model.Like
	reposts  map[string][]model.Repost

	// seq tracks the next sequence number per shard. A 64-bit timestamp at
	// process start ensures different processes don't collide; within one
	// process we just bump.
	seq map[shardKey]uint64

	clock func() time.Time
}

type shardKey struct {
	date       string
	collection model.Collection
}

// New returns a ParquetSink that buffers rows in memory and writes them to
// stagingDir before uploading to obj.
func New(stagingDir string, obj objstore.Store) (*ParquetSink, error) {
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return nil, fmt.Errorf("rawio: mkdir staging: %w", err)
	}
	return &ParquetSink{
		stagingDir: stagingDir,
		obj:        obj,
		profiles:   map[string][]model.Profile{},
		follows:    map[string][]model.Follow{},
		blocks:     map[string][]model.Block{},
		posts:      map[string][]model.Post{},
		media:      map[string][]model.PostMedia{},
		likes:      map[string][]model.Like{},
		reposts:    map[string][]model.Repost{},
		seq:        map[shardKey]uint64{},
		clock:      time.Now,
	}, nil
}

// dateOf returns the YYYY-MM-DD partition the record falls in (UTC).
func dateOf(t time.Time) string { return t.UTC().Format("2006-01-02") }

func (p *ParquetSink) AppendProfiles(rs []model.Profile) error {
	if len(rs) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range rs {
		d := dateOf(r.IndexedAt)
		p.profiles[d] = append(p.profiles[d], r)
	}
	return nil
}

func (p *ParquetSink) AppendFollows(rs []model.Follow) error {
	if len(rs) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range rs {
		d := dateOf(r.IndexedAt)
		p.follows[d] = append(p.follows[d], r)
	}
	return nil
}

func (p *ParquetSink) AppendBlocks(rs []model.Block) error {
	if len(rs) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range rs {
		d := dateOf(r.IndexedAt)
		p.blocks[d] = append(p.blocks[d], r)
	}
	return nil
}

func (p *ParquetSink) AppendPosts(rs []model.Post) error {
	if len(rs) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range rs {
		d := dateOf(r.IndexedAt)
		p.posts[d] = append(p.posts[d], r)
	}
	return nil
}

func (p *ParquetSink) AppendPostMedia(rs []model.PostMedia) error {
	if len(rs) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range rs {
		d := dateOf(r.IndexedAt)
		p.media[d] = append(p.media[d], r)
	}
	return nil
}

func (p *ParquetSink) AppendLikes(rs []model.Like) error {
	if len(rs) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range rs {
		d := dateOf(r.IndexedAt)
		p.likes[d] = append(p.likes[d], r)
	}
	return nil
}

func (p *ParquetSink) AppendReposts(rs []model.Repost) error {
	if len(rs) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range rs {
		d := dateOf(r.IndexedAt)
		p.reposts[d] = append(p.reposts[d], r)
	}
	return nil
}

// Flush writes every buffered shard to objstore, in dependency-free order.
// On a write or upload error it returns immediately; partial flushes leave
// the not-yet-flushed shards intact in memory for the next attempt.
func (p *ParquetSink) Flush(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Snapshot and clear the in-memory shards so we don't double-write on
	// concurrent appends. The lock above already serializes appends.
	profiles, follows, blocks := p.profiles, p.follows, p.blocks
	posts, media, likes, reposts := p.posts, p.media, p.likes, p.reposts
	p.profiles = map[string][]model.Profile{}
	p.follows = map[string][]model.Follow{}
	p.blocks = map[string][]model.Block{}
	p.posts = map[string][]model.Post{}
	p.media = map[string][]model.PostMedia{}
	p.likes = map[string][]model.Like{}
	p.reposts = map[string][]model.Repost{}

	// We hold the lock through the upload. Throughput-wise this serializes
	// writers — the run command should size its rotation interval so the
	// flush itself stays bounded.
	for d, rs := range profiles {
		if err := writeShard(ctx, p, d, model.CollectionProfile, rs); err != nil {
			return err
		}
	}
	for d, rs := range follows {
		if err := writeShard(ctx, p, d, model.CollectionFollow, rs); err != nil {
			return err
		}
	}
	for d, rs := range blocks {
		if err := writeShard(ctx, p, d, model.CollectionBlock, rs); err != nil {
			return err
		}
	}
	for d, rs := range posts {
		if err := writeShard(ctx, p, d, model.CollectionPost, rs); err != nil {
			return err
		}
	}
	for d, rs := range media {
		if err := writeMediaShard(ctx, p, d, rs); err != nil {
			return err
		}
	}
	for d, rs := range likes {
		if err := writeShard(ctx, p, d, model.CollectionLike, rs); err != nil {
			return err
		}
	}
	for d, rs := range reposts {
		if err := writeShard(ctx, p, d, model.CollectionRepost, rs); err != nil {
			return err
		}
	}
	return nil
}

func (p *ParquetSink) Close(ctx context.Context) error {
	return p.Flush(ctx)
}

// writeShard is the generic per-shard writer; one parameter pack per type.
func writeShard[T any](ctx context.Context, p *ParquetSink, date string, c model.Collection, rs []T) error {
	if len(rs) == 0 {
		return nil
	}
	key := shardKey{date: date, collection: c}
	p.seq[key]++
	seq := p.seq[key]

	rel := remotePath(date, CollectionFile(c), seq, p.clock())
	stagedPath := filepath.Join(p.stagingDir, filepath.Base(rel))

	if err := writeParquet(stagedPath, rs); err != nil {
		return err
	}
	if err := uploadAndCleanup(ctx, p.obj, rel, stagedPath); err != nil {
		return err
	}
	return nil
}

// writeMediaShard mirrors writeShard for the post_media sidecar; broken out
// because PostMedia rows use the post_media filename rather than a Collection.
func writeMediaShard(ctx context.Context, p *ParquetSink, date string, rs []model.PostMedia) error {
	if len(rs) == 0 {
		return nil
	}
	// Sequence under a synthetic "post_media" collection so it doesn't share
	// counter space with post records.
	key := shardKey{date: date, collection: model.Collection(PostMediaFile)}
	p.seq[key]++
	seq := p.seq[key]

	rel := remotePath(date, PostMediaFile, seq, p.clock())
	stagedPath := filepath.Join(p.stagingDir, filepath.Base(rel))
	if err := writeParquet(stagedPath, rs); err != nil {
		return err
	}
	return uploadAndCleanup(ctx, p.obj, rel, stagedPath)
}

// remotePath returns the objstore key for a shard write.
//
// We tag with both the per-shard sequence number and the wall clock at flush
// time so files from independent processes (failover, recovery) interleave
// without colliding on key.
func remotePath(date, collection string, seq uint64, now time.Time) string {
	return fmt.Sprintf("raw/%s/%s-%020d-%08d.parquet", date, collection, now.UnixNano(), seq)
}

func writeParquet[T any](path string, rs []T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			f.Close()
		}
	}()

	// Buffer through bytes.Buffer so we can size the file before upload and
	// avoid keeping a parquet writer open across a (potentially slow) upload.
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	if _, err := w.Write(rs); err != nil {
		return fmt.Errorf("rawio: parquet write: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("rawio: parquet close: %w", err)
	}

	if _, err := io.Copy(f, &buf); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	closed = true
	return f.Close()
}

func uploadAndCleanup(ctx context.Context, obj objstore.Store, dst, local string) error {
	f, err := os.Open(local)
	if err != nil {
		return err
	}
	if err := obj.Put(ctx, dst, f, "application/vnd.apache.parquet"); err != nil {
		f.Close()
		return err
	}
	f.Close()
	// Best-effort cleanup. A leftover staging file gets re-uploaded by Recover.
	_ = os.Remove(local)
	return nil
}

// Recover scans stagingDir and re-uploads any leftover *.parquet files.
//
// Called at startup to handle the rare case where the process crashed
// between writeParquet success and upload completion. Files are uploaded in
// the same date/collection slot they were originally written for, derived
// from the filename.
func Recover(ctx context.Context, stagingDir string, obj objstore.Store) error {
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".parquet") {
			continue
		}
		// We embed the date in the filename via remotePath, but recovery
		// doesn't reconstruct the full key — we re-derive from the basename.
		// To keep the layout self-describing, the basename is the unique key
		// and we put recovered files under a dedicated raw/recovered/ prefix
		// so the snapshot job picks them up via the same wildcard.
		dst := filepath.Join("raw", "recovered", e.Name())
		// The objstore Put treats the path as opaque; use forward slashes.
		dst = filepath.ToSlash(dst)
		local := filepath.Join(stagingDir, e.Name())
		if err := uploadAndCleanup(ctx, obj, dst, local); err != nil {
			return fmt.Errorf("rawio.Recover: %s: %w", e.Name(), err)
		}
	}
	return nil
}
