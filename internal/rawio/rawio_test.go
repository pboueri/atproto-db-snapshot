package rawio

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/pboueri/atproto-db-snapshot/internal/model"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

func TestParquetSinkRoundTrip(t *testing.T) {
	obj := objstore.NewMemory()
	sink, err := New(t.TempDir(), obj)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	follows := []model.Follow{
		{SrcDID: "did:plc:a", SrcDIDID: 1, DstDID: "did:plc:b", DstDIDID: 2, RKey: "f1", CreatedAt: now, IndexedAt: now, Op: model.OpCreate, Source: model.SourceFirehose},
		{SrcDID: "did:plc:c", SrcDIDID: 3, DstDID: "did:plc:d", DstDIDID: 4, RKey: "f2", CreatedAt: now, IndexedAt: now.Add(time.Hour), Op: model.OpCreate, Source: model.SourceFirehose},
	}
	if err := sink.AppendFollows(follows); err != nil {
		t.Fatalf("AppendFollows: %v", err)
	}
	ctx := context.Background()
	if err := sink.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Both rows fall on 2026-04-26 UTC; expect a single shard file.
	objs, err := obj.List(ctx, "raw/2026-04-26/follows-")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 1 {
		t.Fatalf("expected 1 follows file, got %d", len(objs))
	}

	rc, err := obj.Get(ctx, objs[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	r := parquet.NewGenericReader[model.Follow](bytes.NewReader(data))
	defer r.Close()
	got := make([]model.Follow, 4)
	n, err := r.Read(got)
	if err != nil && err != io.EOF {
		t.Fatalf("parquet read: %v", err)
	}
	got = got[:n]
	if len(got) != 2 {
		t.Errorf("read %d, want 2", len(got))
	}
	if got[0].SrcDID != "did:plc:a" {
		t.Errorf("got %+v", got[0])
	}
}

func TestParquetSinkPartitionsByDate(t *testing.T) {
	obj := objstore.NewMemory()
	sink, err := New(t.TempDir(), obj)
	if err != nil {
		t.Fatal(err)
	}
	day1 := time.Date(2026, 4, 26, 23, 59, 0, 0, time.UTC)
	day2 := time.Date(2026, 4, 27, 0, 1, 0, 0, time.UTC)
	if err := sink.AppendLikes([]model.Like{
		{ActorDID: "a", SubjectURI: "x", IndexedAt: day1, Op: model.OpCreate},
		{ActorDID: "b", SubjectURI: "y", IndexedAt: day2, Op: model.OpCreate},
	}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	o1, _ := obj.List(context.Background(), "raw/2026-04-26/likes-")
	o2, _ := obj.List(context.Background(), "raw/2026-04-27/likes-")
	if len(o1) != 1 || len(o2) != 1 {
		t.Errorf("partitioning broken: day1=%d day2=%d", len(o1), len(o2))
	}
}

func TestParquetSinkSequencesIncrement(t *testing.T) {
	obj := objstore.NewMemory()
	sink, err := New(t.TempDir(), obj)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		if err := sink.AppendPosts([]model.Post{
			{URI: "at://x/" + string(rune('a'+i)), IndexedAt: day, CreatedAt: day, Op: model.OpCreate},
		}); err != nil {
			t.Fatal(err)
		}
		if err := sink.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	objs, _ := obj.List(context.Background(), "raw/2026-04-26/posts-")
	if len(objs) != 3 {
		t.Errorf("got %d post files, want 3 (one per flush)", len(objs))
	}
	// Each filename should contain a unique seq fragment; we just check uniqueness.
	seen := map[string]bool{}
	for _, o := range objs {
		base := o.Path[strings.LastIndex(o.Path, "/")+1:]
		if seen[base] {
			t.Errorf("duplicate filename: %s", base)
		}
		seen[base] = true
	}
}

func TestRecoverReuploadsLeftovers(t *testing.T) {
	dir := t.TempDir()
	obj := objstore.NewMemory()
	sink, err := New(dir, obj)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a left-behind staging file by writing parquet directly without
	// calling Flush. We write to dir/orphan.parquet using the same pipeline.
	day := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	rs := []model.Profile{{DID: "did:plc:x", DIDID: 1, IndexedAt: day, CreatedAt: day, Op: model.OpCreate}}
	orphan := dir + "/orphan-2026-04-26.parquet"
	if err := writeParquet(orphan, rs); err != nil {
		t.Fatal(err)
	}
	if err := Recover(context.Background(), dir, obj); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	objs, _ := obj.List(context.Background(), "raw/recovered/")
	if len(objs) != 1 {
		t.Errorf("recovered %d files, want 1", len(objs))
	}
	_ = sink
}
