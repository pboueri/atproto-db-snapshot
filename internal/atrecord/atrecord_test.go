package atrecord

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/model"
)

var refTime = time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)

func TestDecodeFollow(t *testing.T) {
	raw := json.RawMessage(`{"subject":"did:plc:dst","createdAt":"2026-03-30T12:00:00Z"}`)
	got, err := DecodeFollow(raw, "did:plc:src", "rk1", refTime, model.SourceBootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if got.SrcDID != "did:plc:src" || got.DstDID != "did:plc:dst" || got.RKey != "rk1" {
		t.Errorf("decode mismatch: %+v", got)
	}
	if got.SrcDIDID == 0 || got.DstDIDID == 0 {
		t.Errorf("ids not interned")
	}
	if got.CreatedAt.Year() != 2026 {
		t.Errorf("createdAt = %v", got.CreatedAt)
	}
}

func TestDecodeProfile(t *testing.T) {
	raw := json.RawMessage(`{"displayName":"Bob","description":"hi","avatar":{"ref":{"$link":"bafy"},"mimeType":"image/png"},"createdAt":"2026-04-01T00:00:00Z"}`)
	got, err := DecodeProfile(raw, "did:plc:bob", refTime, model.SourceBootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if got.DisplayName != "Bob" {
		t.Errorf("DisplayName = %q", got.DisplayName)
	}
	if got.AvatarCID != "bafy" {
		t.Errorf("AvatarCID = %q", got.AvatarCID)
	}
}

func TestDecodePostWithImages(t *testing.T) {
	raw := json.RawMessage(`{
		"text":"hello",
		"langs":["en","ja"],
		"labels":{"$type":"com.atproto.label.defs#selfLabels","values":[{"val":"adult"}]},
		"embed":{"$type":"app.bsky.embed.images","images":[{"image":{"ref":{"$link":"img1"}},"alt":"x"},{"image":{"ref":{"$link":"img2"}},"alt":""}]},
		"createdAt":"2026-04-01T00:00:00Z"
	}`)
	post, media, err := DecodePost(raw, "did:plc:src", "rk", "cidv", refTime, model.SourceFirehose)
	if err != nil {
		t.Fatal(err)
	}
	if !post.HasMedia {
		t.Errorf("HasMedia = false")
	}
	if got := post.Langs; got != "en,ja" {
		t.Errorf("Langs = %q", got)
	}
	if got := post.Labels; got != "adult" {
		t.Errorf("Labels = %q", got)
	}
	if len(media) != 2 {
		t.Errorf("media len = %d, want 2", len(media))
	}
	if media[0].MediaType != "image" || media[0].BlobCID != "img1" {
		t.Errorf("media[0] = %+v", media[0])
	}
}

func TestDecodePostQuote(t *testing.T) {
	raw := json.RawMessage(`{
		"text":"quote!",
		"embed":{"$type":"app.bsky.embed.record","record":{"uri":"at://did:plc:other/app.bsky.feed.post/zz","cid":"c1"}},
		"createdAt":"2026-04-01T00:00:00Z"
	}`)
	post, media, err := DecodePost(raw, "did:plc:src", "rk", "", refTime, model.SourceFirehose)
	if err != nil {
		t.Fatal(err)
	}
	if post.QuoteParentURI != "at://did:plc:other/app.bsky.feed.post/zz" {
		t.Errorf("QuoteParentURI = %q", post.QuoteParentURI)
	}
	if post.QuoteParentID == 0 {
		t.Errorf("QuoteParentID not interned")
	}
	if len(media) != 0 {
		t.Errorf("expected no media for quote-only embed, got %d", len(media))
	}
}

func TestDecodeLike(t *testing.T) {
	raw := json.RawMessage(`{"subject":{"uri":"at://did:plc:p/app.bsky.feed.post/x","cid":"c"},"createdAt":"2026-04-01T00:00:00Z"}`)
	got, err := DecodeLike(raw, "did:plc:liker", "rk", refTime, model.SourceFirehose)
	if err != nil {
		t.Fatal(err)
	}
	if got.SubjectURI == "" || got.SubjectID == 0 {
		t.Errorf("subject not populated: %+v", got)
	}
}

func TestTIDToTime(t *testing.T) {
	// 3l6oveex3ii2l is the rkey of bsky.app's pinned post; it decodes to a
	// 2024-era microsecond timestamp.
	got, ok := TIDToTime("3l6oveex3ii2l")
	if !ok {
		t.Fatalf("TIDToTime returned ok=false on a valid TID")
	}
	if got.Year() != 2024 {
		t.Errorf("TIDToTime year = %d, want 2024", got.Year())
	}
	if _, ok := TIDToTime(""); ok {
		t.Errorf("empty rkey should return ok=false")
	}
	if _, ok := TIDToTime("not-a-tid-13c"); ok {
		t.Errorf("bad-alphabet rkey should return ok=false")
	}
	if _, ok := TIDToTime("3l6oveex3ii2"); ok {
		t.Errorf("12-char rkey should return ok=false")
	}
}

func TestBuildFollowFromBacklink(t *testing.T) {
	indexedAt := time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC)
	f := BuildFollowFromBacklink("did:plc:src", "did:plc:dst", "3l6oveex3ii2l", indexedAt, model.SourceBootstrap)
	if f.SrcDID != "did:plc:src" || f.DstDID != "did:plc:dst" || f.RKey != "3l6oveex3ii2l" {
		t.Errorf("fields = %+v", f)
	}
	if f.CreatedAt.Year() != 2024 {
		t.Errorf("CreatedAt should derive from TID; got %v", f.CreatedAt)
	}
	if f.SrcDIDID == 0 || f.DstDIDID == 0 {
		t.Errorf("ids not interned")
	}
	// Bad rkey → CreatedAt falls back to indexedAt.
	bad := BuildFollowFromBacklink("did:plc:src", "did:plc:dst", "not-a-tid", indexedAt, model.SourceBootstrap)
	if !bad.CreatedAt.Equal(indexedAt) {
		t.Errorf("bad rkey CreatedAt = %v, want indexedAt fallback", bad.CreatedAt)
	}
}

func TestFlexTimeTolerance(t *testing.T) {
	for _, s := range []string{
		`"2026-04-01T00:00:00Z"`,
		`"2026-04-01T00:00:00.123Z"`,
		`"2026-04-01T00:00:00"`,
		`"not-a-date"`, // tolerated; zero time
		`null`,         // tolerated; zero time
	} {
		var ft flexTime
		if err := ft.UnmarshalJSON([]byte(s)); err != nil {
			t.Errorf("unmarshal %s: %v", s, err)
		}
	}
}
