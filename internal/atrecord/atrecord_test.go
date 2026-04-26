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
