package parse

import (
	"testing"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
)

func TestSplitATURI(t *testing.T) {
	cases := []struct {
		name       string
		uri        string
		wantAuthor string
		wantRkey   string
	}{
		{"valid_post",
			"at://did:plc:abc/app.bsky.feed.post/3rkey",
			"did:plc:abc", "3rkey"},
		{"valid_like",
			"at://did:plc:xyz/app.bsky.feed.like/3jzp2qpyvak2a",
			"did:plc:xyz", "3jzp2qpyvak2a"},
		{"did_web",
			"at://did:web:example.com/app.bsky.graph.follow/3zzz",
			"did:web:example.com", "3zzz"},
		{"rkey_with_slashes_not_supported_but_takes_remainder",
			"at://did:plc:abc/app.bsky.feed.post/a/b/c",
			// SplitN(rest, "/", 3) gives ["did:plc:abc", "app.bsky.feed.post", "a/b/c"].
			"did:plc:abc", "a/b/c"},
		{"missing_prefix", "http://did:plc:abc/a/b", "", ""},
		{"empty", "", "", ""},
		{"only_scheme", "at://", "", ""},
		{"missing_collection", "at://did:plc:abc", "", ""},
		{"missing_rkey", "at://did:plc:abc/app.bsky.feed.post", "", ""},
		{"empty_parts", "at:///foo/bar", "", "bar"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotAuthor, gotRkey := splitATURI(tc.uri)
			if gotAuthor != tc.wantAuthor || gotRkey != tc.wantRkey {
				t.Fatalf("splitATURI(%q) = (%q, %q); want (%q, %q)",
					tc.uri, gotAuthor, gotRkey, tc.wantAuthor, tc.wantRkey)
			}
		})
	}
}

func TestExtractHost(t *testing.T) {
	cases := []struct {
		uri  string
		want string
	}{
		{"https://example.com/foo", "example.com"},
		{"http://sub.example.com:8080/x?y=1", "sub.example.com:8080"},
		{"https://example.com", "example.com"},
		{"https://example.com#frag", "example.com"},
		{"ftp://example.com/x", ""},
		{"", ""},
		{"not-a-url", ""},
	}
	for _, tc := range cases {
		if got := extractHost(tc.uri); got != tc.want {
			t.Errorf("extractHost(%q) = %q; want %q", tc.uri, got, tc.want)
		}
	}
}

// TestConvertPostEmbeds covers each FeedPost.Embed shape per spec 002 §1.1.
// The parser is the source of truth for the structured fields downstream
// writers consume — the unwrap logic for recordWithMedia in particular has
// historically been easy to get wrong.
func TestConvertPostEmbeds(t *testing.T) {
	const rkey = "3rkey"
	altA := "alt for image"
	altVid := "alt for video"
	mkExternal := func(uri, title string) *bsky.EmbedExternal {
		return &bsky.EmbedExternal{External: &bsky.EmbedExternal_External{
			Uri:   uri,
			Title: title,
		}}
	}

	t.Run("images_with_alt", func(t *testing.T) {
		p := convertPost(rkey, &bsky.FeedPost{
			Text:      "with images",
			CreatedAt: "2026-04-24T12:00:00Z",
			Embed: &bsky.FeedPost_Embed{EmbedImages: &bsky.EmbedImages{
				Images: []*bsky.EmbedImages_Image{
					{Alt: altA},
					{Alt: ""},
					{Alt: "  "}, // whitespace counts as no alt
				},
			}},
		})
		if p.EmbedType != "images" || p.EmbedKind != "images" {
			t.Fatalf("type/kind mismatch: type=%q kind=%q", p.EmbedType, p.EmbedKind)
		}
		if p.EmbedImageCount != 3 || p.EmbedImageWithAlt != 1 {
			t.Fatalf("image counts wrong: count=%d with_alt=%d", p.EmbedImageCount, p.EmbedImageWithAlt)
		}
	})

	t.Run("video_with_alt", func(t *testing.T) {
		p := convertPost(rkey, &bsky.FeedPost{
			Text:      "video",
			CreatedAt: "2026-04-24T12:00:00Z",
			Embed: &bsky.FeedPost_Embed{EmbedVideo: &bsky.EmbedVideo{
				Alt: &altVid,
			}},
		})
		if p.EmbedKind != "video" || !p.EmbedHasVideo || !p.EmbedVideoHasAlt {
			t.Fatalf("video flags wrong: kind=%q has_video=%v has_alt=%v",
				p.EmbedKind, p.EmbedHasVideo, p.EmbedVideoHasAlt)
		}
	})

	t.Run("video_no_alt", func(t *testing.T) {
		p := convertPost(rkey, &bsky.FeedPost{
			Text:      "video, no alt",
			CreatedAt: "2026-04-24T12:00:00Z",
			Embed:     &bsky.FeedPost_Embed{EmbedVideo: &bsky.EmbedVideo{}},
		})
		if !p.EmbedHasVideo || p.EmbedVideoHasAlt {
			t.Fatalf("video w/o alt flags wrong: has_video=%v has_alt=%v",
				p.EmbedHasVideo, p.EmbedVideoHasAlt)
		}
	})

	t.Run("external", func(t *testing.T) {
		p := convertPost(rkey, &bsky.FeedPost{
			Text:      "link",
			CreatedAt: "2026-04-24T12:00:00Z",
			Embed:     &bsky.FeedPost_Embed{EmbedExternal: mkExternal("https://example.com/path?q=1", "Title")},
		})
		if p.EmbedKind != "external" {
			t.Fatalf("kind = %q; want external", p.EmbedKind)
		}
		if p.EmbedExternalURI != "https://example.com/path?q=1" ||
			p.EmbedExternalDomain != "example.com" ||
			p.EmbedExternalTitle != "Title" {
			t.Fatalf("external fields: uri=%q domain=%q title=%q",
				p.EmbedExternalURI, p.EmbedExternalDomain, p.EmbedExternalTitle)
		}
	})

	t.Run("record_quote", func(t *testing.T) {
		p := convertPost(rkey, &bsky.FeedPost{
			Text:      "quote",
			CreatedAt: "2026-04-24T12:00:00Z",
			Embed: &bsky.FeedPost_Embed{EmbedRecord: &bsky.EmbedRecord{
				Record: &atproto.RepoStrongRef{
					Uri: "at://did:plc:other/app.bsky.feed.post/abc",
					Cid: "bafyq",
				},
			}},
		})
		if p.EmbedKind != "record" {
			t.Fatalf("kind = %q; want record", p.EmbedKind)
		}
		if p.QuoteAuthorDID != "did:plc:other" || p.QuoteRkey != "abc" {
			t.Fatalf("quote fields: author=%q rkey=%q", p.QuoteAuthorDID, p.QuoteRkey)
		}
	})

	t.Run("recordWithMedia_images", func(t *testing.T) {
		p := convertPost(rkey, &bsky.FeedPost{
			Text:      "quote with image",
			CreatedAt: "2026-04-24T12:00:00Z",
			Embed: &bsky.FeedPost_Embed{EmbedRecordWithMedia: &bsky.EmbedRecordWithMedia{
				Record: &bsky.EmbedRecord{Record: &atproto.RepoStrongRef{
					Uri: "at://did:plc:other/app.bsky.feed.post/qq",
					Cid: "bafyq",
				}},
				Media: &bsky.EmbedRecordWithMedia_Media{
					EmbedImages: &bsky.EmbedImages{Images: []*bsky.EmbedImages_Image{
						{Alt: "x"},
					}},
				},
			}},
		})
		if p.EmbedKind != "recordWithMedia:images" {
			t.Fatalf("kind = %q; want recordWithMedia:images", p.EmbedKind)
		}
		if p.QuoteAuthorDID != "did:plc:other" || p.QuoteRkey != "qq" {
			t.Fatalf("quote fields: author=%q rkey=%q", p.QuoteAuthorDID, p.QuoteRkey)
		}
		if p.EmbedImageCount != 1 || p.EmbedImageWithAlt != 1 {
			t.Fatalf("image counts: count=%d with_alt=%d", p.EmbedImageCount, p.EmbedImageWithAlt)
		}
	})

	t.Run("recordWithMedia_external", func(t *testing.T) {
		p := convertPost(rkey, &bsky.FeedPost{
			Text:      "quote with link",
			CreatedAt: "2026-04-24T12:00:00Z",
			Embed: &bsky.FeedPost_Embed{EmbedRecordWithMedia: &bsky.EmbedRecordWithMedia{
				Record: &bsky.EmbedRecord{Record: &atproto.RepoStrongRef{
					Uri: "at://did:plc:other/app.bsky.feed.post/qq",
					Cid: "bafyq",
				}},
				Media: &bsky.EmbedRecordWithMedia_Media{
					EmbedExternal: mkExternal("https://news.example/a", "Headline"),
				},
			}},
		})
		if p.EmbedKind != "recordWithMedia:external" {
			t.Fatalf("kind = %q; want recordWithMedia:external", p.EmbedKind)
		}
		if p.EmbedExternalDomain != "news.example" {
			t.Fatalf("domain = %q; want news.example", p.EmbedExternalDomain)
		}
	})
}

func TestParseAtTime(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		wantZ bool  // expect zero time
		year  int   // otherwise check year
	}{
		{"empty", "", true, 0},
		{"rfc3339nano", "2026-04-24T12:34:56.123456789Z", false, 2026},
		{"rfc3339", "2026-04-24T12:34:56Z", false, 2026},
		{"no_timezone_nano", "2026-04-24T12:34:56.123456789", false, 2026},
		{"no_timezone_micro", "2026-04-24T12:34:56.123456", false, 2026},
		{"no_timezone_plain", "2026-04-24T12:34:56", false, 2026},
		{"with_offset", "2026-04-24T12:34:56+00:00", false, 2026},
		{"garbage", "nope", true, 0},
		{"bad_fmt", "2026-04-24 12:34:56", true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseAtTime(tc.in)
			if tc.wantZ {
				if !got.IsZero() {
					t.Fatalf("parseAtTime(%q) = %v; want zero", tc.in, got)
				}
				return
			}
			if got.IsZero() {
				t.Fatalf("parseAtTime(%q) returned zero; wanted a time", tc.in)
			}
			if got.Year() != tc.year {
				t.Fatalf("parseAtTime(%q).Year() = %d; want %d", tc.in, got.Year(), tc.year)
			}
			// Must be normalized to UTC.
			if got.Location() != time.UTC {
				t.Fatalf("parseAtTime(%q).Location = %v; want UTC", tc.in, got.Location())
			}
		})
	}
}
