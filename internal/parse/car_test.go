package parse

import (
	"testing"
	"time"
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
