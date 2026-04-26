package parse

import (
	"testing"
	"time"
)

func TestParseAtTime(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		wantZ bool
		year  int
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
			got := ParseAtTime(tc.in)
			if tc.wantZ {
				if !got.IsZero() {
					t.Fatalf("ParseAtTime(%q) = %v; want zero", tc.in, got)
				}
				return
			}
			if got.IsZero() {
				t.Fatalf("ParseAtTime(%q) returned zero; wanted a time", tc.in)
			}
			if got.Year() != tc.year {
				t.Fatalf("ParseAtTime(%q).Year() = %d; want %d", tc.in, got.Year(), tc.year)
			}
			if got.Location() != time.UTC {
				t.Fatalf("ParseAtTime(%q).Location = %v; want UTC", tc.in, got.Location())
			}
		})
	}
}
