package run

import (
	"bytes"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// TestJetstreamZstdDictEmbedded asserts the published dictionary made it into
// the binary. Without bytes here, the live decoder would silently emit garbage
// for every frame. The first 4 bytes are the zstd dictionary magic
// (0xEC30A437 little-endian).
func TestJetstreamZstdDictEmbedded(t *testing.T) {
	if len(jetstreamZstdDict) == 0 {
		t.Fatal("jetstreamZstdDict is empty — //go:embed did not pick up internal/run/zstd_dictionary")
	}
	if len(jetstreamZstdDict) < 4 {
		t.Fatalf("dict too short: %d bytes", len(jetstreamZstdDict))
	}
	wantMagic := []byte{0x37, 0xa4, 0x30, 0xec}
	if !bytes.Equal(jetstreamZstdDict[:4], wantMagic) {
		t.Fatalf("dict magic mismatch: got % x want % x", jetstreamZstdDict[:4], wantMagic)
	}
}

// TestJetstreamZstdDecoderConstructs verifies the dictionary loads cleanly
// into klauspost/compress/zstd via WithDecoderDicts. This is a pure
// construction test — a real round-trip requires a sample frame produced by
// the live Jetstream encoder, which is out of scope here.
func TestJetstreamZstdDecoderConstructs(t *testing.T) {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(jetstreamZstdDict))
	if err != nil {
		t.Fatalf("zstd.NewReader with dictionary: %v", err)
	}
	defer dec.Close()

	// Round-trip a small payload encoded WITHOUT the dictionary. The decoder
	// should still handle plain zstd frames; this confirms the dict-loaded
	// reader didn't break the no-dict path.
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd.NewWriter: %v", err)
	}
	defer enc.Close()

	plain := []byte(`{"kind":"commit","did":"did:plc:test"}`)
	compressed := enc.EncodeAll(plain, nil)

	got, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		t.Fatalf("DecodeAll: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, plain)
	}
}
