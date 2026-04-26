package intern

import "testing"

func TestID64Deterministic(t *testing.T) {
	a := ID64("did:plc:abc123")
	b := ID64("did:plc:abc123")
	if a != b {
		t.Errorf("not deterministic: %d != %d", a, b)
	}
}

func TestID64DifferentInputs(t *testing.T) {
	if ID64("a") == ID64("b") {
		t.Errorf("collision on trivially different inputs")
	}
}

func TestID64EmptyIsZero(t *testing.T) {
	if got := ID64(""); got != 0 {
		t.Errorf("ID64(\"\") = %d, want 0", got)
	}
}

func TestDIDIDAndURIIDAlias(t *testing.T) {
	s := "at://did:plc:abc/app.bsky.feed.post/3kk"
	if DIDID(s) != ID64(s) || URIID(s) != ID64(s) {
		t.Errorf("alias drift")
	}
}
