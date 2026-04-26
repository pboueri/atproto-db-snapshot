package duckdbstore

import (
	"path/filepath"
	"testing"
	"time"
)

// TestOpenAllAppliesSchema verifies every table named in schemaAllSQL is
// queryable right after OpenAll.
func TestOpenAllAppliesSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "all.duckdb")
	s, err := OpenAll(path, "", 0)
	if err != nil {
		t.Fatalf("OpenAll: %v", err)
	}
	defer s.Close()

	tables := []string{
		"actors_registry", "actors",
		"follows_current", "blocks_current",
		"posts", "likes_current", "reposts_current", "_meta",
	}
	for _, tbl := range tables {
		var n int64
		if err := s.DB.QueryRow("SELECT count(*) FROM " + tbl).Scan(&n); err != nil {
			t.Fatalf("table %s not present: %v", tbl, err)
		}
		if n != 0 {
			t.Fatalf("fresh %s count = %d; want 0", tbl, n)
		}
	}
}

// TestAllStoreWriteMetaAndLastBuiltAt verifies WriteMeta + LastBuiltAt round-trip.
func TestAllStoreWriteMetaAndLastBuiltAt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "all.duckdb")
	s, err := OpenAll(path, "", 0)
	if err != nil {
		t.Fatalf("OpenAll: %v", err)
	}
	defer s.Close()

	// Empty _meta → zero time.
	got, err := s.LastBuiltAt()
	if err != nil {
		t.Fatalf("LastBuiltAt on empty: %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("LastBuiltAt on empty = %v; want zero", got)
	}

	want := time.Now().UTC().Truncate(time.Second)
	if err := s.WriteMeta(want, 123456, "incremental", `{"posts":{}}`); err != nil {
		t.Fatalf("WriteMeta: %v", err)
	}
	got, err = s.LastBuiltAt()
	if err != nil {
		t.Fatalf("LastBuiltAt: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("LastBuiltAt = %v; want %v", got, want)
	}

	// Multiple writes → max(built_at).
	newer := want.Add(time.Hour)
	if err := s.WriteMeta(newer, 0, "incremental", `{}`); err != nil {
		t.Fatalf("WriteMeta #2: %v", err)
	}
	got, err = s.LastBuiltAt()
	if err != nil {
		t.Fatalf("LastBuiltAt #2: %v", err)
	}
	if !got.Equal(newer) {
		t.Fatalf("LastBuiltAt after 2nd write = %v; want %v", got, newer)
	}
}

// TestAllStoreCountRow verifies CountRow reflects direct inserts.
func TestAllStoreCountRow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "all.duckdb")
	s, err := OpenAll(path, "", 0)
	if err != nil {
		t.Fatalf("OpenAll: %v", err)
	}
	defer s.Close()

	// Seed actors_registry + actors via direct SQL; these tables are the
	// ones the incremental build touches before Appenders come into play.
	for i := 1; i <= 5; i++ {
		if _, err := s.DB.Exec(
			`INSERT INTO actors_registry (actor_id, did, first_seen) VALUES (?, ?, ?)`,
			int64(i), "did:plc:x"+itoaLocalAll(i), time.Now().UTC(),
		); err != nil {
			t.Fatalf("insert registry: %v", err)
		}
	}
	n, err := s.RegistryRowCount()
	if err != nil {
		t.Fatalf("RegistryRowCount: %v", err)
	}
	if n != 5 {
		t.Fatalf("RegistryRowCount = %d; want 5", n)
	}
	n, err = s.CountRow("actors")
	if err != nil {
		t.Fatalf("CountRow actors: %v", err)
	}
	if n != 0 {
		t.Fatalf("actors count = %d; want 0", n)
	}
}

func itoaLocalAll(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
