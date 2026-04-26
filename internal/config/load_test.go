package config

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestLoad_GoldenSpecConfig(t *testing.T) {
	cfg, err := Load(filepath.Join("testdata", "config.yaml"))
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	// object_store
	if cfg.ObjectStore.Type != "r2" {
		t.Errorf("ObjectStore.Type = %q, want %q", cfg.ObjectStore.Type, "r2")
	}
	if cfg.ObjectStore.Bucket != "my-bsky-snapshot" {
		t.Errorf("ObjectStore.Bucket = %q", cfg.ObjectStore.Bucket)
	}
	if cfg.ObjectStore.Endpoint != "https://acct.r2.cloudflarestorage.com" {
		t.Errorf("ObjectStore.Endpoint = %q", cfg.ObjectStore.Endpoint)
	}
	if cfg.ObjectStore.PublicURLBase != "https://pub-hash.r2.dev" {
		t.Errorf("ObjectStore.PublicURLBase = %q", cfg.ObjectStore.PublicURLBase)
	}

	// relay
	if cfg.Relay.Host != "bsky.network" {
		t.Errorf("Relay.Host = %q", cfg.Relay.Host)
	}
	if cfg.Relay.ListReposWorkers != 150 {
		t.Errorf("Relay.ListReposWorkers = %d", cfg.Relay.ListReposWorkers)
	}
	if cfg.Relay.RateLimitRPS != 80 {
		t.Errorf("Relay.RateLimitRPS = %v", cfg.Relay.RateLimitRPS)
	}

	// reconciled flat Config fields
	if cfg.Config.RelayHost != "bsky.network" {
		t.Errorf("Config.RelayHost = %q", cfg.Config.RelayHost)
	}
	if cfg.Config.Workers != 150 {
		t.Errorf("Config.Workers = %d", cfg.Config.Workers)
	}
	if cfg.Config.RateLimitRPS != 80 {
		t.Errorf("Config.RateLimitRPS = %v", cfg.Config.RateLimitRPS)
	}

	// jetstream YAML overlay → Config.Jetstream
	wantEndpoints := []string{
		"wss://jetstream2.us-east.bsky.network/subscribe",
		"wss://jetstream1.us-east.bsky.network/subscribe",
	}
	if !reflect.DeepEqual(cfg.Config.Jetstream.Endpoints, wantEndpoints) {
		t.Errorf("Config.Jetstream.Endpoints = %v, want %v",
			cfg.Config.Jetstream.Endpoints, wantEndpoints)
	}
	if !cfg.Config.Jetstream.Compress {
		t.Errorf("Config.Jetstream.Compress = false, want true")
	}
	if cfg.Config.Jetstream.RewindSeconds != 10 {
		t.Errorf("Config.Jetstream.RewindSeconds = %d", cfg.Config.Jetstream.RewindSeconds)
	}
	// WantedCollections wasn't in YAML — defaults must be preserved.
	if len(cfg.Config.Jetstream.WantedCollections) == 0 {
		t.Errorf("Config.Jetstream.WantedCollections lost defaults")
	}

	// retention
	if cfg.Retention.ParquetDays != 90 {
		t.Errorf("Retention.ParquetDays = %d", cfg.Retention.ParquetDays)
	}

	// filters
	if got, want := cfg.Filters.Posts.Langs, []string{"en"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Filters.Posts.Langs = %v, want %v", got, want)
	}
	if cfg.Filters.Posts.ExcludeNoLang {
		t.Errorf("Filters.Posts.ExcludeNoLang = true, want false")
	}

	// paths
	if cfg.Paths.DataDir != "./data" {
		t.Errorf("Paths.DataDir = %q", cfg.Paths.DataDir)
	}
	if cfg.Paths.TempDir != "./data/tmp" {
		t.Errorf("Paths.TempDir = %q", cfg.Paths.TempDir)
	}
	if cfg.Config.DataDir != "./data" {
		t.Errorf("Config.DataDir = %q (reconcile failed)", cfg.Config.DataDir)
	}

	// duckdb
	if cfg.DuckDB.MemoryLimit != "12GB" {
		t.Errorf("DuckDB.MemoryLimit = %q", cfg.DuckDB.MemoryLimit)
	}
	if cfg.DuckDB.Threads != 4 {
		t.Errorf("DuckDB.Threads = %d", cfg.DuckDB.Threads)
	}

	// serve
	if cfg.Serve.Listen != "127.0.0.1:8080" {
		t.Errorf("Serve.Listen = %q", cfg.Serve.Listen)
	}
	if cfg.Serve.RefreshSeconds != 5 {
		t.Errorf("Serve.RefreshSeconds = %d", cfg.Serve.RefreshSeconds)
	}
	if cfg.Serve.LogTailLines != 20000 {
		t.Errorf("Serve.LogTailLines = %d", cfg.Serve.LogTailLines)
	}
}

func TestLoad_MissingPath(t *testing.T) {
	_, err := Load(filepath.Join("testdata", "does-not-exist.yaml"))
	if err == nil {
		t.Fatalf("expected error for missing path, got nil")
	}
	// Error must mention the path so callers can debug.
	if got := err.Error(); !contains(got, "does-not-exist.yaml") {
		t.Errorf("error message %q does not include path", got)
	}
}

func TestLoad_EmptyYAMLReturnsDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join("testdata", "empty.yaml"))
	if err != nil {
		t.Fatalf("Load(empty.yaml) error: %v", err)
	}

	// Empty YAML overlays nothing → result is DefaultExtended() with the
	// reconcile pass applied (structured defaults flow into the embedded
	// Config's flat fields).
	want := DefaultExtended()
	if err := reconcileFlatConfig(&want); err != nil {
		t.Fatalf("reconcileFlatConfig(default): %v", err)
	}

	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("empty YAML did not match reconciled defaults\n got: %+v\nwant: %+v", cfg, want)
	}

	// Spot-check key defaults survive an empty YAML.
	if cfg.Retention.ParquetDays != 90 {
		t.Errorf("Retention.ParquetDays = %d, want 90", cfg.Retention.ParquetDays)
	}
	if cfg.DuckDB.MemoryLimit != "12GB" {
		t.Errorf("DuckDB.MemoryLimit = %q, want 12GB", cfg.DuckDB.MemoryLimit)
	}
	if cfg.Serve.Listen != "127.0.0.1:8080" {
		t.Errorf("Serve.Listen = %q", cfg.Serve.Listen)
	}
	if cfg.ObjectStore.Type != "file" {
		t.Errorf("ObjectStore.Type = %q, want file", cfg.ObjectStore.Type)
	}
	// Reconciled flat fields:
	if cfg.Config.RelayHost != "bsky.network" {
		t.Errorf("Config.RelayHost = %q, want bsky.network", cfg.Config.RelayHost)
	}
	if cfg.Config.Workers != 150 {
		t.Errorf("Config.Workers = %d, want 150", cfg.Config.Workers)
	}
}

func TestLoad_PartialYAMLPreservesDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join("testdata", "partial.yaml"))
	if err != nil {
		t.Fatalf("Load(partial.yaml) error: %v", err)
	}

	// Overridden:
	if cfg.Relay.Host != "foo" {
		t.Errorf("Relay.Host = %q, want %q", cfg.Relay.Host, "foo")
	}
	if cfg.Config.RelayHost != "foo" {
		t.Errorf("Config.RelayHost = %q (reconcile failed)", cfg.Config.RelayHost)
	}

	// Other fields keep defaults:
	def := DefaultExtended()
	if cfg.Relay.ListReposWorkers != def.Relay.ListReposWorkers {
		t.Errorf("Relay.ListReposWorkers stomped: got %d want %d",
			cfg.Relay.ListReposWorkers, def.Relay.ListReposWorkers)
	}
	if cfg.Relay.RateLimitRPS != def.Relay.RateLimitRPS {
		t.Errorf("Relay.RateLimitRPS stomped: got %v want %v",
			cfg.Relay.RateLimitRPS, def.Relay.RateLimitRPS)
	}
	if cfg.Paths.DataDir != def.Paths.DataDir {
		t.Errorf("Paths.DataDir stomped: got %q want %q",
			cfg.Paths.DataDir, def.Paths.DataDir)
	}
	if cfg.DuckDB.MemoryLimit != def.DuckDB.MemoryLimit {
		t.Errorf("DuckDB.MemoryLimit stomped: got %q want %q",
			cfg.DuckDB.MemoryLimit, def.DuckDB.MemoryLimit)
	}
	if cfg.Serve.Listen != def.Serve.Listen {
		t.Errorf("Serve.Listen stomped: got %q want %q",
			cfg.Serve.Listen, def.Serve.Listen)
	}
	if cfg.Retention.ParquetDays != def.Retention.ParquetDays {
		t.Errorf("Retention.ParquetDays stomped: got %d want %d",
			cfg.Retention.ParquetDays, def.Retention.ParquetDays)
	}
	if cfg.ObjectStore.Type != def.ObjectStore.Type {
		t.Errorf("ObjectStore.Type stomped: got %q want %q",
			cfg.ObjectStore.Type, def.ObjectStore.Type)
	}

	// Embedded Config defaults that weren't touched by YAML:
	if cfg.Config.HTTPTimeout != def.Config.HTTPTimeout {
		t.Errorf("Config.HTTPTimeout stomped: got %v want %v",
			cfg.Config.HTTPTimeout, def.Config.HTTPTimeout)
	}
	if cfg.Config.Jetstream.RewindSeconds != def.Config.Jetstream.RewindSeconds {
		t.Errorf("Config.Jetstream.RewindSeconds stomped: got %d want %d",
			cfg.Config.Jetstream.RewindSeconds, def.Config.Jetstream.RewindSeconds)
	}
	if cfg.Config.Jetstream.CheckpointInterval != 60*time.Second {
		t.Errorf("Config.Jetstream.CheckpointInterval = %v, want 60s",
			cfg.Config.Jetstream.CheckpointInterval)
	}
}

// contains is a tiny stdlib-free substring helper to avoid pulling in
// strings just for one assertion. (gosimple-friendly: trivial loop.)
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
