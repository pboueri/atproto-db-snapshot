package config

import (
	"os"
	"testing"
	"time"
)

func TestParseFlagsDefaults(t *testing.T) {
	clearEnv(t,
		"AT_SNAPSHOT_DATA_DIR",
		"AT_SNAPSHOT_OBJECT_STORE",
		"AT_SNAPSHOT_OBJECT_STORE_ROOT",
		"AT_SNAPSHOT_LOOKBACK_DAYS",
		"AT_SNAPSHOT_LOG_LEVEL",
		"AT_SNAPSHOT_LANGUAGES",
		"AT_SNAPSHOT_LABELERS",
		"AT_SNAPSHOT_JETSTREAM_ENDPOINTS",
		"AT_SNAPSHOT_CONCURRENCY",
		"AT_SNAPSHOT_STATS_INTERVAL",
	)

	cfg, err := ParseFlags("snapshot", nil)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if cfg.DataDir != "./data" {
		t.Errorf("DataDir default = %q, want ./data", cfg.DataDir)
	}
	if cfg.ObjectStore != "local" {
		t.Errorf("ObjectStore default = %q, want local", cfg.ObjectStore)
	}
	if cfg.LookbackDays != 30 {
		t.Errorf("LookbackDays default = %d, want 30", cfg.LookbackDays)
	}
	if cfg.Concurrency <= 0 {
		t.Errorf("Concurrency must be > 0, got %d", cfg.Concurrency)
	}
	if cfg.StatsInterval != 30*time.Second {
		t.Errorf("StatsInterval default = %v, want 30s", cfg.StatsInterval)
	}
	if got, want := len(cfg.Languages), 1; got != want || cfg.Languages[0] != "en" {
		t.Errorf("Languages default = %v, want [en]", cfg.Languages)
	}
	if len(cfg.JetstreamEndpoints) < 2 {
		t.Errorf("expected multiple jetstream endpoints, got %v", cfg.JetstreamEndpoints)
	}
}

func TestParseFlagsOverrides(t *testing.T) {
	args := []string{
		"-data-dir", "/tmp/x",
		"-lookback-days", "7",
		"-languages", "en,ja",
		"-labelers", "",
		"-concurrency", "4",
	}
	cfg, err := ParseFlags("snapshot", args)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if cfg.DataDir != "/tmp/x" {
		t.Errorf("DataDir = %q", cfg.DataDir)
	}
	if cfg.LookbackDays != 7 {
		t.Errorf("LookbackDays = %d", cfg.LookbackDays)
	}
	if got := cfg.Languages; len(got) != 2 || got[0] != "en" || got[1] != "ja" {
		t.Errorf("Languages = %v, want [en ja]", got)
	}
	if cfg.Labelers != nil {
		t.Errorf("Labelers = %v, want nil (filter cleared)", cfg.Labelers)
	}
}

func TestValidate(t *testing.T) {
	good := Config{DataDir: "x", ObjectStore: "local", ObjectStoreRoot: "y", LookbackDays: 1, Concurrency: 1, LogLevel: "info"}
	if err := good.Validate(); err != nil {
		t.Errorf("Validate good: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(c *Config)
	}{
		{"missing data-dir", func(c *Config) { c.DataDir = "" }},
		{"bad object-store", func(c *Config) { c.ObjectStore = "ftp" }},
		{"missing root", func(c *Config) { c.ObjectStoreRoot = "" }},
		{"non-positive lookback", func(c *Config) { c.LookbackDays = 0 }},
		{"non-positive concurrency", func(c *Config) { c.Concurrency = 0 }},
		{"bad log level", func(c *Config) { c.LogLevel = "loud" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := good
			tc.mutate(&c)
			if err := c.Validate(); err == nil {
				t.Errorf("expected error")
			}
		})
	}
}

// clearEnv unsets the named env vars for the duration of the test.
// t.Setenv with an empty string still leaves the var defined, which would
// override our defaults; we want the var truly absent. To get cleanup, we
// stash the current value with t.Setenv first (which registers a Cleanup),
// then Unsetenv to remove it for the test body.
func clearEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if v, ok := os.LookupEnv(k); ok {
			t.Setenv(k, v)
		}
		os.Unsetenv(k)
	}
}
