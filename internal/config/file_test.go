package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileConfigKeyAliasing(t *testing.T) {
	body := `BACKEND: R2
BUCKET: my-bucket
URL: example.r2.cloudflarestorage.com
Access Key: AK
Secret Access Key: SK
data-dir: /tmp/x
lookback_days: 7
languages: [en, ja]
stats_interval: 5s
`
	dir := t.TempDir()
	path := filepath.Join(dir, "at-snapshot.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	fc, err := loadFile(path)
	if err != nil {
		t.Fatalf("loadFile: %v", err)
	}
	if fc.Backend != "R2" {
		t.Errorf("Backend = %q", fc.Backend)
	}
	if fc.Bucket != "my-bucket" {
		t.Errorf("Bucket = %q", fc.Bucket)
	}
	if fc.AccessKey != "AK" || fc.SecretAccessKey != "SK" {
		t.Errorf("creds = %q/%q", fc.AccessKey, fc.SecretAccessKey)
	}
	if fc.DataDir != "/tmp/x" {
		t.Errorf("DataDir = %q", fc.DataDir)
	}
	if fc.LookbackDays != 7 {
		t.Errorf("LookbackDays = %d", fc.LookbackDays)
	}
	if got := fc.Languages; len(got) != 2 || got[0] != "en" || got[1] != "ja" {
		t.Errorf("Languages = %v", got)
	}
	if fc.StatsInterval != 5*time.Second {
		t.Errorf("StatsInterval = %v", fc.StatsInterval)
	}
}

func TestResolveBackendR2DefaultsRegion(t *testing.T) {
	fc := fileConfig{Backend: "r2"}
	store, region := fc.resolveBackend()
	if store != "s3" {
		t.Errorf("store = %q, want s3", store)
	}
	if region != "auto" {
		t.Errorf("region = %q, want auto", region)
	}
}

func TestResolveEndpointPrependsScheme(t *testing.T) {
	fc := fileConfig{URL: "example.r2.cloudflarestorage.com/"}
	got := fc.resolveEndpoint()
	want := "https://example.r2.cloudflarestorage.com"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestParseFlagsLoadsFileAndEnvOverrides(t *testing.T) {
	body := `backend: r2
bucket: from-file
url: example.r2.cloudflarestorage.com
access_key: AK
secret_access_key: SK
data_dir: /tmp/from-file
lookback_days: 14
`
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	clearEnv(t,
		"AT_SNAPSHOT_DATA_DIR",
		"AT_SNAPSHOT_OBJECT_STORE",
		"AT_SNAPSHOT_OBJECT_STORE_ROOT",
		"AT_SNAPSHOT_S3_ENDPOINT",
		"AT_SNAPSHOT_S3_REGION",
		"AT_SNAPSHOT_S3_ACCESS_KEY_ID",
		"AT_SNAPSHOT_S3_SECRET_ACCESS_KEY",
		"AT_SNAPSHOT_LOOKBACK_DAYS",
		"AT_SNAPSHOT_CONFIG",
	)

	cfg, err := ParseFlags("snapshot", []string{"-config", path})
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if cfg.ObjectStore != "s3" {
		t.Errorf("ObjectStore = %q, want s3", cfg.ObjectStore)
	}
	if cfg.ObjectStoreRoot != "from-file" {
		t.Errorf("ObjectStoreRoot = %q", cfg.ObjectStoreRoot)
	}
	if cfg.S3Endpoint != "https://example.r2.cloudflarestorage.com" {
		t.Errorf("S3Endpoint = %q", cfg.S3Endpoint)
	}
	if cfg.S3Region != "auto" {
		t.Errorf("S3Region = %q", cfg.S3Region)
	}
	if cfg.S3AccessKeyID != "AK" || cfg.S3SecretAccessKey != "SK" {
		t.Errorf("creds = %q/%q", cfg.S3AccessKeyID, cfg.S3SecretAccessKey)
	}
	if cfg.LookbackDays != 14 {
		t.Errorf("LookbackDays = %d, want 14", cfg.LookbackDays)
	}

	// Env override beats file.
	t.Setenv("AT_SNAPSHOT_LOOKBACK_DAYS", "60")
	cfg2, err := ParseFlags("snapshot", []string{"-config", path})
	if err != nil {
		t.Fatal(err)
	}
	if cfg2.LookbackDays != 60 {
		t.Errorf("env override LookbackDays = %d, want 60", cfg2.LookbackDays)
	}

	// Flag override beats env.
	cfg3, err := ParseFlags("snapshot", []string{"-config", path, "-lookback-days", "5"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg3.LookbackDays != 5 {
		t.Errorf("flag override LookbackDays = %d, want 5", cfg3.LookbackDays)
	}
}

func TestParseFlagsExplicitMissingFileFatal(t *testing.T) {
	clearEnv(t, "AT_SNAPSHOT_CONFIG")
	_, err := ParseFlags("snapshot", []string{"-config", "/no/such/file.yaml"})
	if err == nil {
		t.Errorf("expected error for missing explicit -config path")
	}
}

func TestParseFlagsImplicitMissingFileSilent(t *testing.T) {
	clearEnv(t, "AT_SNAPSHOT_CONFIG", "AT_SNAPSHOT_DATA_DIR")
	// Default config path almost certainly doesn't exist in CWD during test
	// (we don't chdir). ParseFlags should succeed with built-in defaults.
	cfg, err := ParseFlags("snapshot", nil)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if cfg.DataDir != "./data" {
		t.Errorf("DataDir = %q, want built-in ./data", cfg.DataDir)
	}
}
