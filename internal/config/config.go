// Package config parses flags and environment variables shared across subcommands.
//
// Flags always win over env vars when both are set. Each subcommand calls
// ParseFlags with its name so unknown flags surface as errors instead of being
// silently ignored.
package config

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the resolved configuration for a single subcommand invocation.
type Config struct {
	// Subcommand is the name of the command being run; used in logs.
	Subcommand string

	// DataDir is the local working directory for staging, sqlite, and logs.
	DataDir string

	// ObjectStore selects the backend: "local" (filesystem) or "s3".
	ObjectStore string

	// ObjectStoreRoot is the bucket name for s3 or the directory for local.
	ObjectStoreRoot string

	// S3Endpoint, S3Region, S3AccessKeyID, S3SecretAccessKey configure R2/S3.
	// Keys are read from env vars by default to avoid leaking onto argv.
	S3Endpoint        string
	S3Region          string
	S3AccessKeyID     string
	S3SecretAccessKey string

	// LookbackDays controls the snapshot window. Default 30.
	LookbackDays int

	// DuckDBMemoryLimit caps DuckDB working memory (e.g. "2GB"). Empty = no cap.
	DuckDBMemoryLimit string

	// PLCEndpoint and JetstreamEndpoints are source URLs. Per-DID
	// listRecords is dispatched to whatever PDS PLC reports for that DID,
	// so there is no single repo-side endpoint here.
	PLCEndpoint        string
	JetstreamEndpoints []string

	// Filters applied at write time on records that carry the metadata.
	Languages       []string // default ["en"]
	Labelers        []string // default ["did:plc:ar7c4by46qjdydhdevvrndac"] (bsky moderation)
	IncludeLabels   []string // labels to include; empty = no filter
	ExcludeLabels   []string // labels to exclude

	// Concurrency for fan-out work (bootstrap fetch, snapshot reads).
	Concurrency int

	// MaxDIDs caps the number of DIDs the bootstrap consumes from PLC.
	// Zero means unlimited (the production case). Set to a small value to
	// validate the pipeline against real services without pulling the full
	// millions-strong export.
	MaxDIDs int

	// RunDuration, if non-zero, makes the run command exit cleanly after
	// the configured duration. Useful for smoke-testing against a real
	// jetstream without leaving a long-running process behind.
	RunDuration time.Duration

	// PDSRateLimit is the sustained per-host requests-per-second budget
	// the bootstrap fan-out divides among its workers. The default of 5
	// stays well under bsky.social's published ~10 RPS per-IP global so
	// short bursts plus retries don't trip the cap.
	//
	// Currently only consulted by the legacy PDS-direct path; the
	// bootstrap fetches via Slingshot + Constellation and uses
	// MicrocosmRateLimit / MicrocosmBurst instead.
	PDSRateLimit float64
	// PDSBurst is the maximum number of tokens the per-host bucket can
	// accumulate, governing how many concurrent in-flight requests can
	// clear before throttling kicks in.
	PDSBurst int

	// ConstellationEndpoint is the backlinks index used for follows /
	// blocks during bootstrap. https://constellation.microcosm.blue is the
	// public instance; operators can self-host.
	ConstellationEndpoint string
	// SlingshotEndpoint is the edge record cache used for the profile
	// leg of bootstrap. https://slingshot.microcosm.blue is the public
	// instance.
	SlingshotEndpoint string
	// MicrocosmRateLimit is the sustained RPS budget shared across all
	// workers when calling Slingshot or Constellation. Each service has
	// its own bucket; both default to this value.
	MicrocosmRateLimit float64
	// MicrocosmBurst is the bucket capacity (max in-flight before
	// throttling kicks in).
	MicrocosmBurst int
	// ConstellationPageSize is the per-request /links limit. Constellation's
	// docs cap this at 100 but the live server accepts up to 1000 — set
	// higher to amortize per-request latency on heavy-follower DIDs.
	ConstellationPageSize int
	// Contact is appended to the User-Agent on requests to
	// Constellation / Slingshot per microcosm.blue's "if you want to be
	// nice, put your project name and bsky username (or email) in your
	// user-agent" guidance. Optional.
	Contact string

	// MonitorAddr is the listen address for `at-snapshot monitor`.
	MonitorAddr string

	// LogLevel: "debug", "info", "warn", "error".
	LogLevel string

	// StatsInterval controls how often per-job stats are emitted at INFO.
	StatsInterval time.Duration
}

// DefaultConfigPath is the implicit YAML config path consulted when neither
// -config nor AT_SNAPSHOT_CONFIG is set. A missing file at this path is not
// an error — operators who don't want a file just don't put one there.
const DefaultConfigPath = "./at-snapshot.yaml"

// ParseFlags binds the shared flag set for the named subcommand and resolves
// values from, in order of decreasing precedence:
//
//	flags > env vars > YAML config file > built-in defaults
//
// The config file path is taken from -config, then env AT_SNAPSHOT_CONFIG,
// then ./at-snapshot.yaml. An explicit path that fails to load is fatal; the
// implicit default is silently skipped if the file is missing.
func ParseFlags(sub string, args []string) (Config, error) {
	cfg := Config{Subcommand: sub}
	fs := flag.NewFlagSet(sub, flag.ContinueOnError)

	// First peek at -config so we can resolve the file before binding the
	// other flags' default values.
	cfgPath, explicit := configPath(args)
	var fc fileConfig
	if cfgPath != "" {
		loaded, err := loadFile(cfgPath)
		switch {
		case err == nil:
			fc = loaded
		case explicit:
			return Config{}, fmt.Errorf("config: load %s: %w", cfgPath, err)
		case !os.IsNotExist(err):
			return Config{}, fmt.Errorf("config: load %s: %w", cfgPath, err)
		}
	}
	fileObjectStore, fileRegion := fc.resolveBackend()
	fileEndpoint := fc.resolveEndpoint()

	// -config flag is bound for help output even though it's already parsed.
	configFlag := fs.String("config", cfgPath, "Path to YAML config file (default ./at-snapshot.yaml if present)")

	fs.StringVar(&cfg.DataDir, "data-dir", layered(fc.DataDir, "AT_SNAPSHOT_DATA_DIR", "./data"),
		"Local working directory for staging, sqlite, logs")
	fs.StringVar(&cfg.ObjectStore, "object-store", layered(fileObjectStore, "AT_SNAPSHOT_OBJECT_STORE", "local"),
		"Object store backend: local|s3|memory")
	fs.StringVar(&cfg.ObjectStoreRoot, "object-store-root", layered(fc.Bucket, "AT_SNAPSHOT_OBJECT_STORE_ROOT", "./data/object-store"),
		"Bucket name (s3) or root directory (local)")
	fs.StringVar(&cfg.S3Endpoint, "s3-endpoint", layered(fileEndpoint, "AT_SNAPSHOT_S3_ENDPOINT", ""),
		"S3-compatible endpoint URL (R2). Env: AT_SNAPSHOT_S3_ENDPOINT")
	fs.StringVar(&cfg.S3Region, "s3-region", layered(fileRegion, "AT_SNAPSHOT_S3_REGION", "auto"),
		"S3 region. Env: AT_SNAPSHOT_S3_REGION")
	// Keys are read env-first then fall back to the file. They never get a
	// flag binding because surfacing them as flags would echo secrets onto
	// the process arg list.
	cfg.S3AccessKeyID = layered(fc.AccessKey, "AT_SNAPSHOT_S3_ACCESS_KEY_ID", "")
	cfg.S3SecretAccessKey = layered(fc.SecretAccessKey, "AT_SNAPSHOT_S3_SECRET_ACCESS_KEY", "")

	fs.IntVar(&cfg.LookbackDays, "lookback-days", layeredInt(fc.LookbackDays, "AT_SNAPSHOT_LOOKBACK_DAYS", 30),
		"Snapshot lookback window in days (snapshot only)")
	fs.StringVar(&cfg.DuckDBMemoryLimit, "duckdb-memory-limit", layered(fc.DuckDBMemoryLimit, "AT_SNAPSHOT_DUCKDB_MEMORY_LIMIT", "2GB"),
		"DuckDB working memory cap (snapshot only)")

	fs.StringVar(&cfg.PLCEndpoint, "plc-endpoint", layered(fc.PLCEndpoint, "AT_SNAPSHOT_PLC_ENDPOINT", "https://plc.directory"),
		"PLC directory base URL")
	jsDefault := layered(joinCSV(fc.JetstreamEndpoints), "AT_SNAPSHOT_JETSTREAM_ENDPOINTS",
		"wss://jetstream1.us-east.bsky.network/subscribe,wss://jetstream2.us-east.bsky.network/subscribe,wss://jetstream1.us-west.bsky.network/subscribe,wss://jetstream2.us-west.bsky.network/subscribe")
	jsCSV := fs.String("jetstream-endpoints", jsDefault,
		"Comma-separated jetstream WS endpoints; failover order")

	langCSV := fs.String("languages", layered(joinCSV(fc.Languages), "AT_SNAPSHOT_LANGUAGES", "en"),
		"Comma-separated language filter for posts (empty = no filter)")
	labelerCSV := fs.String("labelers", layered(joinCSV(fc.Labelers), "AT_SNAPSHOT_LABELERS", "did:plc:ar7c4by46qjdydhdevvrndac"),
		"Comma-separated labeler DIDs to honor (empty = no filter)")
	includeLabelsCSV := fs.String("include-labels", layered(joinCSV(fc.IncludeLabels), "AT_SNAPSHOT_INCLUDE_LABELS", ""),
		"Comma-separated labels to include (empty = no allow-list)")
	excludeLabelsCSV := fs.String("exclude-labels", layered(joinCSV(fc.ExcludeLabels), "AT_SNAPSHOT_EXCLUDE_LABELS", ""),
		"Comma-separated labels to drop")

	fs.IntVar(&cfg.Concurrency, "concurrency", layeredInt(fc.Concurrency, "AT_SNAPSHOT_CONCURRENCY", 16),
		"Parallel worker count for fan-out fetches")
	pdsRateStr := fs.String("pds-rate-limit", layered(formatFloat(fc.PDSRateLimit), "AT_SNAPSHOT_PDS_RATE_LIMIT", "5"),
		"Per-PDS sustained requests-per-second across all workers")
	fs.IntVar(&cfg.PDSBurst, "pds-burst", layeredInt(fc.PDSBurst, "AT_SNAPSHOT_PDS_BURST", 5),
		"Per-PDS burst capacity (max in-flight before throttling)")
	fs.StringVar(&cfg.ConstellationEndpoint, "constellation-endpoint",
		layered(fc.ConstellationEndpoint, "AT_SNAPSHOT_CONSTELLATION_ENDPOINT", "https://constellation.microcosm.blue"),
		"Backlinks index URL (used for follows/blocks during bootstrap)")
	fs.StringVar(&cfg.SlingshotEndpoint, "slingshot-endpoint",
		layered(fc.SlingshotEndpoint, "AT_SNAPSHOT_SLINGSHOT_ENDPOINT", "https://slingshot.microcosm.blue"),
		"Edge record cache URL (used for profile fetches during bootstrap)")
	microRateStr := fs.String("microcosm-rate-limit",
		layered(formatFloat(fc.MicrocosmRateLimit), "AT_SNAPSHOT_MICROCOSM_RATE_LIMIT", "20"),
		"Sustained requests-per-second to Slingshot / Constellation (shared across workers, per service)")
	fs.IntVar(&cfg.MicrocosmBurst, "microcosm-burst",
		layeredInt(fc.MicrocosmBurst, "AT_SNAPSHOT_MICROCOSM_BURST", 40),
		"Microcosm bucket capacity (max in-flight before throttling)")
	fs.IntVar(&cfg.ConstellationPageSize, "constellation-page-size",
		layeredInt(fc.ConstellationPageSize, "AT_SNAPSHOT_CONSTELLATION_PAGE_SIZE", 1000),
		"Constellation /links page size (docs cap 100; live server accepts up to 1000)")
	fs.StringVar(&cfg.Contact, "contact",
		layered(fc.Contact, "AT_SNAPSHOT_CONTACT", ""),
		"Contact (bsky handle or email) appended to outgoing User-Agent — microcosm.blue requests it")
	fs.IntVar(&cfg.MaxDIDs, "max-dids", layeredInt(fc.MaxDIDs, "AT_SNAPSHOT_MAX_DIDS", 0),
		"Bootstrap: cap the number of DIDs consumed from PLC (0 = no cap)")
	fs.DurationVar(&cfg.RunDuration, "run-duration", layeredDuration(fc.RunDuration, "AT_SNAPSHOT_RUN_DURATION", 0),
		"Run: exit cleanly after this duration (0 = run forever)")
	fs.StringVar(&cfg.MonitorAddr, "addr", layered(fc.MonitorAddr, "AT_SNAPSHOT_MONITOR_ADDR", ":8080"),
		"Monitor HTTP listen address (monitor only)")
	fs.StringVar(&cfg.LogLevel, "log-level", layered(fc.LogLevel, "AT_SNAPSHOT_LOG_LEVEL", "info"),
		"Log level: debug|info|warn|error")
	fs.DurationVar(&cfg.StatsInterval, "stats-interval", layeredDuration(fc.StatsInterval, "AT_SNAPSHOT_STATS_INTERVAL", 30*time.Second),
		"Periodic stats emission interval")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	_ = configFlag // already consumed by configPath; bound for -h visibility

	cfg.JetstreamEndpoints = splitCSV(*jsCSV)
	cfg.Languages = splitCSV(*langCSV)
	cfg.Labelers = splitCSV(*labelerCSV)
	cfg.IncludeLabels = splitCSV(*includeLabelsCSV)
	cfg.ExcludeLabels = splitCSV(*excludeLabelsCSV)

	if v, err := strconv.ParseFloat(*pdsRateStr, 64); err == nil {
		cfg.PDSRateLimit = v
	}
	if v, err := strconv.ParseFloat(*microRateStr, 64); err == nil {
		cfg.MicrocosmRateLimit = v
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func formatFloat(f float64) string {
	if f == 0 {
		return ""
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// configPath finds the -config value across args, env, and the implicit
// default. The boolean reports whether the path was *explicitly* set: an
// explicit path that fails to load is fatal; the implicit default is
// silently skipped on miss.
func configPath(args []string) (path string, explicit bool) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-config" || a == "--config":
			if i+1 < len(args) {
				return args[i+1], true
			}
		case strings.HasPrefix(a, "-config=") || strings.HasPrefix(a, "--config="):
			_, v, _ := strings.Cut(a, "=")
			return v, true
		}
	}
	if v, ok := os.LookupEnv("AT_SNAPSHOT_CONFIG"); ok && v != "" {
		return v, true
	}
	if _, err := os.Stat(DefaultConfigPath); err == nil {
		return DefaultConfigPath, false
	}
	return "", false
}

// layered resolves a single string field with env > file > builtin precedence.
// (Flags layer on top of this via the flag.* binding default.)
func layered(fileVal, envKey, builtin string) string {
	if v, ok := os.LookupEnv(envKey); ok && v != "" {
		return v
	}
	if fileVal != "" {
		return fileVal
	}
	return builtin
}

func layeredInt(fileVal int, envKey string, builtin int) int {
	if v, ok := os.LookupEnv(envKey); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	if fileVal != 0 {
		return fileVal
	}
	return builtin
}

func layeredDuration(fileVal time.Duration, envKey string, builtin time.Duration) time.Duration {
	if v, ok := os.LookupEnv(envKey); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	if fileVal != 0 {
		return fileVal
	}
	return builtin
}

func joinCSV(s []string) string { return strings.Join(s, ",") }

// Validate checks invariants. Called by ParseFlags but exported for tests.
func (c *Config) Validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}
	switch c.ObjectStore {
	case "local", "s3", "memory":
	default:
		return fmt.Errorf("object-store %q not supported (local|s3|memory)", c.ObjectStore)
	}
	if c.ObjectStoreRoot == "" {
		return fmt.Errorf("object-store-root is required")
	}
	if c.LookbackDays <= 0 {
		return fmt.Errorf("lookback-days must be > 0")
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be > 0")
	}
	if _, err := ParseLogLevel(c.LogLevel); err != nil {
		return err
	}
	return nil
}

// ParseLogLevel maps the textual log level to slog.Level.
func ParseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log level %q", s)
	}
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envOrInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envOrDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
