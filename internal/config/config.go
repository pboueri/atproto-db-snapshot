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

	// PLCEndpoint, ConstellationEndpoint, JetstreamEndpoints are source URLs.
	PLCEndpoint           string
	ConstellationEndpoint string
	JetstreamEndpoints    []string

	// Filters applied at write time on records that carry the metadata.
	Languages       []string // default ["en"]
	Labelers        []string // default ["did:plc:ar7c4by46qjdydhdevvrndac"] (bsky moderation)
	IncludeLabels   []string // labels to include; empty = no filter
	ExcludeLabels   []string // labels to exclude

	// Concurrency for fan-out work (bootstrap fetch, snapshot reads).
	Concurrency int

	// MonitorAddr is the listen address for `at-snapshot monitor`.
	MonitorAddr string

	// LogLevel: "debug", "info", "warn", "error".
	LogLevel string

	// StatsInterval controls how often per-job stats are emitted at INFO.
	StatsInterval time.Duration
}

// ParseFlags binds the shared flag set for the named subcommand and resolves env defaults.
//
// Flag values take precedence; if a flag is left at its zero/default value, an
// env var of the same name (uppercased, dotted-to-underscored) is consulted.
func ParseFlags(sub string, args []string) (Config, error) {
	cfg := Config{Subcommand: sub}
	fs := flag.NewFlagSet(sub, flag.ContinueOnError)

	fs.StringVar(&cfg.DataDir, "data-dir", envOr("AT_SNAPSHOT_DATA_DIR", "./data"),
		"Local working directory for staging, sqlite, logs")
	fs.StringVar(&cfg.ObjectStore, "object-store", envOr("AT_SNAPSHOT_OBJECT_STORE", "local"),
		"Object store backend: local|s3")
	fs.StringVar(&cfg.ObjectStoreRoot, "object-store-root", envOr("AT_SNAPSHOT_OBJECT_STORE_ROOT", "./data/object-store"),
		"Bucket name (s3) or root directory (local)")
	fs.StringVar(&cfg.S3Endpoint, "s3-endpoint", os.Getenv("AT_SNAPSHOT_S3_ENDPOINT"),
		"S3-compatible endpoint URL (R2). Env: AT_SNAPSHOT_S3_ENDPOINT")
	fs.StringVar(&cfg.S3Region, "s3-region", envOr("AT_SNAPSHOT_S3_REGION", "auto"),
		"S3 region. Env: AT_SNAPSHOT_S3_REGION")
	// Keys deliberately read from env only; surfacing them as flags would echo
	// secrets onto the process arg list.
	cfg.S3AccessKeyID = os.Getenv("AT_SNAPSHOT_S3_ACCESS_KEY_ID")
	cfg.S3SecretAccessKey = os.Getenv("AT_SNAPSHOT_S3_SECRET_ACCESS_KEY")

	fs.IntVar(&cfg.LookbackDays, "lookback-days", envOrInt("AT_SNAPSHOT_LOOKBACK_DAYS", 30),
		"Snapshot lookback window in days (snapshot only)")
	fs.StringVar(&cfg.DuckDBMemoryLimit, "duckdb-memory-limit", envOr("AT_SNAPSHOT_DUCKDB_MEMORY_LIMIT", "2GB"),
		"DuckDB working memory cap (snapshot only)")

	fs.StringVar(&cfg.PLCEndpoint, "plc-endpoint", envOr("AT_SNAPSHOT_PLC_ENDPOINT", "https://plc.directory"),
		"PLC directory base URL")
	fs.StringVar(&cfg.ConstellationEndpoint, "constellation-endpoint", envOr("AT_SNAPSHOT_CONSTELLATION_ENDPOINT", "https://constellation.microcosm.blue"),
		"Constellation base URL")
	jsDefault := envOr("AT_SNAPSHOT_JETSTREAM_ENDPOINTS",
		"wss://jetstream1.us-east.bsky.network/subscribe,wss://jetstream2.us-east.bsky.network/subscribe,wss://jetstream1.us-west.bsky.network/subscribe,wss://jetstream2.us-west.bsky.network/subscribe")
	jsCSV := fs.String("jetstream-endpoints", jsDefault,
		"Comma-separated jetstream WS endpoints; failover order")

	langCSV := fs.String("languages", envOr("AT_SNAPSHOT_LANGUAGES", "en"),
		"Comma-separated language filter for posts (empty = no filter)")
	labelerCSV := fs.String("labelers", envOr("AT_SNAPSHOT_LABELERS", "did:plc:ar7c4by46qjdydhdevvrndac"),
		"Comma-separated labeler DIDs to honor (empty = no filter)")
	includeLabelsCSV := fs.String("include-labels", os.Getenv("AT_SNAPSHOT_INCLUDE_LABELS"),
		"Comma-separated labels to include (empty = no allow-list)")
	excludeLabelsCSV := fs.String("exclude-labels", os.Getenv("AT_SNAPSHOT_EXCLUDE_LABELS"),
		"Comma-separated labels to drop")

	fs.IntVar(&cfg.Concurrency, "concurrency", envOrInt("AT_SNAPSHOT_CONCURRENCY", 16),
		"Parallel worker count for fan-out fetches")
	fs.StringVar(&cfg.MonitorAddr, "addr", envOr("AT_SNAPSHOT_MONITOR_ADDR", ":8080"),
		"Monitor HTTP listen address (monitor only)")
	fs.StringVar(&cfg.LogLevel, "log-level", envOr("AT_SNAPSHOT_LOG_LEVEL", "info"),
		"Log level: debug|info|warn|error")
	fs.DurationVar(&cfg.StatsInterval, "stats-interval", envOrDuration("AT_SNAPSHOT_STATS_INTERVAL", 30*time.Second),
		"Periodic stats emission interval")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	cfg.JetstreamEndpoints = splitCSV(*jsCSV)
	cfg.Languages = splitCSV(*langCSV)
	cfg.Labelers = splitCSV(*labelerCSV)
	cfg.IncludeLabels = splitCSV(*includeLabelsCSV)
	cfg.ExcludeLabels = splitCSV(*excludeLabelsCSV)

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

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
