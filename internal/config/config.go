package config

import "time"

type Config struct {
	RelayHost    string        // e.g. "https://bsky.network"
	Workers      int           // parallel getRepo workers
	RateLimitRPS float64       // global rate limit (req/s)
	HTTPTimeout  time.Duration // per-request timeout
	DataDir      string        // local working directory
	DIDLimit     int           // cap backfill at this many DIDs (0 = no cap)

	// ----- run-mode (Jetstream consumer) settings -----

	// Jetstream is the configuration for `at-snapshotter run`.
	Jetstream JetstreamConfig

	// Retention controls archive pruning.
	Retention RetentionConfig

	// Filters configures build-time filtering.
	Filters FilterConfig
}

// RetentionConfig controls how long archive artifacts live in the object
// store. See specs/001_bootstrap.md §9 / §11.
type RetentionConfig struct {
	// ParquetDays bounds how many days of `daily/*` are retained in the
	// object store. Bootstraps are NEVER pruned.
	ParquetDays int
}

// FilterConfig captures build-time filters per spec §11. The parquet
// archive is unaffected — filters apply only when assembling the snapshot.
type FilterConfig struct {
	Posts               PostFilter
	ExcludeCollections  []string
}

// PostFilter selects which post rows survive into current_all.duckdb.
type PostFilter struct {
	// Langs is an allowlist of BCP-47 codes. nil/empty disables filtering.
	Langs []string
	// ExcludeNoLang drops rows with NULL lang when Langs is set.
	ExcludeNoLang bool
}

// JetstreamConfig configures the Jetstream WebSocket consumer.
type JetstreamConfig struct {
	// Endpoints is the ordered failover list. Defaults set in DefaultJetstream().
	Endpoints []string

	// WantedCollections lists the AT-Proto NSIDs to subscribe to.
	WantedCollections []string

	// Compress requests zstd-compressed frames from the server.
	Compress bool

	// RewindSeconds is how many seconds the cursor is rewound on
	// reconnect / failover to cover clock skew between Jetstream instances.
	RewindSeconds int

	// CheckpointEveryRows triggers a SQLite checkpoint when this many
	// staging rows have been inserted since the last checkpoint.
	CheckpointEveryRows int

	// CheckpointInterval is the maximum wall-clock duration between
	// SQLite checkpoints (so cursor can advance even when traffic is light).
	CheckpointInterval time.Duration

	// RolloverGrace is the grace window after UTC midnight before sealing
	// the prior day's parquet shards.
	RolloverGrace time.Duration

	// MinFreeBytes is the minimum free disk on `DataDir` below which
	// the consumer stops accepting new events.
	MinFreeBytes int64

	// MaxStagingBytes is the size of `staging.db` that triggers a warning.
	MaxStagingBytes int64

	// LagAlarm is the `now - cursor` lag at which a warning is logged.
	LagAlarm time.Duration

	// NoUpload disables object-store uploads even when an ObjectStore is wired in.
	NoUpload bool
}

// DefaultJetstream returns the spec-defined defaults for the Jetstream consumer.
func DefaultJetstream() JetstreamConfig {
	return JetstreamConfig{
		Endpoints: []string{
			"wss://jetstream2.us-east.bsky.network/subscribe",
			"wss://jetstream1.us-east.bsky.network/subscribe",
			"wss://jetstream2.us-west.bsky.network/subscribe",
			"wss://jetstream1.us-west.bsky.network/subscribe",
		},
		WantedCollections: []string{
			"app.bsky.actor.profile",
			"app.bsky.feed.post",
			"app.bsky.feed.like",
			"app.bsky.feed.repost",
			"app.bsky.graph.follow",
			"app.bsky.graph.block",
		},
		// Compress requests zstd-compressed frames; the consumer ships the
		// upstream zstd dictionary (see internal/run/zstd_dictionary) so the
		// decoder can interpret frames correctly.
		Compress:            true,
		RewindSeconds:       10,
		CheckpointEveryRows: 10_000,
		CheckpointInterval:  60 * time.Second,
		RolloverGrace:       10 * time.Minute,
		MinFreeBytes:        10 * 1024 * 1024 * 1024, // 10 GB
		MaxStagingBytes:     3 * 1024 * 1024 * 1024,  // 3 GB
		LagAlarm:            5 * time.Minute,
		NoUpload:            false,
	}
}

func Default() Config {
	return Config{
		RelayHost:    "https://bsky.network",
		Workers:      64,
		RateLimitRPS: 40,
		HTTPTimeout:  15 * time.Second,
		DataDir:      "./data",
		DIDLimit:     10_000,
		Jetstream:    DefaultJetstream(),
		Retention:    RetentionConfig{ParquetDays: 90},
		Filters:      FilterConfig{},
	}
}
