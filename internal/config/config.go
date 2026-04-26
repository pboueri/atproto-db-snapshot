package config

import "time"

type Config struct {
	HTTPTimeout time.Duration // per-request timeout for the PLC client
	DataDir     string        // local working directory
	DIDLimit    int           // cap backfill at this many DIDs (0 = no cap)

	// ----- listRecords graph backfill (spec 003) -----

	PLC           PLCConfig
	PDS           PDSConfig
	Constellation ConstellationConfig

	// ----- run-mode (Jetstream consumer) settings -----

	// Jetstream is the configuration for `at-snapshotter run`.
	Jetstream JetstreamConfig

	// Retention controls archive pruning.
	Retention RetentionConfig

	// Filters configures build-time filtering.
	Filters FilterConfig
}

// PLCConfig configures the plc.directory enumeration phase.
type PLCConfig struct {
	Endpoint    string  // default "https://plc.directory"
	PageSize    int     // default 1000
	RPS         float64 // default 5
	RefreshDays int     // default 7 — re-dump PLC if older
}

// PDSConfig configures the per-PDS listRecords dispatcher.
type PDSConfig struct {
	PerHostWorkers   int           // default 8
	PerHostRPS       float64       // default 9
	HTTPTimeout      time.Duration // default 10s
	MaxRetries       int           // default 4
	BreakerThreshold int           // default 5 consecutive failures
	BreakerCooldown  time.Duration // default 5m
}

// ConstellationConfig configures the optional /links/all enrichment pass.
type ConstellationConfig struct {
	Enabled  bool    // default false (must be explicitly enabled)
	Endpoint string  // default "https://constellation.microcosm.blue"
	RPS      float64 // default 5
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
	Posts              PostFilter
	ExcludeCollections []string
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

// DefaultPLC returns the spec defaults for the PLC enumerator.
func DefaultPLC() PLCConfig {
	return PLCConfig{
		Endpoint:    "https://plc.directory",
		PageSize:    1000,
		RPS:         5,
		RefreshDays: 7,
	}
}

// DefaultPDS returns the spec defaults for the per-host listRecords pool.
func DefaultPDS() PDSConfig {
	return PDSConfig{
		PerHostWorkers:   8,
		PerHostRPS:       9,
		HTTPTimeout:      10 * time.Second,
		MaxRetries:       4,
		BreakerThreshold: 5,
		BreakerCooldown:  5 * time.Minute,
	}
}

// DefaultConstellation returns the spec defaults for the enrichment pass.
func DefaultConstellation() ConstellationConfig {
	return ConstellationConfig{
		Enabled:  false,
		Endpoint: "https://constellation.microcosm.blue",
		RPS:      5,
	}
}

func Default() Config {
	return Config{
		HTTPTimeout:   30 * time.Second,
		DataDir:       "./data",
		DIDLimit:      10_000,
		PLC:           DefaultPLC(),
		PDS:           DefaultPDS(),
		Constellation: DefaultConstellation(),
		Jetstream:     DefaultJetstream(),
		Retention:     RetentionConfig{ParquetDays: 90},
		Filters:       FilterConfig{},
	}
}
