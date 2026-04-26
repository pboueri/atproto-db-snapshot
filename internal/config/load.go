package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// ExtendedConfig is the full, YAML-driven configuration. It embeds the
// existing Config so callers that already accept *Config keep working,
// while exposing the additional spec sections that don't yet have a home
// on Config (object_store, paths, duckdb, serve).
//
// Merge rule: the structured sections (Paths, …) are the source of
// truth when populated from YAML. After Load() they are reconciled into
// the embedded Config so legacy code paths (Config.DataDir,
// Config.Filters, Config.Retention) see fresh values.
//
// The Relay block is kept as a tolerated, vestigial YAML section so
// pre-existing config.yamls don't fail to parse; it is no longer
// reconciled into Config (spec 003 removed RelayHost/Workers/RateLimitRPS).
type ExtendedConfig struct {
	Config `yaml:"-"`

	ObjectStore   ObjectStoreConfig       `yaml:"object_store"`
	Relay         RelayConfig             `yaml:"relay"`
	PLC           PLCYAMLConfig           `yaml:"plc"`
	PDS           PDSYAMLConfig           `yaml:"pds"`
	Constellation ConstellationYAMLConfig `yaml:"constellation"`
	Jetstream     JetstreamYAMLConfig     `yaml:"jetstream"`
	Retention     RetentionYAMLConfig     `yaml:"retention"`
	Filters       FiltersYAMLConfig       `yaml:"filters"`
	Paths         PathsConfig             `yaml:"paths"`
	DuckDB        DuckDBConfig            `yaml:"duckdb"`
	Serve         ServeConfig             `yaml:"serve"`
}

// PLCYAMLConfig mirrors the `plc:` block.
type PLCYAMLConfig struct {
	Endpoint    string  `yaml:"endpoint"`
	PageSize    int     `yaml:"page_size"`
	RPS         float64 `yaml:"rps"`
	RefreshDays int     `yaml:"refresh_days"`
}

// PDSYAMLConfig mirrors the `pds:` block.
type PDSYAMLConfig struct {
	PerHostWorkers   int     `yaml:"per_host_workers"`
	PerHostRPS       float64 `yaml:"per_host_rps"`
	HTTPTimeout      string  `yaml:"http_timeout"`
	MaxRetries       int     `yaml:"max_retries"`
	BreakerThreshold int     `yaml:"breaker_threshold"`
	BreakerCooldown  string  `yaml:"breaker_cooldown"`
}

// ConstellationYAMLConfig mirrors the `constellation:` block.
type ConstellationYAMLConfig struct {
	Enabled  *bool   `yaml:"enabled"`
	Endpoint string  `yaml:"endpoint"`
	RPS      float64 `yaml:"rps"`
}

// ObjectStoreConfig is the `object_store:` block. Credentials are sourced
// from environment variables (OS_ACCESS_KEY / OS_SECRET_KEY) and are
// never read here.
type ObjectStoreConfig struct {
	Type          string `yaml:"type"` // r2 | s3 | gcs | file
	Bucket        string `yaml:"bucket"`
	Endpoint      string `yaml:"endpoint"`
	PublicURLBase string `yaml:"public_url_base"`
}

// RelayConfig is the `relay:` block.
type RelayConfig struct {
	Host             string  `yaml:"host"`
	ListReposWorkers int     `yaml:"listrepos_workers"`
	RateLimitRPS     float64 `yaml:"rate_limit_rps"`
}

// JetstreamYAMLConfig mirrors the `jetstream:` block in YAML form.
// It has explicit yaml tags so spec-shaped snake_case keys match. The
// values are merged into the embedded Config.Jetstream at the end of
// Load (only non-zero overrides are applied, defaults are preserved).
type JetstreamYAMLConfig struct {
	Endpoints           []string `yaml:"endpoints"`
	WantedCollections   []string `yaml:"wanted_collections"`
	Compress            *bool    `yaml:"compress"`
	RewindSeconds       int      `yaml:"rewind_seconds"`
	CheckpointEveryRows int      `yaml:"checkpoint_every_rows"`
	CheckpointInterval  string   `yaml:"checkpoint_interval"`
	RolloverGrace       string   `yaml:"rollover_grace"`
	LagAlarm            string   `yaml:"lag_alarm"`
	MinFreeBytes        int64    `yaml:"min_free_bytes"`
	MaxStagingBytes     int64    `yaml:"max_staging_bytes"`
	NoUpload            *bool    `yaml:"no_upload"`
}

// RetentionYAMLConfig mirrors the `retention:` block. Reuses the
// existing RetentionConfig (defined on Config) at reconcile time.
type RetentionYAMLConfig struct {
	ParquetDays int `yaml:"parquet_days"`
}

// FiltersYAMLConfig mirrors the `filters:` block in YAML. Merged into
// Config.Filters (the existing FilterConfig type) at reconcile time.
type FiltersYAMLConfig struct {
	Posts              PostsYAMLFilter `yaml:"posts"`
	ExcludeCollections []string        `yaml:"exclude_collections"`
}

// PostsYAMLFilter is `filters.posts`.
type PostsYAMLFilter struct {
	Langs         []string `yaml:"langs"`
	ExcludeNoLang bool     `yaml:"exclude_no_lang"`
}

// PathsConfig is the `paths:` block.
type PathsConfig struct {
	DataDir string `yaml:"data_dir"`
	TempDir string `yaml:"temp_dir"`
}

// DuckDBConfig is the `duckdb:` block.
type DuckDBConfig struct {
	MemoryLimit string `yaml:"memory_limit"`
	Threads     int    `yaml:"threads"`
}

// ServeConfig is the `serve:` block.
type ServeConfig struct {
	Listen         string `yaml:"listen"`
	RefreshSeconds int    `yaml:"refresh_seconds"`
	LogTailLines   int    `yaml:"log_tail_lines"`
}

// DefaultExtended returns a fully populated ExtendedConfig with all
// spec defaults applied. YAML overlay sits on top of this.
func DefaultExtended() ExtendedConfig {
	base := Default()
	return ExtendedConfig{
		Config: base,
		ObjectStore: ObjectStoreConfig{
			Type: "file",
		},
		// Relay is a vestigial YAML section retained only so legacy
		// config files don't fail to parse. Not reconciled into Config.
		Relay: RelayConfig{
			Host:             "bsky.network",
			ListReposWorkers: 150,
			RateLimitRPS:     80,
		},
		PLC: PLCYAMLConfig{
			Endpoint:    base.PLC.Endpoint,
			PageSize:    base.PLC.PageSize,
			RPS:         base.PLC.RPS,
			RefreshDays: base.PLC.RefreshDays,
		},
		PDS: PDSYAMLConfig{
			PerHostWorkers:   base.PDS.PerHostWorkers,
			PerHostRPS:       base.PDS.PerHostRPS,
			HTTPTimeout:      base.PDS.HTTPTimeout.String(),
			MaxRetries:       base.PDS.MaxRetries,
			BreakerThreshold: base.PDS.BreakerThreshold,
			BreakerCooldown:  base.PDS.BreakerCooldown.String(),
		},
		Constellation: ConstellationYAMLConfig{
			Enabled:  ptrBool(base.Constellation.Enabled),
			Endpoint: base.Constellation.Endpoint,
			RPS:      base.Constellation.RPS,
		},
		Retention: RetentionYAMLConfig{
			ParquetDays: base.Retention.ParquetDays,
		},
		Filters: FiltersYAMLConfig{
			Posts: PostsYAMLFilter{
				Langs:         nil,
				ExcludeNoLang: false,
			},
			ExcludeCollections: nil,
		},
		Paths: PathsConfig{
			DataDir: "./data",
			TempDir: "./data/tmp",
		},
		DuckDB: DuckDBConfig{
			MemoryLimit: "12GB",
			Threads:     4,
		},
		Serve: ServeConfig{
			Listen:         "127.0.0.1:8080",
			RefreshSeconds: 5,
			LogTailLines:   20000,
		},
	}
}

func ptrBool(b bool) *bool { return &b }

// Load reads a YAML file at path, overlays it on top of DefaultExtended,
// and reconciles structured sections back onto the embedded Config so
// callers that consume the flat fields stay in sync.
//
// YAML keys that are absent leave the corresponding default in place.
// YAML keys that are present and non-zero override defaults. Empty
// slices / zero scalars from YAML are treated the same as "absent" for
// fields that have a meaningful default — this avoids accidentally
// stomping defaults with a half-specified config.
func Load(path string) (ExtendedConfig, error) {
	cfg := DefaultExtended()

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config %s: %w", path, err)
	}

	// Unmarshal into a fresh overlay so we can detect which fields the
	// YAML actually set vs. which are still zero. The overlay starts
	// fully zero-valued; only fields present in the YAML get populated.
	var overlay ExtendedConfig
	if err := yaml.Unmarshal(data, &overlay); err != nil {
		return cfg, fmt.Errorf("parse config %s: %w", path, err)
	}

	mergeExtended(&cfg, &overlay)

	if err := reconcileFlatConfig(&cfg); err != nil {
		return cfg, fmt.Errorf("reconcile config %s: %w", path, err)
	}

	return cfg, nil
}

// mergeExtended overlays non-zero fields from src onto dst. dst already
// holds the defaults; src holds the freshly-unmarshaled YAML.
func mergeExtended(dst, src *ExtendedConfig) {
	// object_store
	if src.ObjectStore.Type != "" {
		dst.ObjectStore.Type = src.ObjectStore.Type
	}
	if src.ObjectStore.Bucket != "" {
		dst.ObjectStore.Bucket = src.ObjectStore.Bucket
	}
	if src.ObjectStore.Endpoint != "" {
		dst.ObjectStore.Endpoint = src.ObjectStore.Endpoint
	}
	if src.ObjectStore.PublicURLBase != "" {
		dst.ObjectStore.PublicURLBase = src.ObjectStore.PublicURLBase
	}

	// relay (vestigial — accept overrides for forward-compat parsing only)
	if src.Relay.Host != "" {
		dst.Relay.Host = src.Relay.Host
	}
	if src.Relay.ListReposWorkers != 0 {
		dst.Relay.ListReposWorkers = src.Relay.ListReposWorkers
	}
	if src.Relay.RateLimitRPS != 0 {
		dst.Relay.RateLimitRPS = src.Relay.RateLimitRPS
	}

	// plc
	if src.PLC.Endpoint != "" {
		dst.PLC.Endpoint = src.PLC.Endpoint
	}
	if src.PLC.PageSize != 0 {
		dst.PLC.PageSize = src.PLC.PageSize
	}
	if src.PLC.RPS != 0 {
		dst.PLC.RPS = src.PLC.RPS
	}
	if src.PLC.RefreshDays != 0 {
		dst.PLC.RefreshDays = src.PLC.RefreshDays
	}

	// pds
	if src.PDS.PerHostWorkers != 0 {
		dst.PDS.PerHostWorkers = src.PDS.PerHostWorkers
	}
	if src.PDS.PerHostRPS != 0 {
		dst.PDS.PerHostRPS = src.PDS.PerHostRPS
	}
	if src.PDS.HTTPTimeout != "" {
		dst.PDS.HTTPTimeout = src.PDS.HTTPTimeout
	}
	if src.PDS.MaxRetries != 0 {
		dst.PDS.MaxRetries = src.PDS.MaxRetries
	}
	if src.PDS.BreakerThreshold != 0 {
		dst.PDS.BreakerThreshold = src.PDS.BreakerThreshold
	}
	if src.PDS.BreakerCooldown != "" {
		dst.PDS.BreakerCooldown = src.PDS.BreakerCooldown
	}

	// constellation
	if src.Constellation.Enabled != nil {
		dst.Constellation.Enabled = src.Constellation.Enabled
	}
	if src.Constellation.Endpoint != "" {
		dst.Constellation.Endpoint = src.Constellation.Endpoint
	}
	if src.Constellation.RPS != 0 {
		dst.Constellation.RPS = src.Constellation.RPS
	}

	// jetstream — merge into the YAML view, then push into Config.Jetstream
	// in reconcileFlatConfig.
	if len(src.Jetstream.Endpoints) != 0 {
		dst.Jetstream.Endpoints = src.Jetstream.Endpoints
	}
	if len(src.Jetstream.WantedCollections) != 0 {
		dst.Jetstream.WantedCollections = src.Jetstream.WantedCollections
	}
	if src.Jetstream.Compress != nil {
		dst.Jetstream.Compress = src.Jetstream.Compress
	}
	if src.Jetstream.RewindSeconds != 0 {
		dst.Jetstream.RewindSeconds = src.Jetstream.RewindSeconds
	}
	if src.Jetstream.CheckpointEveryRows != 0 {
		dst.Jetstream.CheckpointEveryRows = src.Jetstream.CheckpointEveryRows
	}
	if src.Jetstream.CheckpointInterval != "" {
		dst.Jetstream.CheckpointInterval = src.Jetstream.CheckpointInterval
	}
	if src.Jetstream.RolloverGrace != "" {
		dst.Jetstream.RolloverGrace = src.Jetstream.RolloverGrace
	}
	if src.Jetstream.LagAlarm != "" {
		dst.Jetstream.LagAlarm = src.Jetstream.LagAlarm
	}
	if src.Jetstream.MinFreeBytes != 0 {
		dst.Jetstream.MinFreeBytes = src.Jetstream.MinFreeBytes
	}
	if src.Jetstream.MaxStagingBytes != 0 {
		dst.Jetstream.MaxStagingBytes = src.Jetstream.MaxStagingBytes
	}
	if src.Jetstream.NoUpload != nil {
		dst.Jetstream.NoUpload = src.Jetstream.NoUpload
	}

	// retention
	if src.Retention.ParquetDays != 0 {
		dst.Retention.ParquetDays = src.Retention.ParquetDays
	}

	// filters
	if len(src.Filters.Posts.Langs) != 0 {
		dst.Filters.Posts.Langs = src.Filters.Posts.Langs
	}
	// ExcludeNoLang is a bool with a meaningful zero — only the YAML's
	// explicit presence should override. yaml.v3 doesn't expose presence
	// for plain bool, so we treat any decoded value as authoritative
	// when the YAML had a `posts:` block at all. Practically: if Langs
	// or ExcludeNoLang is set in src, accept ExcludeNoLang from src.
	if len(src.Filters.Posts.Langs) != 0 || src.Filters.Posts.ExcludeNoLang {
		dst.Filters.Posts.ExcludeNoLang = src.Filters.Posts.ExcludeNoLang
	}
	if src.Filters.ExcludeCollections != nil {
		dst.Filters.ExcludeCollections = src.Filters.ExcludeCollections
	}

	// paths
	if src.Paths.DataDir != "" {
		dst.Paths.DataDir = src.Paths.DataDir
	}
	if src.Paths.TempDir != "" {
		dst.Paths.TempDir = src.Paths.TempDir
	}

	// duckdb
	if src.DuckDB.MemoryLimit != "" {
		dst.DuckDB.MemoryLimit = src.DuckDB.MemoryLimit
	}
	if src.DuckDB.Threads != 0 {
		dst.DuckDB.Threads = src.DuckDB.Threads
	}

	// serve
	if src.Serve.Listen != "" {
		dst.Serve.Listen = src.Serve.Listen
	}
	if src.Serve.RefreshSeconds != 0 {
		dst.Serve.RefreshSeconds = src.Serve.RefreshSeconds
	}
	if src.Serve.LogTailLines != 0 {
		dst.Serve.LogTailLines = src.Serve.LogTailLines
	}
}

// reconcileFlatConfig pushes structured-section values into the legacy
// flat fields on the embedded Config so existing call sites stay valid.
// Only non-zero structured values overwrite the flat field; zero values
// preserve whatever Default() seeded.
func reconcileFlatConfig(cfg *ExtendedConfig) error {
	// PLC → Config.PLC
	if cfg.PLC.Endpoint != "" {
		cfg.Config.PLC.Endpoint = cfg.PLC.Endpoint
	}
	if cfg.PLC.PageSize != 0 {
		cfg.Config.PLC.PageSize = cfg.PLC.PageSize
	}
	if cfg.PLC.RPS != 0 {
		cfg.Config.PLC.RPS = cfg.PLC.RPS
	}
	if cfg.PLC.RefreshDays != 0 {
		cfg.Config.PLC.RefreshDays = cfg.PLC.RefreshDays
	}

	// PDS → Config.PDS
	if cfg.PDS.PerHostWorkers != 0 {
		cfg.Config.PDS.PerHostWorkers = cfg.PDS.PerHostWorkers
	}
	if cfg.PDS.PerHostRPS != 0 {
		cfg.Config.PDS.PerHostRPS = cfg.PDS.PerHostRPS
	}
	if cfg.PDS.HTTPTimeout != "" {
		d, err := time.ParseDuration(cfg.PDS.HTTPTimeout)
		if err != nil {
			return fmt.Errorf("pds.http_timeout: %w", err)
		}
		cfg.Config.PDS.HTTPTimeout = d
	}
	if cfg.PDS.MaxRetries != 0 {
		cfg.Config.PDS.MaxRetries = cfg.PDS.MaxRetries
	}
	if cfg.PDS.BreakerThreshold != 0 {
		cfg.Config.PDS.BreakerThreshold = cfg.PDS.BreakerThreshold
	}
	if cfg.PDS.BreakerCooldown != "" {
		d, err := time.ParseDuration(cfg.PDS.BreakerCooldown)
		if err != nil {
			return fmt.Errorf("pds.breaker_cooldown: %w", err)
		}
		cfg.Config.PDS.BreakerCooldown = d
	}

	// Constellation → Config.Constellation
	if cfg.Constellation.Enabled != nil {
		cfg.Config.Constellation.Enabled = *cfg.Constellation.Enabled
	}
	if cfg.Constellation.Endpoint != "" {
		cfg.Config.Constellation.Endpoint = cfg.Constellation.Endpoint
	}
	if cfg.Constellation.RPS != 0 {
		cfg.Config.Constellation.RPS = cfg.Constellation.RPS
	}

	// Paths → Config.DataDir
	if cfg.Paths.DataDir != "" {
		cfg.Config.DataDir = cfg.Paths.DataDir
	}

	// Retention → Config.Retention
	if cfg.Retention.ParquetDays != 0 {
		cfg.Config.Retention.ParquetDays = cfg.Retention.ParquetDays
	}

	// Filters → Config.Filters (FilterConfig + PostFilter)
	if len(cfg.Filters.Posts.Langs) != 0 {
		cfg.Config.Filters.Posts.Langs = cfg.Filters.Posts.Langs
	}
	if cfg.Filters.Posts.ExcludeNoLang {
		cfg.Config.Filters.Posts.ExcludeNoLang = cfg.Filters.Posts.ExcludeNoLang
	}
	if cfg.Filters.ExcludeCollections != nil {
		cfg.Config.Filters.ExcludeCollections = cfg.Filters.ExcludeCollections
	}

	// Jetstream YAML view → Config.Jetstream
	js := &cfg.Config.Jetstream
	yjs := &cfg.Jetstream
	if len(yjs.Endpoints) != 0 {
		js.Endpoints = yjs.Endpoints
	}
	if len(yjs.WantedCollections) != 0 {
		js.WantedCollections = yjs.WantedCollections
	}
	if yjs.Compress != nil {
		js.Compress = *yjs.Compress
	}
	if yjs.RewindSeconds != 0 {
		js.RewindSeconds = yjs.RewindSeconds
	}
	if yjs.CheckpointEveryRows != 0 {
		js.CheckpointEveryRows = yjs.CheckpointEveryRows
	}
	if yjs.CheckpointInterval != "" {
		d, err := time.ParseDuration(yjs.CheckpointInterval)
		if err != nil {
			return fmt.Errorf("jetstream.checkpoint_interval: %w", err)
		}
		js.CheckpointInterval = d
	}
	if yjs.RolloverGrace != "" {
		d, err := time.ParseDuration(yjs.RolloverGrace)
		if err != nil {
			return fmt.Errorf("jetstream.rollover_grace: %w", err)
		}
		js.RolloverGrace = d
	}
	if yjs.LagAlarm != "" {
		d, err := time.ParseDuration(yjs.LagAlarm)
		if err != nil {
			return fmt.Errorf("jetstream.lag_alarm: %w", err)
		}
		js.LagAlarm = d
	}
	if yjs.MinFreeBytes != 0 {
		js.MinFreeBytes = yjs.MinFreeBytes
	}
	if yjs.MaxStagingBytes != 0 {
		js.MaxStagingBytes = yjs.MaxStagingBytes
	}
	if yjs.NoUpload != nil {
		js.NoUpload = *yjs.NoUpload
	}

	return nil
}
