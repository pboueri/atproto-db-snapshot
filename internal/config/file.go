package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// fileConfig is the on-disk shape parsed from at-snapshot.yaml.
//
// It mirrors the runtime Config but only with fields that are sensible to
// pin in a versioned operations file. Secrets (S3 access keys) are read
// here but loaded into the env-only Config fields downstream so the
// resulting Config layout stays uniform.
//
// Keys are canonical lowercase snake_case. The custom UnmarshalYAML
// normalizes user-provided keys (case + spaces + hyphens → underscores)
// before matching, so a file written with BACKEND, Access Key, or
// access-key all map to the same field.
type fileConfig struct {
	// Object storage. backend is local | s3 | r2 | memory; r2 is an alias
	// for s3 with sensible defaults filled in.
	Backend         string `yaml:"backend"`
	Bucket          string `yaml:"bucket"`
	URL             string `yaml:"url"`
	Region          string `yaml:"region"`
	AccessKey       string `yaml:"access_key"`
	SecretAccessKey string `yaml:"secret_access_key"`

	// Local working state.
	DataDir string `yaml:"data_dir"`

	// Snapshot.
	LookbackDays      int    `yaml:"lookback_days"`
	DuckDBMemoryLimit string `yaml:"duckdb_memory_limit"`

	// Filtering.
	Languages     []string `yaml:"languages"`
	Labelers      []string `yaml:"labelers"`
	IncludeLabels []string `yaml:"include_labels"`
	ExcludeLabels []string `yaml:"exclude_labels"`

	// Sources.
	PLCEndpoint        string   `yaml:"plc_endpoint"`
	JetstreamEndpoints []string `yaml:"jetstream_endpoints"`

	// Concurrency / timing.
	Concurrency    int           `yaml:"concurrency"`
	StatsInterval  time.Duration `yaml:"stats_interval"`
	MaxDIDs        int           `yaml:"max_dids"`
	RunDuration    time.Duration `yaml:"run_duration"`
	PDSRateLimit   float64       `yaml:"pds_rate_limit"`
	PDSBurst       int           `yaml:"pds_burst"`

	// Microcosm services (slingshot + constellation) used by bootstrap.
	ConstellationEndpoint string  `yaml:"constellation_endpoint"`
	SlingshotEndpoint     string  `yaml:"slingshot_endpoint"`
	MicrocosmRateLimit    float64 `yaml:"microcosm_rate_limit"`
	MicrocosmBurst        int     `yaml:"microcosm_burst"`
	ConstellationPageSize int     `yaml:"constellation_page_size"`
	Contact               string  `yaml:"contact"`

	// Monitor.
	MonitorAddr string `yaml:"monitor_addr"`

	// Misc.
	LogLevel string `yaml:"log_level"`
}

// UnmarshalYAML normalizes keys (case, spaces, hyphens) before matching, so a
// human-friendly file like:
//
//	BACKEND: R2
//	Access Key: ...
//
// loads into the same fields as the canonical:
//
//	backend: r2
//	access_key: ...
func (c *fileConfig) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("config: expected a YAML mapping at top level")
	}
	out := map[string]*yaml.Node{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		k := normalizeKey(node.Content[i].Value)
		out[k] = node.Content[i+1]
	}
	mappings := map[string]any{
		"backend":             &c.Backend,
		"bucket":              &c.Bucket,
		"url":                 &c.URL,
		"region":              &c.Region,
		"access_key":          &c.AccessKey,
		"access_key_id":       &c.AccessKey,
		"secret_access_key":   &c.SecretAccessKey,
		"secret_key":          &c.SecretAccessKey,
		"data_dir":            &c.DataDir,
		"lookback_days":       &c.LookbackDays,
		"duckdb_memory_limit": &c.DuckDBMemoryLimit,
		"languages":           &c.Languages,
		"labelers":            &c.Labelers,
		"include_labels":      &c.IncludeLabels,
		"exclude_labels":      &c.ExcludeLabels,
		"plc_endpoint":        &c.PLCEndpoint,
		"jetstream_endpoints": &c.JetstreamEndpoints,
		"concurrency":         &c.Concurrency,
		"max_dids":            &c.MaxDIDs,
		"pds_rate_limit":         &c.PDSRateLimit,
		"pds_burst":              &c.PDSBurst,
		"constellation_endpoint": &c.ConstellationEndpoint,
		"slingshot_endpoint":     &c.SlingshotEndpoint,
		"microcosm_rate_limit":     &c.MicrocosmRateLimit,
		"microcosm_burst":          &c.MicrocosmBurst,
		"constellation_page_size":  &c.ConstellationPageSize,
		"contact":                  &c.Contact,
		"monitor_addr":           &c.MonitorAddr,
		"log_level":              &c.LogLevel,
	}
	durationFields := map[string]*time.Duration{
		"stats_interval": &c.StatsInterval,
		"run_duration":   &c.RunDuration,
	}
	for key, n := range out {
		if dur, ok := durationFields[key]; ok {
			d, err := decodeDuration(n)
			if err != nil {
				return fmt.Errorf("config: decode %s: %w", key, err)
			}
			*dur = d
			continue
		}
		dst, ok := mappings[key]
		if !ok {
			// Unknown keys are tolerated rather than rejected so a single
			// YAML can carry future fields without breaking older binaries.
			continue
		}
		if err := n.Decode(dst); err != nil {
			return fmt.Errorf("config: decode %s: %w", key, err)
		}
	}
	return nil
}

// decodeDuration accepts either a Go duration string ("30s", "1h") or a bare
// number (interpreted as seconds; "0" is the natural way to write "no
// timeout"). yaml.v3 doesn't coerce bare ints into time.Duration on its own,
// so we handle the conversion here rather than forcing operators to quote
// every zero.
func decodeDuration(n *yaml.Node) (time.Duration, error) {
	var s string
	if err := n.Decode(&s); err == nil && s != "" {
		return time.ParseDuration(s)
	}
	var i int64
	if err := n.Decode(&i); err != nil {
		return 0, err
	}
	return time.Duration(i) * time.Second, nil
}

// normalizeKey lowercases and collapses spaces / hyphens to underscores so
// "Access Key", "access-key", and "ACCESS_KEY" all hash to the same bucket.
func normalizeKey(k string) string {
	k = strings.ToLower(strings.TrimSpace(k))
	k = strings.ReplaceAll(k, " ", "_")
	k = strings.ReplaceAll(k, "-", "_")
	return k
}

// loadFile reads path and returns the parsed config.
//
// A missing file is reported as an error; the caller decides whether the
// path was explicit (error fatal) or implicit (error swallowed).
func loadFile(path string) (fileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return fileConfig{}, err
	}
	var fc fileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return fileConfig{}, fmt.Errorf("config: parse %s: %w", path, err)
	}
	return fc, nil
}

// resolveBackend translates the file's backend label to the runtime
// ObjectStore value. r2 maps to s3 with auto region filled in if blank.
func (c *fileConfig) resolveBackend() (objectStore, region string) {
	region = c.Region
	switch normalizeKey(c.Backend) {
	case "r2":
		objectStore = "s3"
		if region == "" {
			region = "auto"
		}
	case "s3":
		objectStore = "s3"
	case "local", "":
		objectStore = "local"
	case "memory":
		objectStore = "memory"
	default:
		objectStore = c.Backend
	}
	return
}

// resolveEndpoint returns the URL ready to hand to the S3 SDK: scheme is
// prepended if absent (R2 docs typically just give the host), and trailing
// slashes are trimmed.
func (c *fileConfig) resolveEndpoint() string {
	u := strings.TrimSpace(c.URL)
	if u == "" {
		return ""
	}
	if !strings.Contains(u, "://") {
		u = "https://" + u
	}
	if parsed, err := url.Parse(u); err == nil {
		parsed.Path = strings.TrimRight(parsed.Path, "/")
		return parsed.String()
	}
	return strings.TrimRight(u, "/")
}
