package build

import (
	"encoding/json"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

// LatestJSON is the on-disk shape of latest.json defined in spec §3.1.
//
// The intent: any field MAY be omitted from the rendered JSON when zero
// (forward-compat per the spec's last paragraph of §3.1). Fields consumers
// MUST tolerate being missing: anything outside
//
//	{schema_version, built_at, current_graph.url, current_all.url, archive_base}.
type LatestJSON struct {
	SchemaVersion        string       `json:"schema_version"`
	BuiltAt              time.Time    `json:"built_at"`
	BuildMode            string       `json:"build_mode,omitempty"`
	BuildDurationSeconds int64        `json:"build_duration_seconds,omitempty"`
	CurrentGraph         *ArtifactRef `json:"current_graph,omitempty"`
	CurrentAll           *ArtifactRef `json:"current_all,omitempty"`
	Registry             *RegistryRef `json:"registry,omitempty"`

	ArchiveBase         string `json:"archive_base,omitempty"`
	BootstrapBase       string `json:"bootstrap_base,omitempty"`
	LatestBootstrapDate string `json:"latest_bootstrap_date,omitempty"`
	OldestDailyDate     string `json:"oldest_daily_date,omitempty"`
	NewestDailyDate     string `json:"newest_daily_date,omitempty"`

	JetstreamCursor   int64  `json:"jetstream_cursor,omitempty"`
	JetstreamEndpoint string `json:"jetstream_endpoint,omitempty"`

	RowCounts    map[string]int64 `json:"row_counts,omitempty"`
	FilterConfig json.RawMessage  `json:"filter_config,omitempty"`
}

// ArtifactRef points at a published DuckDB snapshot.
type ArtifactRef struct {
	URL       string `json:"url"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

// RegistryRef points at registry/actors.parquet.
type RegistryRef struct {
	URL       string `json:"url"`
	SizeBytes int64  `json:"size_bytes"`
	RowCount  int64  `json:"row_count"`
}

// filterConfigForJSON returns the filter_config blob shaped for latest.json.
// Returns nil if everything is at default (omitted from JSON).
func filterConfigForJSON(f config.FilterConfig) json.RawMessage {
	s, err := marshalFilters(f)
	if err != nil || s == "" {
		return nil
	}
	return json.RawMessage(s)
}
