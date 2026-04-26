// Package intern provides deterministic 64-bit ids for the long opaque strings
// (DIDs and post URIs) that dominate the data model.
//
// Choosing the id function carefully matters because no central allocator is
// shared across the bootstrap, run, and snapshot processes — each one
// independently computes ids for the strings it sees, and they need to agree
// without coordination. xxhash64 satisfies that with negligible collision
// probability at the scales we expect (a few hundred million strings):
//
//	birthday bound for 1e9 inputs in 2^64 space ≈ 2.7e-2 collisions
//	expected, dominated by a handful of pairs at most. Snapshot keeps the
//	original string alongside the id in the actors / posts tables so any
//	collision is detectable and reportable downstream rather than silent.
//
// All ids are returned as int64 (DuckDB's BIGINT is signed) by reinterpreting
// the bits — sign is irrelevant for an opaque identifier and avoids needing a
// 65-bit type.
package intern

import "github.com/cespare/xxhash/v2"

// ID64 returns a deterministic 64-bit id for s, suitable for use as a BIGINT
// primary key. Empty string maps to 0 to keep "no value" obvious in queries.
func ID64(s string) int64 {
	if s == "" {
		return 0
	}
	return int64(xxhash.Sum64String(s))
}

// DIDID returns the interned id for a DID. Thin alias kept for call-site clarity.
func DIDID(did string) int64 { return ID64(did) }

// URIID returns the interned id for a post URI.
func URIID(uri string) int64 { return ID64(uri) }
