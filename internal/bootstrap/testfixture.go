package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2" // register duckdb driver

	"github.com/pboueri/atproto-db-snapshot/internal/model"
)

// WriteFixture creates a social_graph.duckdb at path with the canonical
// bootstrap schema and the requested row counts. It is intended for use by
// other packages' tests (notably internal/monitor) that need a realistic
// duckdb file without recreating the schema string.
//
// The number of rows is the only knob; values are synthetic and not
// expected to be valid for downstream consumers — but counts and PK
// uniqueness are honored so the duckdb file is a faithful shape match.
func WriteFixture(ctx context.Context, path string, actors, follows, blocks int) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}

	now := time.Now().UTC()
	if _, err := db.ExecContext(ctx,
		"INSERT INTO bootstrap_meta(started_at, completed_at, plc_endpoint, constellation_endpoint) VALUES (?, ?, ?, ?)",
		now.Add(-time.Hour), now, "fixture-plc", "fixture-constellation",
	); err != nil {
		return fmt.Errorf("insert meta: %w", err)
	}

	for i := 0; i < actors; i++ {
		did := fmt.Sprintf("did:plc:fixture%d", i)
		if _, err := db.ExecContext(ctx,
			`INSERT INTO actors(did_id, did, indexed_at, source) VALUES (?, ?, ?, ?)`,
			int64(i+1), did, now, model.SourceBootstrap,
		); err != nil {
			return fmt.Errorf("insert actor %d: %w", i, err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO bootstrap_progress(did, completed_at) VALUES (?, ?)`,
			did, now,
		); err != nil {
			return fmt.Errorf("insert progress %d: %w", i, err)
		}
	}
	for i := 0; i < follows; i++ {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO follows(src_did_id, rkey, dst_did_id, src_did, dst_did, created_at, indexed_at, source)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			int64(i+1), fmt.Sprintf("r%d", i), int64(i+2),
			fmt.Sprintf("did:plc:fixture%d", i), fmt.Sprintf("did:plc:fixture%d", i+1),
			now, now, model.SourceBootstrap,
		); err != nil {
			return fmt.Errorf("insert follow %d: %w", i, err)
		}
	}
	for i := 0; i < blocks; i++ {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO blocks(src_did_id, rkey, dst_did_id, src_did, dst_did, created_at, indexed_at, source)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			int64(i+1), fmt.Sprintf("b%d", i), int64(i+2),
			fmt.Sprintf("did:plc:fixture%d", i), fmt.Sprintf("did:plc:fixture%d", i+1),
			now, now, model.SourceBootstrap,
		); err != nil {
			return fmt.Errorf("insert block %d: %w", i, err)
		}
	}
	return nil
}
