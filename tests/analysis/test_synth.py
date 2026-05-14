"""Smoke test: synthetic snapshot has all expected tables."""

import duckdb


EXPECTED_TABLES = {
    "actor_aggs", "post_aggs", "posts", "likes", "follows", "reposts",
    "blocks", "snapshot_metadata",
}


def test_synthetic_snapshot_has_expected_tables(synthetic_snapshot_path):
    con = duckdb.connect(str(synthetic_snapshot_path), read_only=True)
    try:
        rows = con.execute("SELECT table_name FROM information_schema.tables").fetchall()
        names = {r[0] for r in rows}
        missing = EXPECTED_TABLES - names
        assert not missing, f"synthetic snapshot missing tables: {missing}"

        # Sanity: every table has at least one row (otherwise the
        # downstream analyses are running against empty inputs).
        for t in EXPECTED_TABLES - {"snapshot_metadata"}:
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            assert n > 0, f"{t} is empty in the synthetic snapshot"
    finally:
        con.close()
