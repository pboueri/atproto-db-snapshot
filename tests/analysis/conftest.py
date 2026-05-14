"""Pytest fixtures for the analysis tests.

Builds a synthetic snapshot.duckdb once per session, then hands each test
an opened connection.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

# `synth` lives next to this conftest.py; pytest auto-adds the conftest's
# directory to sys.path, so a flat import works without package fiddling.
from synth import (  # noqa: E402
    DEFAULT_SNAPSHOT_DATE,
    make_synthetic_snapshot,
)


@pytest.fixture(scope="session")
def snapshot_date() -> str:
    return DEFAULT_SNAPSHOT_DATE


@pytest.fixture(scope="session")
def synthetic_snapshot_path(tmp_path_factory) -> Path:
    """Path to a synthetic snapshot.duckdb, built once per test session."""
    db_path = tmp_path_factory.mktemp("snap") / "snapshot.duckdb"
    make_synthetic_snapshot(db_path)
    return db_path


@pytest.fixture()
def synthetic_con(synthetic_snapshot_path):
    """Fresh DuckDB connection to the synthetic snapshot.

    Per-test scope so each analysis gets a clean session (no leaked
    TEMPORARY tables from a sibling test). The underlying file is
    shared.
    """
    con = duckdb.connect(str(synthetic_snapshot_path), read_only=True)
    yield con
    con.close()
