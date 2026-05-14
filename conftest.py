"""Top-level pytest conftest.

Adds the repo root to sys.path so tests can `from analysis.<name> import run`
regardless of where pytest is invoked from. Pytest loads conftest.py files
top-down before collecting tests, so this runs before any `tests/analysis/*`
module is imported.
"""

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
