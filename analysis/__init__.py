"""Snapshot analyses package.

Each analysis lives in its own submodule (`likes`, `ratio`, `attrition`,
`blocks`) and exposes a `run(con, snapshot_date, ...) -> (html_bytes,
sidecar_dict)` entrypoint. `modal_app` is the Modal dispatcher that wraps
each `run` in a remote function. Tests exercise the `run` entrypoints
directly against synthetic snapshots — no Modal involvement.
"""
