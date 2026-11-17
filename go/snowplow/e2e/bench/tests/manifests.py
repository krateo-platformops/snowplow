"""The loadgen manifest a test's evaluate is given (#1525 review: evaluate judges a real browser by what its pod
was configured to do, from the run's own loadgen-job.yaml, and refuses a browser without it). Each helper writes
the k6browser ConfigMap's browser-config.json as run606 records it, and states the renderer watch explicitly."""

from __future__ import annotations

import json
from pathlib import Path

import yaml


def manifest(d: Path, *, renderer_watch: bool | None, sessions: int = 3) -> str:
    """d/loadgen-job.yaml: renderer_watch True (a bench from #1517 on: the per-renderer rule, its records owed), or
    None (no such flag: OLD EVIDENCE, a bench before #1517: the container rule)."""
    cfg = {"sessions": [{"name": f"s{i}"} for i in range(1, sessions + 1)]}
    if renderer_watch is not None:
        cfg["rendererWatch"] = renderer_watch
    d.mkdir(parents=True, exist_ok=True)
    p = d / "loadgen-job.yaml"
    p.write_text(yaml.safe_dump({"kind": "ConfigMap", "metadata": {"name": "k6browser"},
                                 "data": {"browser-config.json": json.dumps(cfg)}}))
    return str(p)


def old_evidence_manifest(d: Path, sessions: int = 3) -> str:
    """A browser config with no renderer watch: the fixture is OLD EVIDENCE (no renderers sidecar, no per-renderer
    records), judged by the container rule, as a run from before #1517 is."""
    return manifest(d, renderer_watch=None, sessions=sessions)
