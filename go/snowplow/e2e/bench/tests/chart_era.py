"""#1572: the run's values for the installer charts the suite renders, which predate krateo-057's
recorded features.

The bench writes every feature 057 runs (bench/installer_features.py) and refuses, before any
cluster exists, an installer whose schema cannot express them (check_chart): 057 runs installer
0.3.697, and the published archives the suite renders (tests/fixtures/installer_charts: 0.3.545,
0.3.616, 0.3.641) have no `internalTls` nor `publicPages`, so helm's schema validation rejects the
document as written. A test about ANOTHER property of those charts (their private-image list, their
route table, their portal readers, the snowplow env they render) renders them with the run's
document less the feature lines one of those archives' schemas does not declare; every other byte
is the document's. tests/test_installer_features.py holds the refusal itself.
"""

from __future__ import annotations

import json
import re
import tarfile
from functools import lru_cache
from pathlib import Path

from bench import installer_features

ARCHIVES = Path(__file__).resolve().parent / "fixtures" / "installer_charts"


@lru_cache(maxsize=None)
def undeclared() -> tuple[str, ...]:
    """The record's features one of the suite's installer archives does not declare."""
    out: set[str] = set()
    for tgz in sorted(ARCHIVES.glob("installer-*.tgz")):
        with tarfile.open(tgz) as tf:
            schema = json.load(tf.extractfile("installer/values.schema.json"))
        declared = set(schema["properties"]["features"]["properties"])
        out |= set(installer_features.load()["features"]) - declared
    return tuple(sorted(out))


def for_archives(doc: str) -> str:
    """`doc` (bench.installer_values.document) without the feature lines the suite's archives cannot take."""
    for f in undeclared():
        doc, n = re.subn(rf"(?m)^  {re.escape(f)}: (?:true|false)\n", "", doc)
        assert n == 1, f
    return doc


def patch(monkeypatch, *, chart_check: bool = False) -> None:
    """Production code that writes the document itself (vm606.chart_check, the CLIs run in-process)
    writes it for the suite's archives; `chart_check`: and the features check, which refuses those
    archives by design (held in tests/test_installer_features.py), says nothing."""
    from bench import installer_values
    real = installer_values.document
    monkeypatch.setattr(installer_values, "document", lambda *a, **k: for_archives(real(*a, **k)))
    if chart_check:
        monkeypatch.setattr(installer_features, "check_chart", lambda *a, **k: [])
